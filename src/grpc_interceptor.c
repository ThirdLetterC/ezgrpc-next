/* grpc_interceptor.c - call interceptors and the built-in bearer-auth and rate-limit ones
 *
 * The server keeps a copy of each interceptor added with grpc_server_add_interceptor() in an
 * array, run in insertion order and filtered by path_prefix. The interceptors_run_* hooks are
 * called by grpc_call.c on the server thread: on_call when a call is first dispatched, on_recv
 * before each received message is delivered, on_send before each response is queued, and on_done
 * once when the call ends. A non-OK status from the first three finishes the call with that
 * status; the caller then stops processing it.
 *
 * The server owns each interceptor's userdata after a successful add and releases it with
 * userdata_free in interceptors_free(). The rate limiter guards its bucket with a mutex.
 */

#include <stdlib.h>
#include <string.h>
#include "internal/grpc_platform.h"
#include "core.h"
#include "grpc_timeout.h"

/* Returns true if the interceptor applies to the call: no path_prefix, or the call's method
 * (grpc_call_method()) starts with it. */
static bool interceptor_matches_call(const grpc_interceptor *inc, const grpc_call *call) {
  if (inc->path_prefix == nullptr)
    return true;
  const char *method = grpc_call_method(call);
  if (method == nullptr)
    return false;
  size_t plen = strlen(inc->path_prefix);
  return strncmp(method, inc->path_prefix, plen) == 0;
}

/* Grows the array first and keeps the grown block even if copying path_prefix then fails, so the
 * server never points at a freed array. The copy's path_prefix is a grpc_strdup() owned by the
 * server. Not synchronized: add interceptors before the server runs. */
GRPC_API grpc_err grpc_server_add_interceptor(grpc_server *server,
                                              const grpc_interceptor *interceptor) {
  if (server == nullptr || interceptor == nullptr) {
    return GRPC_ERR_INVALID;
  }

  size_t new_nb = server->nb_interceptors + 1;
  grpc_interceptor *new_arr = grpc_realloc(server->interceptors, new_nb * sizeof(grpc_interceptor));
  if (new_arr == nullptr) {
    return GRPC_ERR_NOMEM;
  }

  grpc_interceptor copy = *interceptor;
  if (interceptor->path_prefix != nullptr) {
    copy.path_prefix = grpc_strdup(interceptor->path_prefix);
    if (copy.path_prefix == nullptr) {
      server->interceptors = new_arr;
      return GRPC_ERR_NOMEM;
    }
  }

  server->interceptors = new_arr;
  server->interceptors[server->nb_interceptors] = copy;
  server->nb_interceptors = new_nb;
  return GRPC_OK;
}

/* Runs the matching on_call hooks. On the first non-OK status finishes the call with it and
 * returns false; the remaining interceptors do not run. */
bool interceptors_run_on_call(grpc_call *call) {
  grpc_server *server = call->server;
  for (size_t i = 0; i < server->nb_interceptors; i++) {
    const grpc_interceptor *inc = &server->interceptors[i];
    if (inc->on_call == nullptr || !interceptor_matches_call(inc, call))
      continue;
    const char *message = nullptr;
    grpc_status status = inc->on_call(call, &message, inc->userdata);
    if (status != GRPC_STATUS_OK) {
      (void)grpc_call_finish(call, status, message);
      return false;
    }
  }
  return true;
}

/* Runs the matching on_recv_message hooks before `message` is delivered. On the first non-OK
 * status finishes the call and returns false; the caller still owns `message`. */
bool interceptors_run_on_recv(grpc_call *call, const grpc_message *message) {
  grpc_server *server = call->server;
  for (size_t i = 0; i < server->nb_interceptors; i++) {
    const grpc_interceptor *inc = &server->interceptors[i];
    if (inc->on_recv_message == nullptr || !interceptor_matches_call(inc, call))
      continue;
    const char *err_msg = nullptr;
    grpc_status status = inc->on_recv_message(call, message, &err_msg, inc->userdata);
    if (status != GRPC_STATUS_OK) {
      (void)grpc_call_finish(call, status, err_msg);
      return false;
    }
  }
  return true;
}

/* Runs the matching on_send_message hooks before `message` is queued. On the first non-OK
 * status finishes the call and returns false; the caller still owns `message`. */
bool interceptors_run_on_send(grpc_call *call, const grpc_message *message) {
  grpc_server *server = call->server;
  for (size_t i = 0; i < server->nb_interceptors; i++) {
    const grpc_interceptor *inc = &server->interceptors[i];
    if (inc->on_send_message == nullptr || !interceptor_matches_call(inc, call))
      continue;
    const char *err_msg = nullptr;
    grpc_status status = inc->on_send_message(call, message, &err_msg, inc->userdata);
    if (status != GRPC_STATUS_OK) {
      (void)grpc_call_finish(call, status, err_msg);
      return false;
    }
  }
  return true;
}

/* Runs the matching on_done hooks with the time since the request arrived. Called from both the
 * finish and the cancel paths, so it guards itself to run once per call, and skips calls that
 * never started (on_call never ran for them). */
void interceptors_run_on_done(grpc_call *call, grpc_status status) {
  /* once, and only for calls that went through on_call */
  if (call->done_reported || !call->started)
    return;
  call->done_reported = true;

  grpc_server *server = call->server;
  uint64_t duration_ms = grpc_time_now_ms() - call->start_ms;
  for (size_t i = 0; i < server->nb_interceptors; i++) {
    const grpc_interceptor *inc = &server->interceptors[i];
    if (inc->on_done != nullptr && interceptor_matches_call(inc, call))
      inc->on_done(call, status, duration_ms, inc->userdata);
  }
}

/* Frees the interceptor array, the path_prefix copies and each userdata. Called when the server
 * is freed. */
void interceptors_free(grpc_server *server) {
  for (size_t i = 0; i < server->nb_interceptors; i++) {
    const grpc_interceptor *inc = &server->interceptors[i];
    grpc_free((void *)inc->path_prefix);
    if (inc->userdata_free != nullptr)
      inc->userdata_free(inc->userdata);
  }
  grpc_free(server->interceptors);
  server->interceptors = nullptr;
  server->nb_interceptors = 0;
}

typedef struct bearer_auth_data {
  grpc_token_validator_fn validator;
  void *userdata;
} bearer_auth_data;

/* on_call hook of the bearer-auth interceptor: requires an "authorization: Bearer <token>"
 * header (scheme matched case-insensitively, extra spaces before the token skipped) and passes
 * the token to the validator. Returns UNAUTHENTICATED with a static message otherwise. Fails
 * closed: a null state (allocation failed in grpc_interceptor_bearer_auth()) or a null validator
 * rejects every call, so an allocation failure can never switch authentication off. */
static grpc_status bearer_auth_on_call(grpc_call *call, const char **message, void *userdata) {
  bearer_auth_data *auth = userdata;
  if (auth == nullptr) {
    *message = "authentication unavailable";
    return GRPC_STATUS_INTERNAL;
  }
  if (auth->validator == nullptr) {
    *message = "no token validator configured";
    return GRPC_STATUS_UNAUTHENTICATED;
  }

  size_t len = 0;
  const char *header = grpc_call_metadata(call, "authorization", &len);
  if (header == nullptr) {
    *message = "missing authorization metadata";
    return GRPC_STATUS_UNAUTHENTICATED;
  }
  if (len < 7 || strncasecmp(header, "Bearer ", 7) != 0) {
    *message = "invalid authorization header format (expected 'Bearer <token>')";
    return GRPC_STATUS_UNAUTHENTICATED;
  }

  const char *token = header + 7;
  while (*token == ' ')
    token++;

  if (!auth->validator(token, auth->userdata)) {
    *message = "invalid or expired token";
    return GRPC_STATUS_UNAUTHENTICATED;
  }
  return GRPC_STATUS_OK;
}

/* Allocates the validator state as userdata, released with grpc_free by interceptors_free(). If
 * the allocation fails the interceptor is still returned, with null userdata, and rejects every
 * call (see bearer_auth_on_call()). */
GRPC_API grpc_interceptor grpc_interceptor_bearer_auth(grpc_token_validator_fn validator,
                                                       void *userdata) {
  bearer_auth_data *data = grpc_malloc(sizeof(*data));
  if (data != nullptr) {
    data->validator = validator;
    data->userdata = userdata;
  }

  return (grpc_interceptor){
      .name = "bearer_auth",
      .on_call = bearer_auth_on_call,
      .userdata = data,
      .userdata_free = grpc_free,
  };
}

typedef struct rate_limit_data {
  pthread_mutex_t lock;
  double rate_per_ms;
  double burst;
  double tokens;
  uint64_t last_ms;
} rate_limit_data;

/* userdata_free of the rate limiter: destroys its mutex and frees it. */
static void rate_limit_free(void *userdata) {
  rate_limit_data *rl = userdata;
  if (rl == nullptr)
    return;
  pthread_mutex_destroy(&rl->lock);
  grpc_free(rl);
}

/* on_call hook of the rate limiter: refills the token bucket for the elapsed time (capped at
 * the burst size) and takes one token, or rejects the call with RESOURCE_EXHAUSTED. The bucket
 * is updated under its mutex. Fails closed: a null state (allocation failed) rejects every call
 * rather than silently removing the limit. */
static grpc_status rate_limit_on_call([[maybe_unused]] grpc_call *call, const char **message,
                                      void *userdata) {
  rate_limit_data *rl = userdata;
  if (rl == nullptr) {
    *message = "rate limiter unavailable";
    return GRPC_STATUS_INTERNAL;
  }

  pthread_mutex_lock(&rl->lock);
  uint64_t now = grpc_time_now_ms();
  if (now > rl->last_ms) {
    double elapsed = (double)(now - rl->last_ms);
    rl->tokens += elapsed * rl->rate_per_ms;
    if (rl->tokens > rl->burst)
      rl->tokens = rl->burst;
    rl->last_ms = now;
  }
  bool allowed = rl->tokens >= 1.0;
  if (allowed)
    rl->tokens -= 1.0;
  pthread_mutex_unlock(&rl->lock);

  if (!allowed) {
    *message = "rate limit exceeded";
    return GRPC_STATUS_RESOURCE_EXHAUSTED;
  }
  return GRPC_STATUS_OK;
}

/* Starts with a full bucket. A non-positive rate never refills it; a zero burst becomes 1. If
 * the allocation fails the interceptor is still returned, with null userdata, and rejects every
 * call (see rate_limit_on_call()). */
GRPC_API grpc_interceptor grpc_interceptor_rate_limit(double requests_per_sec, size_t burst) {
  rate_limit_data *rl = grpc_calloc(1, sizeof(*rl));
  if (rl != nullptr) {
    pthread_mutex_init(&rl->lock, nullptr);
    rl->rate_per_ms = requests_per_sec > 0.0 ? requests_per_sec / 1000.0 : 0.0;
    rl->burst = burst > 0 ? (double)burst : 1.0;
    rl->tokens = rl->burst;
    rl->last_ms = grpc_time_now_ms();
  }
  return (grpc_interceptor){
      .name = "rate_limit",
      .on_call = rate_limit_on_call,
      .userdata = rl,
      .userdata_free = rate_limit_free,
  };
}
