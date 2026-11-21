#include <stdlib.h>
#include <string.h>
#include "internal/grpc_platform.h"
#include "core.h"
#include "grpc_timeout.h"

static bool interceptor_matches_call(const grpc_interceptor *inc, const grpc_call *call) {
  if (inc->path_prefix == nullptr)
    return true;
  const char *method = grpc_call_method(call);
  if (method == nullptr)
    return false;
  size_t plen = strlen(inc->path_prefix);
  return strncmp(method, inc->path_prefix, plen) == 0;
}

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

static grpc_status bearer_auth_on_call(grpc_call *call, const char **message, void *userdata) {
  bearer_auth_data *auth = userdata;
  if (auth == nullptr || auth->validator == nullptr) {
    return GRPC_STATUS_OK;
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

static void rate_limit_free(void *userdata) {
  rate_limit_data *rl = userdata;
  if (rl == nullptr)
    return;
  pthread_mutex_destroy(&rl->lock);
  grpc_free(rl);
}

static grpc_status rate_limit_on_call([[maybe_unused]] grpc_call *call, const char **message,
                                      void *userdata) {
  rate_limit_data *rl = userdata;
  if (rl == nullptr)
    return GRPC_STATUS_OK;

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
