/* grpc_health.c - the grpc.health.v1.Health service (Check and Watch)
 *
 * Each server has a lazily created health_state: the overall status (the "" service) and a list
 * of per-service statuses. As the gRPC health protocol requires, a service with no status set is
 * known when the server serves routes of it (SERVING) and unknown otherwise: Check answers it with
 * NOT_FOUND, Watch with SERVICE_UNKNOWN (and later changes). Statuses can be set from any
 * thread, so they are guarded by a mutex; the list of Watch streams is touched only on the
 * server thread. A status change posts notify_watchers_on_loop() to the server thread, which
 * sends the new status to every watcher whose service status changed.
 *
 * Requests and responses are encoded by hand: HealthCheckRequest is parsed for field 1
 * (service), and HealthCheckResponse is the two bytes {0x08, status}. Service names are
 * truncated to MAX_SERVICE_NAME_LEN - 1 bytes.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "internal/grpc_threads.h"
#include "core.h"

enum { MAX_SERVICE_NAME_LEN = 128 };

typedef struct service_health_entry {
  char *name;
  grpc_health_status status;
} service_health_entry;

typedef struct health_watcher {
  grpc_call *call;
  char service[MAX_SERVICE_NAME_LEN];
  grpc_health_status last_status;
  struct health_watcher *next;
} health_watcher;

/* per server. guarded by `lock`, since statuses may be set from any thread */
struct health_state {
  pthread_mutex_t lock;
  grpc_health_status overall;
  service_health_entry *entries;
  size_t nb_entries;
  /* Accessed on the server event-loop thread. */
  health_watcher *watchers;
};

/* Returns the server's health state, creating it (overall SERVING) on first use. The creation
 * itself is not synchronized, so the first call must happen before other threads use the
 * server. Returns nullptr on allocation failure. */
static struct health_state *health_state_get(grpc_server *server) {
  if (server->health == nullptr) {
    struct health_state *health = grpc_calloc(1, sizeof(*health));
    if (health == nullptr)
      return nullptr;
    pthread_mutex_init(&health->lock, nullptr);
    health->overall = GRPC_HEALTH_SERVING;
    server->health = health;
  }
  return server->health;
}

/* Frees the health state when the server is freed, dropping the reference each watcher holds on
 * its call. */
void health_free(grpc_server *server) {
  struct health_state *health = server->health;
  if (health == nullptr)
    return;
  health_watcher *w = health->watchers;
  while (w != nullptr) {
    health_watcher *next = w->next;
    grpc_call_unref(w->call);
    grpc_free(w);
    w = next;
  }
  for (size_t i = 0; i < health->nb_entries; i++)
    grpc_free(health->entries[i].name);
  grpc_free(health->entries);
  pthread_mutex_destroy(&health->lock);
  grpc_free(health);
  server->health = nullptr;
}

/* True if the server has an enabled route of `service` ("/service/Method"). Routes change only
 * on the server thread, so this runs there. */
static bool service_has_route(grpc_server *server, const char *service) {
  size_t n = strlen(service);
  for (size_t i = 0; i < server->nb_routes; i++) {
    const char *path = server->routes[i]->path;
    if (!server->routes[i]->disabled && path != nullptr && path[0] == '/' &&
        strncmp(path + 1, service, n) == 0 && path[1 + n] == '/')
      return true;
  }
  return false;
}

/* The status reported for `service` on the server thread: the one set, else SERVING for a
 * service the server serves, else SERVICE_UNKNOWN. */
static grpc_health_status status_on_loop(grpc_server *server, const char *service) {
  grpc_health_status st = grpc_health_get_status(server, service);
  if (st == GRPC_HEALTH_SERVICE_UNKNOWN && service_has_route(server, service))
    return GRPC_HEALTH_SERVING;
  return st;
}

/* Posted to the server thread on a status change. Removes watchers whose call was cancelled
 * (dropping their reference) and sends the current status to every other watcher whose service
 * status differs from the last one it was sent. */
static void notify_watchers_on_loop(void *arg) {
  grpc_server *server = arg;
  struct health_state *health = server->health;
  if (health == nullptr)
    return;
  health_watcher **link = &health->watchers;
  while (*link != nullptr) {
    health_watcher *w = *link;
    if (grpc_call_is_cancelled(w->call)) {
      *link = w->next;
      grpc_call_unref(w->call);
      grpc_free(w);
      continue;
    }
    grpc_health_status cur = status_on_loop(server, w->service);
    if (cur != w->last_status) {
      w->last_status = cur;
      uint8_t response[2] = {0x08, (uint8_t)cur};
      (void)grpc_call_send_data(w->call, response, sizeof(response));
    }
    link = &w->next;
  }
}

/* Names are kept truncated to MAX_SERVICE_NAME_LEN - 1 bytes (as are names read from Check and
 * Watch requests), so they are compared on that prefix: a long name set by the application is
 * found again by the same long name. */
static bool service_name_eq(const char *stored, const char *service) {
  return strncmp(stored, service, MAX_SERVICE_NAME_LEN - 1) == 0;
}

/* An empty or null service sets the overall status. New names are added to the list (truncated
 * to MAX_SERVICE_NAME_LEN - 1 bytes). Watchers are notified through grpc_server_post() only when
 * the status actually changed and the server's async handle is up. */
GRPC_API grpc_err grpc_health_set_status(grpc_server *server, const char *service,
                                         grpc_health_status status) {
  if (server == nullptr)
    return GRPC_ERR_INVALID;
  /* created on first use; set statuses before other threads use the server */
  struct health_state *health = health_state_get(server);
  if (health == nullptr)
    return GRPC_ERR_NOMEM;

  grpc_err err = GRPC_OK;
  bool changed = false;
  pthread_mutex_lock(&health->lock);
  if (service == nullptr || *service == '\0') {
    changed = (health->overall != status);
    health->overall = status;
    goto out;
  }
  for (size_t i = 0; i < health->nb_entries; i++) {
    if (service_name_eq(health->entries[i].name, service)) {
      changed = (health->entries[i].status != status);
      health->entries[i].status = status;
      goto out;
    }
  }
  service_health_entry *entries =
      grpc_realloc_array(health->entries, (health->nb_entries + 1), sizeof(*entries));
  char *name = grpc_strndup(service, MAX_SERVICE_NAME_LEN - 1);
  if (entries != nullptr)
    health->entries = entries;
  if (entries == nullptr || name == nullptr) {
    grpc_free(name);
    err = GRPC_ERR_NOMEM;
    goto out;
  }
  health->entries[health->nb_entries++] = (service_health_entry){name, status};
  changed = true;
out:
  pthread_mutex_unlock(&health->lock);
  if (err == GRPC_OK && changed && server->async_active)
    (void)grpc_server_post(server, notify_watchers_on_loop, server);
  return err;
}

/* Returns UNKNOWN when health was never set up on the server, the overall status for an empty
 * or null service, the status set for the service, or SERVICE_UNKNOWN when none was set. Takes
 * the lock, so it is safe from any thread. */
GRPC_API grpc_health_status grpc_health_get_status(grpc_server *server, const char *service) {
  if (server == nullptr || server->health == nullptr)
    return GRPC_HEALTH_UNKNOWN;
  struct health_state *health = server->health;

  grpc_health_status res = GRPC_HEALTH_SERVICE_UNKNOWN;
  pthread_mutex_lock(&health->lock);
  if (service == nullptr || *service == '\0') {
    res = health->overall;
  } else {
    for (size_t i = 0; i < health->nb_entries; i++) {
      if (service_name_eq(health->entries[i].name, service)) {
        res = health->entries[i].status;
        break;
      }
    }
  }
  pthread_mutex_unlock(&health->lock);
  return res;
}

/* Reads a protobuf varint at *idx, advancing it, without reading past `len`. A truncated varint
 * returns the bits read so far; bits past the 64th are dropped (a shift by >= 64 is undefined). */
static uint64_t read_varint(const uint8_t *data, size_t len, size_t *idx) {
  uint64_t val = 0;
  unsigned shift = 0;
  while (*idx < len) {
    uint8_t b = data[(*idx)++];
    if (shift < 64)
      val |= (uint64_t)(b & 0x7F) << shift;
    if (!(b & 0x80))
      break;
    shift += 7;
  }
  return val;
}

/* Extracts HealthCheckRequest.service (field 1) from the serialized request into `out_service`
 * (truncated to max_len - 1 bytes, empty if absent or malformed). Skips varint and
 * length-delimited fields; stops at any other wire type. Only single-byte tags are understood. */
static void parse_health_request_service(const uint8_t *data, size_t len, char *out_service,
                                         size_t max_len) {
  out_service[0] = '\0';
  if (len == 0 || data == nullptr) {
    return;
  }

  // Protobuf wire format: Field 1, type 2 (length-delimited): tag = 0x0A
  size_t idx = 0;
  while (idx < len) {
    uint8_t tag = data[idx++];
    uint8_t field_num = tag >> 3;
    uint8_t wire_type = tag & 0x07;

    if (field_num == 1 && wire_type == 2) {
      uint64_t str_len = read_varint(data, len, &idx);
      /* compare with the remaining length: idx + str_len could overflow on a hostile length */
      if (str_len <= len - idx) {
        size_t cpy = str_len < max_len - 1 ? (size_t)str_len : max_len - 1;
        memcpy(out_service, data + idx, cpy);
        out_service[cpy] = '\0';
      }
      return;
    } else if (wire_type == 0) {
      (void)read_varint(data, len, &idx);
    } else if (wire_type == 2) {
      uint64_t skip_len = read_varint(data, len, &idx);
      if (skip_len > len - idx)
        return;
      idx += (size_t)skip_len;
    } else {
      break;
    }
  }
}

/* /grpc.health.v1.Health/Check */
/* Unary handler for /grpc.health.v1.Health/Check: replies with the service's status and OK.
 * `userdata` is the server. */
static void health_check(grpc_call *call, const grpc_message *request, void *userdata) {
  grpc_server *server = userdata;
  char service_name[MAX_SERVICE_NAME_LEN] = {};
  if (request->len > 0) {
    parse_health_request_service(request->data, request->len, service_name, sizeof(service_name));
  }

  grpc_health_status status = status_on_loop(server, service_name);
  if (status == GRPC_HEALTH_SERVICE_UNKNOWN) {
    /* the health protocol: Check of a service the server does not know fails with NOT_FOUND */
    (void)grpc_call_finish(call, GRPC_STATUS_NOT_FOUND, "unknown service");
    return;
  }

  /* HealthCheckResponse { ServingStatus status = 1; } */
  uint8_t response[2] = {0x08, (uint8_t)status};
  (void)grpc_call_send_data(call, response, sizeof(response));
  (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

/* /grpc.health.v1.Health/Watch */
/* on_message handler for /grpc.health.v1.Health/Watch: sends the current status of the
 * requested service, then registers a watcher holding a call reference so later changes are
 * streamed. The stream is never finished by the server; it ends when the client cancels.
 * `userdata` is the server. */
static void health_watch_message(grpc_call *call, const grpc_message *request, void *userdata) {
  grpc_server *server = userdata;
  struct health_state *health = server->health;
  if (health == nullptr)
    return;
  char service_name[MAX_SERVICE_NAME_LEN] = {};
  if (request->len > 0)
    parse_health_request_service(request->data, request->len, service_name, sizeof(service_name));

  /* an unknown service is reported as SERVICE_UNKNOWN, and watched for a status to be set */
  grpc_health_status status = status_on_loop(server, service_name);
  uint8_t response[2] = {0x08, (uint8_t)status};
  if (grpc_call_send_data(call, response, sizeof(response)) != GRPC_OK)
    return;

  health_watcher *w = grpc_calloc(1, sizeof(*w));
  if (w == nullptr)
    return;
  w->call = grpc_call_ref(call);
  memcpy(w->service, service_name, sizeof(w->service));
  w->last_status = status;
  w->next = health->watchers;
  health->watchers = w;
}

/* on_cancel handler for Watch: removes the call's first watcher and drops its reference. Any
 * further watchers of the same call (one per request message) are pruned by
 * notify_watchers_on_loop() or health_free(). */
static void health_watch_cancel(grpc_call *call, void *userdata) {
  grpc_server *server = userdata;
  struct health_state *health = server->health;
  if (health == nullptr)
    return;
  health_watcher **link = &health->watchers;
  while (*link != nullptr) {
    health_watcher *w = *link;
    if (w->call == call) {
      *link = w->next;
      grpc_call_unref(w->call);
      grpc_free(w);
      return;
    }
    link = &w->next;
  }
}

/* Creates the health state and registers the Check (unary) and Watch (stream) handlers with
 * the server as userdata. If registering Watch fails, Check is unregistered again. */
GRPC_API grpc_err grpc_health_enable(grpc_server *server) {
  if (server == nullptr)
    return GRPC_ERR_INVALID;
  if (health_state_get(server) == nullptr)
    return GRPC_ERR_NOMEM;
  grpc_err err = grpc_server_handle(server, GRPC_HEALTH_PATH_CHECK, health_check, server);
  if (err != GRPC_OK)
    return err;
  grpc_stream_handler watch_handler = {
      .on_message = health_watch_message,
      .on_cancel = health_watch_cancel,
  };
  err = grpc_server_handle_stream(server, GRPC_HEALTH_PATH_WATCH, &watch_handler, server);
  if (err != GRPC_OK)
    (void)grpc_server_unregister(server, GRPC_HEALTH_PATH_CHECK);
  return err;
}
