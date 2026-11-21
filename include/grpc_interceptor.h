#pragma once

#include "grpc_call.h"
#include "grpc_defs.h"
#include "grpc_error.h"
#include "grpc_status.h"

typedef struct grpc_server grpc_server;

/**
 * Interceptors run for every call, in the order they were added.
 *
 * ``on_call`` runs before the handler (or the first event) of a call, when
 * its metadata is available. Returning anything but GRPC_STATUS_OK ends
 * the call with that status and ``*message`` (if set) instead of running it.
 *
 * ``on_done`` runs when the call ends: finished by the application or the
 * library, or cancelled. ``duration_ms`` is the time since the request arrived.
 */
typedef struct grpc_interceptor {
  const char *name;
  /** Optional path prefix filter (e.g. "/helloworld.Greeter/"). nullptr matches all methods. */
  const char *path_prefix;
  grpc_status (*on_call)(grpc_call *call, const char **message, void *userdata);
  /** Optional hook called before each received message is delivered to the handler. */
  grpc_status (*on_recv_message)(grpc_call *call, const grpc_message *message,
                                 const char **error_msg, void *userdata);
  /** Optional hook called before each response message is queued for sending. */
  grpc_status (*on_send_message)(grpc_call *call, const grpc_message *message,
                                 const char **error_msg, void *userdata);
  void (*on_done)(grpc_call *call, grpc_status status, uint64_t duration_ms, void *userdata);
  void *userdata;
  /** Called on ``userdata`` when the server is freed. Optional. */
  void (*userdata_free)(void *userdata);
} grpc_interceptor;

/**
 * Adds an interceptor. The struct is copied.
 */
GRPC_API grpc_err grpc_server_add_interceptor(grpc_server *server,
                                              const grpc_interceptor *interceptor);

/** Returns true if ``token`` (from ``authorization: Bearer <token>``) is valid. */
typedef bool (*grpc_token_validator_fn)(const char *token, void *userdata);

/**
 * An interceptor that ends calls with UNAUTHENTICATED unless they carry
 * ``authorization: Bearer <token>`` accepted by ``validator``.
 */
GRPC_API grpc_interceptor grpc_interceptor_bearer_auth(grpc_token_validator_fn validator,
                                                       void *userdata);

/**
 * A token-bucket rate limiter interceptor. Ends calls with RESOURCE_EXHAUSTED
 * once ``burst`` tokens are consumed faster than ``requests_per_sec``.
 */
GRPC_API grpc_interceptor grpc_interceptor_rate_limit(double requests_per_sec, size_t burst);
