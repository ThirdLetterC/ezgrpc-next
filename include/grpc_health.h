#pragma once

/* grpc_health.h - standard gRPC health checking service
 *
 * Implements ``grpc.health.v1.Health`` (Check and Watch) on a server. Statuses are kept per
 * service name plus one overall status for the empty name; they can be changed at any time, and
 * Watch streams are notified of changes.
 */

#include "grpc_defs.h"
#include "grpc_error.h"

typedef struct grpc_server grpc_server;

/** Method paths registered by grpc_health_enable(). */
#define GRPC_HEALTH_PATH_CHECK "/grpc.health.v1.Health/Check"
#define GRPC_HEALTH_PATH_WATCH "/grpc.health.v1.Health/Watch"

/** ``grpc.health.v1.HealthCheckResponse.ServingStatus`` values. */
typedef enum grpc_health_status {
  GRPC_HEALTH_UNKNOWN = 0,
  GRPC_HEALTH_SERVING = 1,
  GRPC_HEALTH_NOT_SERVING = 2,
  GRPC_HEALTH_SERVICE_UNKNOWN = 3,
} grpc_health_status;

/**
 * Serves the standard gRPC health check (/grpc.health.v1.Health/Check) and
 * the streaming /grpc.health.v1.Health/Watch, which sends the current status
 * and then every change. Services report SERVING until grpc_health_set_status()
 * says otherwise, including services that were never set. Returns the error of
 * grpc_server_handle() (e.g. GRPC_ERR_EXISTS) if a path is already registered.
 */
GRPC_API grpc_err grpc_health_enable(grpc_server *server);

/**
 * Sets the status of a service, or of the whole server when ``service`` is
 * nullptr or "". Safe to call from any thread once the health state exists:
 * call grpc_health_enable() (or a first grpc_health_set_status()) before other
 * threads use the server. Service names are truncated to 127 bytes.
 */
GRPC_API grpc_err grpc_health_set_status(grpc_server *server, const char *service,
                                         grpc_health_status status);

/**
 * Returns the status set for a service, or of the whole server when
 * ``service`` is nullptr or "". Safe to call from any thread. Returns
 * GRPC_HEALTH_SERVICE_UNKNOWN for a service whose status was never set, and
 * GRPC_HEALTH_UNKNOWN if health checking was never enabled or set on
 * ``server``. (The Check and Watch RPCs additionally report a service the
 * server has routes for as SERVING; Check answers any other service with
 * NOT_FOUND, as the gRPC health protocol requires.)
 */
GRPC_API grpc_health_status grpc_health_get_status(grpc_server *server, const char *service);
