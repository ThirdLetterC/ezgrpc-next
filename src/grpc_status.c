/* grpc_status.c - human-readable names for status and error codes
 *
 * Maps grpc_status (the gRPC status sent to peers in grpc-status) and grpc_err (the library's
 * own API result codes) to static, lowercase English strings for logging. The strings have
 * static storage duration and must not be freed.
 */

#include "core.h"
#include "grpc_error.h"
#include "grpc_status.h"

/* Returns a static name for a gRPC status code, or "(null)" for values outside the enum. */
GRPC_API const char *grpc_status_str(grpc_status status) {
  switch (status) {
  case GRPC_STATUS_OK:
    return "ok";
  case GRPC_STATUS_CANCELLED:
    return "cancelled";
  case GRPC_STATUS_UNKNOWN:
    return "unknown";
  case GRPC_STATUS_INVALID_ARGUMENT:
    return "invalid argument";
  case GRPC_STATUS_DEADLINE_EXCEEDED:
    return "deadline exceeded";
  case GRPC_STATUS_NOT_FOUND:
    return "not found";
  case GRPC_STATUS_ALREADY_EXISTS:
    return "already exists";
  case GRPC_STATUS_PERMISSION_DENIED:
    return "permission denied";
  case GRPC_STATUS_UNAUTHENTICATED:
    return "unauthenticated";
  case GRPC_STATUS_RESOURCE_EXHAUSTED:
    return "resource exhausted";
  case GRPC_STATUS_FAILED_PRECONDITION:
    return "failed precondition";
  case GRPC_STATUS_ABORTED:
    return "aborted";
  case GRPC_STATUS_OUT_OF_RANGE:
    return "out of range";
  case GRPC_STATUS_UNIMPLEMENTED:
    return "unimplemented";
  case GRPC_STATUS_INTERNAL:
    return "internal";
  case GRPC_STATUS_UNAVAILABLE:
    return "unavailable";
  case GRPC_STATUS_DATA_LOSS:
    return "data loss";
  default:
    return "(null)";
  }
}

/* Returns a static description of a library error code, or "unknown error" for values outside
 * the enum. The switch has no default case so the compiler can warn when a grpc_err value is
 * added without a message here. */
GRPC_API const char *grpc_strerror(grpc_err err) {
  switch (err) {
  case GRPC_OK:
    return "success";
  case GRPC_ERR_INVALID:
    return "invalid argument";
  case GRPC_ERR_NOMEM:
    return "out of memory";
  case GRPC_ERR_CLOSED:
    return "the call or connection is closed";
  case GRPC_ERR_FINISHED:
    return "the call is already finished";
  case GRPC_ERR_STATE:
    return "not allowed in the current state";
  case GRPC_ERR_PROTOCOL:
    return "HTTP/2 protocol error";
  case GRPC_ERR_EXISTS:
    return "already exists";
  case GRPC_ERR_NOT_FOUND:
    return "not found";
  case GRPC_ERR_UNSUPPORTED:
    return "not supported by this build";
  case GRPC_ERR_IO:
    return "I/O error";
  case GRPC_ERR_LIMIT:
    return "output limit exceeded";
  case GRPC_ERR_CODEC:
    return "compression or decompression failed";
  case GRPC_ERR_TIMEOUT:
    return "operation timed out";
  }
  return "unknown error";
}
