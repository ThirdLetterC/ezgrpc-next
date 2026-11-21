#pragma once

/* grpc_header.h - request header view
 *
 * grpc_header is a borrowed view of one received HTTP/2 header; the bytes belong to nghttp2 and
 * live as long as the stream or call holds references (rcbufs) to its header buffers.
 * grpc_call_metadata*() expose them to the application as grpc_metadata.
 */

#include <stddef.h>

/* A request header whose name and value point into the stream's nghttp2
 * buffers and are NUL-terminated. */
typedef struct grpc_header grpc_header;
struct grpc_header {
  size_t namelen;
  const char *name;
  size_t valuelen;
  const char *value;
};
