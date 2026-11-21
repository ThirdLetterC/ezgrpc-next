#pragma once

/* internal_nghttp2_callbacks.h - server-side nghttp2 callbacks
 *
 * The HTTP/2 event handlers that turn nghttp2 frames into streams, calls and messages, and the
 * data source that writes queued response messages. Implemented in
 * src/internal_nghttp2_callbacks.c; all run on the server loop thread.
 */
#include <sys/types.h>
#include <nghttp2/nghttp2.h>
#include "common.h"
#include "grpc_defs.h"
#include "core.h"

/* Install the server's nghttp2 callbacks into `ngcallbacks` (shared by all sessions). */
void server_setup_ngcallbacks(nghttp2_session_callbacks *ngcallbacks);
/* The HTTP/2 transport ops installed on TCP / Unix-socket sessions. */
extern const grpc_transport_ops grpc_h2_transport_ops;
