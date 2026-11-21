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

/* Install the server's nghttp2 callbacks into `ngcallbacks` (shared by all sessions). */
void server_setup_ngcallbacks(nghttp2_session_callbacks *ngcallbacks);
/* nghttp2 data source read callback for response bodies: copies up to `buf_len` bytes of the
 * stream's queued messages (5-byte prefix plus payload) into `buf`, frees finished messages, and
 * when the queue is empty either ends the stream or submits the pending trailers. Returns the
 * bytes written or NGHTTP2_ERR_CALLBACK_FAILURE. */
ssize_t data_source_read_callback2(nghttp2_session *session, i32 stream_id, u8 *buf, size_t buf_len,
                                   u32 *data_flags, nghttp2_data_source *source, void *user_data);
