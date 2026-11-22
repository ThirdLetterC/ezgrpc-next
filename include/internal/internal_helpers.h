#pragma once

/* internal_helpers.h - session, stream and buffer helpers
 *
 * Connection (grpc_session) lifecycle and output flushing, stream allocation and linking, logging
 * helpers, and the growable grpc_buf byte buffer. Everything here runs on the server loop thread.
 * Implemented in src/internal_helpers.c.
 */

#include <stdio.h>
#include <string.h>
#include "core.h"

/* Allocator hooks in the shape shared by nghttp2_mem, ngtcp2_mem and nghttp3_mem (the user data
 * pointer comes last and is ignored): they route the protocol libraries' memory through
 * grpc_malloc() and friends, so grpc_set_allocator() and the GRPC_ALLOCATOR build option cover
 * it too. */
void *grpc_mem_hook_malloc(size_t size, void *user_data);
void grpc_mem_hook_free(void *ptr, void *user_data);
void *grpc_mem_hook_calloc(size_t nmemb, size_t size, void *user_data);
void *grpc_mem_hook_realloc(void *ptr, size_t size, void *user_data);
/* The nghttp2 memory table built from these hooks, for the server and client sessions. */
extern const nghttp2_mem grpc_nghttp2_mem;

/* Start closing a session: detach and free its streams (their calls end with UNAVAILABLE), free
 * the nghttp2/TLS state and uv_close() the handle. No-op if already closing or inactive. */
void session_close(grpc_session *session);

/* Queue the session's pending nghttp2 output for the next poll. */
void session_schedule_flush(grpc_session *session);
/* Serialize pending nghttp2 frames and write them to the socket. */
void session_flush(grpc_session *session);
/* Flush every session queued with session_schedule_flush(). */
void server_flush_sessions(grpc_server *server);
/* libuv connection callback of the listeners: accepts a client into a free session slot. */
void on_listener_connection(uv_stream_t *listener, int status);

/* Free an unlinked stream and its buffers and queued messages (not its call). Accepts nullptr. */
void stream_free(grpc_stream *stream);
/* Take a free session slot (caller checks nb_free_sessions > 0) and mark it active / return a
 * closed session's slot to the free stack. */
grpc_session *session_slot_acquire(grpc_server *server);
void session_slot_release(grpc_session *session);
/* Format "a.b.c.d:port" or "[v6]:port" into out. */
void format_peer(const struct sockaddr *sa, char *out, size_t out_len);
/* Allocate a zeroed stream for `stream_id`; nullptr when out of memory. */
grpc_stream *stream_new(int64_t stream_id, const grpc_transport_ops *ops);
/* Add `stream` to the session's stream list and bump the stream counters. */
void stream_link(grpc_session *session, grpc_stream *stream);
/* Remove `stream` from the session's list and decrement the counters; does not free it. */
void stream_unlink(grpc_session *session, grpc_stream *stream);

/* printf-style write of "[timestamp] " plus the message to `fp`, ignoring log levels. */
void ezflog(FILE *fp, const char *fmt, ...);
/* Write the local date and time into `buf` (at most `len` bytes) and return `buf`. */
char *ezgetdt(char *buf, size_t len);

/* Ensure that `buf` has room for `extra` more bytes, growing it by doubling (from 4096). Returns
 * 0 on success, 1 when out of memory (the buffer is then unchanged). */
static inline int buf_reserve(grpc_buf *buf, size_t extra) {
  if (buf->cap - buf->len >= extra)
    return 0;
  size_t cap = buf->cap ? buf->cap : 4096;
  while (cap - buf->len < extra)
    cap *= 2;
  /* the library allocator, never libc: on MSVC grpc_free() is _aligned_free(), so
   * buffers must come from the same family (e.g. server->tls_obuf is grpc_free()d) */
  u8 *data = grpc_realloc(buf->data, cap);
  if (data == nullptr)
    return 1;
  buf->data = data;
  buf->cap = cap;
  return 0;
}

/* Append `len` bytes of `data` to `buf`. Returns 0 on success, 1 when out of memory. */
static inline int buf_append(grpc_buf *buf, const void *data, size_t len) {
  if (buf_reserve(buf, len))
    return 1;
  memcpy(buf->data + buf->len, data, len);
  buf->len += len;
  return 0;
}

/* Free the buffer's storage with grpc_free() and reset it to empty. */
static inline void buf_free(grpc_buf *buf) {
  grpc_free(buf->data);
  *buf = (grpc_buf){};
}

/* Read an unaligned 32-bit unsigned integer in host byte order (use ntohl() for wire values). */
static inline uint32_t uread_u32(const void *p) {
  uint32_t ret;
  memcpy(&ret, p, 4);
  return ret;
}
