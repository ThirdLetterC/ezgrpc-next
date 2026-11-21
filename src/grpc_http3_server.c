/* grpc_http3_server.c - the HTTP/3 (QUIC) server transport
 *
 * When config.http3.enable is set the server also listens on UDP. Datagrams are routed to QUIC
 * connections (ngtcp2) by their destination connection ID; HTTP/3 framing and QPACK run in
 * nghttp3. Each QUIC connection occupies a slot of server->sessions like a TCP connection, with
 * session->ops set to grpc_h3_transport_ops and session->quic pointing here, so deadlines, the
 * maintenance timer, drain, channelz and max_connections treat both transports alike. Request
 * handling is shared with HTTP/2 through grpc_stream_core.c.
 *
 * Everything runs on the server loop thread. Received datagrams are processed in the UDP read
 * callback; output is produced when the session is flushed (server_flush_sessions() after each
 * loop iteration) and when the connection's timer fires (ngtcp2 loss detection, ACK delay, idle
 * timeout, keepalive). Response bodies are pulled with stream_core_pull() into per-stream chunks
 * that stay alive until QUIC acknowledges them, because nghttp3 sends from the caller's memory.
 *
 * Not implemented: 0-RTT, connection migration (disable_active_migration is advertised), and the
 * closing/draining period (a connection is freed right after its CONNECTION_CLOSE is sent).
 */
#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include <nghttp3/nghttp3.h>
#include <ngtcp2/ngtcp2.h>
#include <ngtcp2/ngtcp2_crypto.h>

#include "core.h"
#include "grpc_stream_core.h"
#include "grpc_timeout.h"
#include "internal_helpers.h"
#include "internal_http3.h"
#include "internal_quic_tls.h"

/* The grpc_nv used by the call layer must be usable as an nghttp3_nv array as is. */
static_assert(sizeof(grpc_nv) == sizeof(nghttp3_nv) &&
                  offsetof(grpc_nv, name) == offsetof(nghttp3_nv, name) &&
                  offsetof(grpc_nv, value) == offsetof(nghttp3_nv, value) &&
                  offsetof(grpc_nv, namelen) == offsetof(nghttp3_nv, namelen) &&
                  offsetof(grpc_nv, valuelen) == offsetof(nghttp3_nv, valuelen) &&
                  offsetof(grpc_nv, flags) == offsetof(nghttp3_nv, flags),
              "grpc_nv must match nghttp3_nv");

/* Length of the connection IDs the server issues. */
enum { H3_SCID_LEN = 18 };
/* Buckets of the connection-ID table (a power of two). */
enum { H3_CID_BUCKETS = 4096 };
/* Size of one response send chunk. */
enum { H3_CHUNK = 16384 };
/* Largest UDP payload written. */
enum { H3_MAX_UDP_PAYLOAD = 1452 };
/* How long a Retry token stays valid. */
#define H3_RETRY_TOKEN_TIMEOUT (10 * NGTCP2_SECONDS)

typedef struct grpc_quic_conn grpc_quic_conn;

/* One entry of the connection-ID table. */
typedef struct cid_entry cid_entry;
struct cid_entry {
  ngtcp2_cid cid;
  grpc_quic_conn *qc;
  cid_entry *next;
};

/* The HTTP/3 state of a server (server->h3). */
typedef struct grpc_h3_server {
  grpc_server *server;
  uv_udp_t udp4;
  uv_udp_t udp6;
  bool udp4_active;
  bool udp6_active;
  /* Bound local addresses, used as the local side of every path. */
  struct sockaddr_storage local4;
  struct sockaddr_storage local6;
  uint16_t port;
  bool accepting;

  grpc_quic_tls_ctx *tls_ctx;
  grpc_quic_tls_ctx **retired_tls;
  size_t nb_retired_tls;

  /* Secret for stateless reset and Retry tokens, random per server. */
  uint8_t secret[32];

  cid_entry *cids[H3_CID_BUCKETS];

  /* Datagram buffers; the loop is single-threaded and each datagram is handled synchronously. */
  uint8_t rbuf[65536];
  uint8_t sbuf[H3_MAX_UDP_PAYLOAD];
} grpc_h3_server;

/* One QUIC connection (session->quic). */
struct grpc_quic_conn {
  grpc_session *session;
  grpc_h3_server *h3s;
  ngtcp2_conn *conn;
  nghttp3_conn *h3;
  grpc_quic_tls_session *tls;
  ngtcp2_crypto_conn_ref conn_ref;
  uv_udp_t *udp;
  struct sockaddr_storage local;
  socklen_t locallen;
  struct sockaddr_storage remote;
  socklen_t remotelen;
  uv_timer_t timer;
  /* Error sent with CONNECTION_CLOSE. */
  ngtcp2_ccerr ccerr;
  /* Connection IDs registered in the table for this connection. */
  ngtcp2_cid *cids;
  size_t nb_cids;
  size_t cap_cids;
};

/* Per-stream transport state (grpc_stream.transport). */
typedef struct h3_chunk h3_chunk;
struct h3_chunk {
  h3_chunk *next;
  size_t len;
  uint8_t data[H3_CHUNK];
};
typedef struct h3_stream {
  /* Body bytes handed to nghttp3 and not yet acknowledged, oldest first. */
  h3_chunk *head;
  h3_chunk *tail;
  size_t head_acked;
  /* Response headers waiting for the body (nghttp3 submits headers and body together). */
  grpc_nv *headers;
  size_t nb_headers;
  bool response_submitted;
  /* The body is complete; the data reader returns EOF. */
  bool body_done;
} h3_stream;

extern const grpc_transport_ops grpc_h3_transport_ops;

static void h3_session_close(grpc_session *session);
static void h3_session_flush(grpc_session *session);

/* ---- helpers ---- */

/* Current time for ngtcp2 / nghttp3, in nanoseconds of the monotonic clock. */
static ngtcp2_tstamp h3_now(void) {
  return uv_hrtime();
}

/* Fills dest with random bytes from the OS CSPRNG. */
static void h3_random(uint8_t *dest, size_t len) {
  if (uv_random(nullptr, nullptr, dest, len, 0, nullptr) != 0) {
    /* uv_random only fails if the OS has no entropy source; never ship zeroes silently */
    for (size_t i = 0; i < len; i++)
      dest[i] = (uint8_t)(uv_hrtime() >> (i % 8));
  }
}

/* ngtcp2 rand callback. */
static void cb_rand(uint8_t *dest, size_t destlen, const ngtcp2_rand_ctx *rand_ctx) {
  (void)rand_ctx;
  h3_random(dest, destlen);
}

/* FNV-1a over a connection ID, for the table. */
static size_t cid_hash(const ngtcp2_cid *cid) {
  uint32_t h = 2166136261u;
  for (size_t i = 0; i < cid->datalen; i++)
    h = (h ^ cid->data[i]) * 16777619u;
  return h & (H3_CID_BUCKETS - 1);
}

/* Returns the connection owning `cid`, or nullptr. */
static grpc_quic_conn *cid_lookup(grpc_h3_server *h3s, const uint8_t *data, size_t len) {
  ngtcp2_cid cid;
  ngtcp2_cid_init(&cid, data, len);
  for (cid_entry *e = h3s->cids[cid_hash(&cid)]; e != nullptr; e = e->next) {
    if (ngtcp2_cid_eq(&e->cid, &cid))
      return e->qc;
  }
  return nullptr;
}

/* Routes `cid` to qc and remembers it for removal. Returns 0, or -1 when out of memory. */
static int cid_add(grpc_quic_conn *qc, const ngtcp2_cid *cid) {
  if (qc->nb_cids == qc->cap_cids) {
    size_t cap = qc->cap_cids ? qc->cap_cids * 2 : 8;
    ngtcp2_cid *cids = grpc_realloc(qc->cids, cap * sizeof(*cids));
    if (cids == nullptr)
      return -1;
    qc->cids = cids;
    qc->cap_cids = cap;
  }
  cid_entry *e = grpc_malloc(sizeof(*e));
  if (e == nullptr)
    return -1;
  size_t b = cid_hash(cid);
  *e = (cid_entry){.cid = *cid, .qc = qc, .next = qc->h3s->cids[b]};
  qc->h3s->cids[b] = e;
  qc->cids[qc->nb_cids++] = *cid;
  return 0;
}

/* Removes the table entry routing `cid` to qc. */
static void cid_remove(grpc_quic_conn *qc, const ngtcp2_cid *cid) {
  for (cid_entry **p = &qc->h3s->cids[cid_hash(cid)]; *p != nullptr; p = &(*p)->next) {
    if ((*p)->qc == qc && ngtcp2_cid_eq(&(*p)->cid, cid)) {
      cid_entry *e = *p;
      *p = e->next;
      grpc_free(e);
      break;
    }
  }
  for (size_t i = 0; i < qc->nb_cids; i++) {
    if (ngtcp2_cid_eq(&qc->cids[i], cid)) {
      qc->cids[i] = qc->cids[--qc->nb_cids];
      break;
    }
  }
}

/* Sends one datagram on the connection's socket. A full socket buffer drops it; QUIC loss
 * recovery retransmits. */
static void udp_send(grpc_server *server, uv_udp_t *udp, const struct sockaddr *to,
                     const uint8_t *data, size_t len) {
  uv_buf_t buf = uv_buf_init((char *)data, (unsigned int)len);
  int rv = uv_udp_try_send(udp, &buf, 1, to);
  if (rv >= 0)
    atomic_fetch_add(&server->stat_bytes_sent, (uint64_t)len);
  else
    LOG_DEBUG(server, "http3: udp send: %s\n", uv_strerror(rv));
}

/* ---- per-stream state ---- */

/* Frees a stream's send chunks and parked headers. */
static void h3_stream_free(grpc_stream *stream) {
  h3_stream *hs = stream->transport;
  if (hs == nullptr)
    return;
  while (hs->head != nullptr) {
    h3_chunk *c = hs->head;
    hs->head = c->next;
    grpc_free(c);
  }
  grpc_free(hs->headers);
  grpc_free(hs);
  stream->transport = nullptr;
}

/* Deep-copies nva into one allocation (array, then names and values). */
static grpc_nv *nv_copy(const grpc_nv *nva, size_t nvlen) {
  size_t size = nvlen * sizeof(*nva);
  for (size_t i = 0; i < nvlen; i++)
    size += nva[i].namelen + nva[i].valuelen;
  grpc_nv *copy = grpc_malloc(size ? size : 1);
  if (copy == nullptr)
    return nullptr;
  uint8_t *p = (uint8_t *)(copy + nvlen);
  for (size_t i = 0; i < nvlen; i++) {
    copy[i] = nva[i];
    copy[i].name = p;
    memcpy(p, nva[i].name, nva[i].namelen);
    p += nva[i].namelen;
    copy[i].value = p;
    memcpy(p, nva[i].value, nva[i].valuelen);
    p += nva[i].valuelen;
  }
  return copy;
}

/* ---- nghttp3 callbacks ---- */

/* nghttp3 data reader for response bodies. Pulls queued messages into a new chunk (kept until
 * acknowledged) and returns it as one vector. When the queue is empty: ends the stream for
 * gRPC-Web, submits parked trailers (EOF without END_STREAM), or blocks until start_data()
 * resumes the stream. */
static nghttp3_ssize h3_read_data(nghttp3_conn *conn, int64_t stream_id, nghttp3_vec *vec,
                                  size_t veccnt, uint32_t *pflags, void *conn_user_data,
                                  void *stream_user_data) {
  (void)conn;
  (void)stream_id;
  grpc_quic_conn *qc = conn_user_data;
  grpc_stream *stream = stream_user_data;
  h3_stream *hs = stream != nullptr ? stream->transport : nullptr;
  if (hs == nullptr || veccnt == 0)
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  if (hs->body_done) {
    *pflags = NGHTTP3_DATA_FLAG_EOF | NGHTTP3_DATA_FLAG_NO_END_STREAM;
    return 0;
  }

  h3_chunk *chunk = grpc_malloc(sizeof(*chunk));
  if (chunk == nullptr)
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  bool eof = false, end_stream = false;
  chunk->len = stream_core_pull(stream, chunk->data, sizeof(chunk->data), &eof, &end_stream);
  chunk->next = nullptr;
  nghttp3_ssize cnt = 0;
  if (chunk->len > 0) {
    if (hs->tail != nullptr)
      hs->tail->next = chunk;
    else
      hs->head = chunk;
    hs->tail = chunk;
    vec[0] = (nghttp3_vec){.base = chunk->data, .len = chunk->len};
    cnt = 1;
  } else {
    grpc_free(chunk);
  }

  if (eof) {
    if (end_stream) {
      hs->body_done = true;
      *pflags = NGHTTP3_DATA_FLAG_EOF;
    } else if (stream->pending_trailers != nullptr) {
      hs->body_done = true;
      *pflags = NGHTTP3_DATA_FLAG_EOF | NGHTTP3_DATA_FLAG_NO_END_STREAM;
      if (stream_core_flush_pending_trailers(qc->session, stream) != 0)
        return NGHTTP3_ERR_CALLBACK_FAILURE;
    } else if (cnt == 0) {
      return NGHTTP3_ERR_WOULDBLOCK;
    }
  }
  return cnt;
}

static const nghttp3_data_reader h3_data_reader = {.read_data = h3_read_data};

/* nghttp3 acked_stream_data: QUIC acknowledged datalen more body bytes; frees chunks that are
 * fully acknowledged. */
static int h3_on_acked_stream_data(nghttp3_conn *conn, int64_t stream_id, uint64_t datalen,
                                   void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_id;
  (void)conn_user_data;
  grpc_stream *stream = stream_user_data;
  h3_stream *hs = stream != nullptr ? stream->transport : nullptr;
  while (hs != nullptr && datalen > 0 && hs->head != nullptr) {
    size_t left = hs->head->len - hs->head_acked;
    size_t take = datalen < left ? (size_t)datalen : left;
    hs->head_acked += take;
    datalen -= take;
    if (hs->head_acked == hs->head->len) {
      h3_chunk *c = hs->head;
      hs->head = c->next;
      if (hs->head == nullptr)
        hs->tail = nullptr;
      hs->head_acked = 0;
      grpc_free(c);
    }
  }
  return 0;
}

/* nghttp3 begin_headers: a request stream starts; allocates its grpc_stream. */
static int h3_on_begin_headers(nghttp3_conn *conn, int64_t stream_id, void *conn_user_data,
                               void *stream_user_data) {
  grpc_quic_conn *qc = conn_user_data;
  if (stream_user_data != nullptr)
    return 0;
  grpc_stream *stream = stream_new(stream_id, &grpc_h3_transport_ops);
  if (stream == nullptr)
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  stream->transport = grpc_calloc(1, sizeof(h3_stream));
  if (stream->transport == nullptr) {
    stream_free(stream);
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  }
  stream_link(qc->session, stream);
  nghttp3_conn_set_stream_user_data(conn, stream_id, stream);
  return 0;
}

/* nghttp3 recv_header: hands the field and its rcbufs to the stream core. Too many fields reset
 * just this stream. */
static int h3_on_recv_header(nghttp3_conn *conn, int64_t stream_id, int32_t token,
                             nghttp3_rcbuf *name, nghttp3_rcbuf *value, uint8_t flags,
                             void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_id;
  (void)token;
  (void)flags;
  grpc_quic_conn *qc = conn_user_data;
  grpc_stream *stream = stream_user_data;
  if (stream == nullptr || stream->is_discarding)
    return 0;
  nghttp3_vec n = nghttp3_rcbuf_get_buf(name);
  nghttp3_vec v = nghttp3_rcbuf_get_buf(value);
  if (stream_core_on_header(qc->session, stream, n.base, n.len, v.base, v.len, name, value) != 0) {
    stream->is_discarding = true;
    grpc_h3_transport_ops.reset_stream(qc->session, stream, GRPC_RESET_INTERNAL_ERROR);
  }
  return 0;
}

/* nghttp3 end_headers: the request header block is complete. */
static int h3_on_end_headers(nghttp3_conn *conn, int64_t stream_id, int fin, void *conn_user_data,
                             void *stream_user_data) {
  (void)conn;
  (void)stream_id;
  grpc_quic_conn *qc = conn_user_data;
  grpc_stream *stream = stream_user_data;
  if (stream != nullptr)
    stream_core_on_headers_end(qc->session, stream, fin != 0);
  return 0;
}

/* Returns credit for consumed body bytes to the peer (stream and connection flow control). */
static void h3_consume(grpc_quic_conn *qc, int64_t stream_id, size_t n) {
  ngtcp2_conn_extend_max_stream_offset(qc->conn, stream_id, n);
  ngtcp2_conn_extend_max_offset(qc->conn, n);
}

/* nghttp3 recv_data: a chunk of request body; buffered and parsed by the stream core. */
static int h3_on_recv_data(nghttp3_conn *conn, int64_t stream_id, const uint8_t *data,
                           size_t datalen, void *conn_user_data, void *stream_user_data) {
  (void)conn;
  grpc_quic_conn *qc = conn_user_data;
  grpc_stream *stream = stream_user_data;
  h3_consume(qc, stream_id, datalen);
  if (stream == nullptr)
    return 0;
  if (stream_core_on_data(qc->session, stream, data, datalen) != 0)
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  stream_core_on_data_end(qc->session, stream, false);
  return 0;
}

/* nghttp3 deferred_consume: framing bytes nghttp3 consumed later (e.g. after QPACK unblocking). */
static int h3_on_deferred_consume(nghttp3_conn *conn, int64_t stream_id, size_t consumed,
                                  void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  h3_consume(conn_user_data, stream_id, consumed);
  return 0;
}

/* nghttp3 end_stream: the client finished its request body. */
static int h3_on_end_stream(nghttp3_conn *conn, int64_t stream_id, void *conn_user_data,
                            void *stream_user_data) {
  (void)conn;
  (void)stream_id;
  grpc_quic_conn *qc = conn_user_data;
  grpc_stream *stream = stream_user_data;
  if (stream != nullptr)
    stream_core_on_data_end(qc->session, stream, true);
  return 0;
}

/* nghttp3 stream_close: the request stream is gone; detaches its call and frees it. A finished
 * client stream also makes room for one more. */
static int h3_on_stream_close(nghttp3_conn *conn, int64_t stream_id, uint64_t app_error_code,
                              void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)app_error_code;
  grpc_quic_conn *qc = conn_user_data;
  grpc_stream *stream = stream_user_data;
  if (ngtcp2_is_bidi_stream(stream_id))
    ngtcp2_conn_extend_max_streams_bidi(qc->conn, 1);
  if (stream != nullptr) {
    h3_stream_free(stream);
    stream_core_on_close(qc->session, stream);
  }
  return 0;
}

/* nghttp3 stop_sending: the peer asked us to stop sending on the stream. */
static int h3_on_stop_sending(nghttp3_conn *conn, int64_t stream_id, uint64_t app_error_code,
                              void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  grpc_quic_conn *qc = conn_user_data;
  (void)ngtcp2_conn_shutdown_stream_read(qc->conn, 0, stream_id, app_error_code);
  return 0;
}

/* nghttp3 reset_stream: nghttp3 wants the stream's sending side reset. */
static int h3_on_reset_stream(nghttp3_conn *conn, int64_t stream_id, uint64_t app_error_code,
                              void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  grpc_quic_conn *qc = conn_user_data;
  (void)ngtcp2_conn_shutdown_stream_write(qc->conn, 0, stream_id, app_error_code);
  return 0;
}

/* Creates the server's nghttp3 connection and its control and QPACK streams. Called when the
 * 1-RTT keys are installed, so the HTTP/3 layer exists before any request stream arrives. */
static int h3_setup(grpc_quic_conn *qc) {
  nghttp3_callbacks callbacks = {
      .acked_stream_data = h3_on_acked_stream_data,
      .stream_close = h3_on_stream_close,
      .recv_data = h3_on_recv_data,
      .deferred_consume = h3_on_deferred_consume,
      .begin_headers = h3_on_begin_headers,
      .recv_header = h3_on_recv_header,
      .end_headers = h3_on_end_headers,
      .stop_sending = h3_on_stop_sending,
      .end_stream = h3_on_end_stream,
      .reset_stream = h3_on_reset_stream,
  };
  nghttp3_settings settings;
  nghttp3_settings_default(&settings);
  settings.qpack_max_dtable_capacity = 4096;
  settings.qpack_blocked_streams = 100;
  if (nghttp3_conn_server_new(&qc->h3, &callbacks, &settings, nullptr, qc) != 0)
    return -1;

  const ngtcp2_transport_params *params = ngtcp2_conn_get_local_transport_params(qc->conn);
  nghttp3_conn_set_max_client_streams_bidi(qc->h3, params->initial_max_streams_bidi);

  int64_t ctrl_id, qenc_id, qdec_id;
  if (ngtcp2_conn_open_uni_stream(qc->conn, &ctrl_id, nullptr) != 0 ||
      nghttp3_conn_bind_control_stream(qc->h3, ctrl_id) != 0 ||
      ngtcp2_conn_open_uni_stream(qc->conn, &qenc_id, nullptr) != 0 ||
      ngtcp2_conn_open_uni_stream(qc->conn, &qdec_id, nullptr) != 0 ||
      nghttp3_conn_bind_qpack_streams(qc->h3, qenc_id, qdec_id) != 0)
    return -1;
  return 0;
}

/* ---- ngtcp2 callbacks ---- */

/* ngtcp2 recv_stream_data: feeds stream bytes to nghttp3 and returns credit for the framing it
 * consumed (body bytes are credited in h3_on_recv_data). */
static int cb_recv_stream_data(ngtcp2_conn *conn, uint32_t flags, int64_t stream_id,
                               uint64_t offset, const uint8_t *data, size_t datalen,
                               void *user_data, void *stream_user_data) {
  (void)conn;
  (void)offset;
  (void)stream_user_data;
  grpc_quic_conn *qc = user_data;
  if (qc->h3 == nullptr)
    return 0;
  nghttp3_ssize nconsumed = nghttp3_conn_read_stream(qc->h3, stream_id, data, datalen,
                                                     (flags & NGTCP2_STREAM_DATA_FLAG_FIN) != 0);
  if (nconsumed < 0) {
    ngtcp2_ccerr_set_application_error(
        &qc->ccerr, nghttp3_err_infer_quic_app_error_code((int)nconsumed), nullptr, 0);
    return NGTCP2_ERR_CALLBACK_FAILURE;
  }
  h3_consume(qc, stream_id, (size_t)nconsumed);
  return 0;
}

/* ngtcp2 acked_stream_data_offset: tells nghttp3 how much of a stream was acknowledged. */
static int cb_acked_stream_data_offset(ngtcp2_conn *conn, int64_t stream_id, uint64_t offset,
                                       uint64_t datalen, void *user_data, void *stream_user_data) {
  (void)conn;
  (void)offset;
  (void)stream_user_data;
  grpc_quic_conn *qc = user_data;
  if (qc->h3 != nullptr && nghttp3_conn_add_ack_offset(qc->h3, stream_id, datalen) != 0)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}

/* ngtcp2 stream_close: forwards to nghttp3 (which calls h3_on_stream_close). */
static int cb_stream_close(ngtcp2_conn *conn, uint32_t flags, int64_t stream_id,
                           uint64_t app_error_code, void *user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  grpc_quic_conn *qc = user_data;
  if (qc->h3 == nullptr)
    return 0;
  if (!(flags & NGTCP2_STREAM_CLOSE_FLAG_APP_ERROR_CODE_SET))
    app_error_code = NGHTTP3_H3_NO_ERROR;
  int rv = nghttp3_conn_close_stream(qc->h3, stream_id, app_error_code);
  if (rv != 0 && rv != NGHTTP3_ERR_STREAM_NOT_FOUND) {
    ngtcp2_ccerr_set_application_error(&qc->ccerr, nghttp3_err_infer_quic_app_error_code(rv),
                                       nullptr, 0);
    return NGTCP2_ERR_CALLBACK_FAILURE;
  }
  return 0;
}

/* ngtcp2 stream_reset / stream_stop_sending: the peer abandoned a direction of the stream. */
static int cb_stream_reset(ngtcp2_conn *conn, int64_t stream_id, uint64_t final_size,
                           uint64_t app_error_code, void *user_data, void *stream_user_data) {
  (void)conn;
  (void)final_size;
  (void)app_error_code;
  (void)stream_user_data;
  grpc_quic_conn *qc = user_data;
  if (qc->h3 != nullptr && nghttp3_conn_shutdown_stream_read(qc->h3, stream_id) != 0)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}
static int cb_stream_stop_sending(ngtcp2_conn *conn, int64_t stream_id, uint64_t app_error_code,
                                  void *user_data, void *stream_user_data) {
  return cb_stream_reset(conn, stream_id, 0, app_error_code, user_data, stream_user_data);
}

/* ngtcp2 extend_max_remote_streams_bidi: the client may open more request streams. */
static int cb_extend_max_remote_streams_bidi(ngtcp2_conn *conn, uint64_t max_streams,
                                             void *user_data) {
  (void)conn;
  grpc_quic_conn *qc = user_data;
  if (qc->h3 != nullptr)
    nghttp3_conn_set_max_client_streams_bidi(qc->h3, max_streams);
  return 0;
}

/* ngtcp2 extend_max_stream_data: the peer opened its window; a blocked stream may write. */
static int cb_extend_max_stream_data(ngtcp2_conn *conn, int64_t stream_id, uint64_t max_data,
                                     void *user_data, void *stream_user_data) {
  (void)conn;
  (void)max_data;
  (void)stream_user_data;
  grpc_quic_conn *qc = user_data;
  if (qc->h3 != nullptr && nghttp3_conn_unblock_stream(qc->h3, stream_id) != 0)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}

/* ngtcp2 recv_tx_key: once the 1-RTT keys exist, set up HTTP/3. */
static int cb_recv_tx_key(ngtcp2_conn *conn, ngtcp2_encryption_level level, void *user_data) {
  (void)conn;
  grpc_quic_conn *qc = user_data;
  if (level != NGTCP2_ENCRYPTION_LEVEL_1RTT || qc->h3 != nullptr)
    return 0;
  return h3_setup(qc) == 0 ? 0 : NGTCP2_ERR_CALLBACK_FAILURE;
}

/* ngtcp2 handshake_completed: records the verified client certificate subject. */
static int cb_handshake_completed(ngtcp2_conn *conn, void *user_data) {
  (void)conn;
  grpc_quic_conn *qc = user_data;
  qc->session->tls_handshake_done = true;
  if (quic_tls_peer_subject(qc->tls, qc->session->peer_cert_subject,
                            sizeof(qc->session->peer_cert_subject)) != 0)
    qc->session->peer_cert_subject[0] = '\0';
  return 0;
}

/* ngtcp2 get_new_connection_id: issues a random connection ID with its stateless reset token and
 * routes it to this connection. */
static int cb_get_new_connection_id(ngtcp2_conn *conn, ngtcp2_cid *cid, uint8_t *token,
                                    size_t cidlen, void *user_data) {
  (void)conn;
  grpc_quic_conn *qc = user_data;
  uint8_t buf[NGTCP2_MAX_CIDLEN];
  h3_random(buf, cidlen);
  ngtcp2_cid_init(cid, buf, cidlen);
  if (ngtcp2_crypto_generate_stateless_reset_token(token, qc->h3s->secret, sizeof(qc->h3s->secret),
                                                   cid) != 0 ||
      cid_add(qc, cid) != 0)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}

/* ngtcp2 remove_connection_id: the ID is retired; stop routing it. */
static int cb_remove_connection_id(ngtcp2_conn *conn, const ngtcp2_cid *cid, void *user_data) {
  (void)conn;
  cid_remove(user_data, cid);
  return 0;
}

/* ngtcp2_crypto_conn_ref::get_conn: the TLS callbacks reach the ngtcp2_conn through this. */
static ngtcp2_conn *get_conn(ngtcp2_crypto_conn_ref *ref) {
  return ((grpc_quic_conn *)ref->user_data)->conn;
}

/* ---- connection lifecycle ---- */

static void on_qc_timer(uv_timer_t *timer);

/* Re-arms the connection timer for ngtcp2's next deadline. */
static void qc_arm_timer(grpc_quic_conn *qc) {
  ngtcp2_tstamp expiry = ngtcp2_conn_get_expiry(qc->conn);
  if (expiry == UINT64_MAX) {
    uv_timer_stop(&qc->timer);
    return;
  }
  ngtcp2_tstamp now = h3_now();
  uint64_t ms = expiry > now ? (expiry - now + NGTCP2_MILLISECONDS - 1) / NGTCP2_MILLISECONDS : 0;
  uv_timer_start(&qc->timer, on_qc_timer, ms, 0);
}

/* Connection timer: lets ngtcp2 handle the expiry (retransmission, ACK, keepalive, idle), then
 * writes. An idle timeout or fatal error closes the session. */
static void on_qc_timer(uv_timer_t *timer) {
  grpc_quic_conn *qc = timer->data;
  int rv = ngtcp2_conn_handle_expiry(qc->conn, h3_now());
  if (rv != 0) {
    if (rv != NGTCP2_ERR_IDLE_CLOSE)
      ngtcp2_ccerr_set_liberr(&qc->ccerr, rv, nullptr, 0);
    else
      qc->ccerr.type = NGTCP2_CCERR_TYPE_IDLE_CLOSE;
    session_close(qc->session);
    return;
  }
  qc->h3s->server->ready_events++;
  h3_session_flush(qc->session);
}

/* uv_close callback of the connection timer: the last step of closing; returns the slot. */
static void on_qc_timer_closed(uv_handle_t *handle) {
  grpc_quic_conn *qc = handle->data;
  grpc_session *session = qc->session;
  grpc_free(qc->cids);
  grpc_free(qc);
  session->quic = nullptr;
  session_slot_release(session);
}

/* close op: sends CONNECTION_CLOSE (unless the connection is draining or closed by idle timeout),
 * stops routing its connection IDs, detaches every call with UNAVAILABLE, frees nghttp3, ngtcp2
 * and TLS state, and closes the timer; its close callback returns the slot. */
static void h3_session_close(grpc_session *session) {
  grpc_quic_conn *qc = session->quic;
  session->is_closing = true;
  LOG_DEBUG(session->server, "http3: closing connection from %s\n", session->peer);

  if (qc->conn != nullptr && qc->ccerr.type != NGTCP2_CCERR_TYPE_IDLE_CLOSE &&
      !ngtcp2_conn_in_draining_period(qc->conn) && !ngtcp2_conn_in_closing_period(qc->conn)) {
    ngtcp2_path_storage ps;
    ngtcp2_path_storage_zero(&ps);
    ngtcp2_pkt_info pi;
    ngtcp2_ssize n = ngtcp2_conn_write_connection_close(
        qc->conn, &ps.path, &pi, qc->h3s->sbuf, sizeof(qc->h3s->sbuf), &qc->ccerr, h3_now());
    if (n > 0)
      udp_send(session->server, qc->udp, (const struct sockaddr *)&qc->remote, qc->h3s->sbuf,
               (size_t)n);
  }

  while (qc->nb_cids > 0)
    cid_remove(qc, &qc->cids[qc->nb_cids - 1]);

  grpc_stream *stream;
  while ((stream = session->streams) != nullptr) {
    stream_unlink(session, stream);
    if (stream->call != nullptr)
      call_detach(stream->call, GRPC_STATUS_UNAVAILABLE);
    h3_stream_free(stream);
    stream_free(stream);
  }

  if (qc->h3 != nullptr)
    nghttp3_conn_del(qc->h3);
  qc->h3 = nullptr;
  if (qc->conn != nullptr)
    ngtcp2_conn_del(qc->conn);
  qc->conn = nullptr;
  quic_tls_session_free(qc->tls);
  qc->tls = nullptr;

  uv_timer_stop(&qc->timer);
  uv_close((uv_handle_t *)&qc->timer, on_qc_timer_closed);
}

/* flush op: writes everything ngtcp2 and nghttp3 have to send, packet by packet, then re-arms
 * the timer. Errors close the session. */
static void h3_session_flush(grpc_session *session) {
  grpc_quic_conn *qc = session->quic;
  if (!session->is_active || session->is_closing || qc == nullptr || qc->conn == nullptr)
    return;
  ngtcp2_tstamp ts = h3_now();
  uint8_t *buf = qc->h3s->sbuf;

  for (;;) {
    int64_t stream_id = -1;
    int fin = 0;
    nghttp3_vec vec[16];
    nghttp3_ssize sveccnt = 0;
    if (qc->h3 != nullptr && ngtcp2_conn_get_max_data_left(qc->conn)) {
      sveccnt = nghttp3_conn_writev_stream(qc->h3, &stream_id, &fin, vec, 16);
      if (sveccnt < 0) {
        ngtcp2_ccerr_set_application_error(
            &qc->ccerr, nghttp3_err_infer_quic_app_error_code((int)sveccnt), nullptr, 0);
        session_close(session);
        return;
      }
    }

    ngtcp2_ssize ndatalen = -1;
    ngtcp2_path_storage ps;
    ngtcp2_path_storage_zero(&ps);
    ngtcp2_pkt_info pi;
    uint32_t flags = NGTCP2_WRITE_STREAM_FLAG_MORE | (fin ? NGTCP2_WRITE_STREAM_FLAG_FIN : 0);
    ngtcp2_ssize nwrite =
        ngtcp2_conn_writev_stream(qc->conn, &ps.path, &pi, buf, H3_MAX_UDP_PAYLOAD, &ndatalen,
                                  flags, stream_id, (const ngtcp2_vec *)vec, (size_t)sveccnt, ts);
    if (nwrite < 0) {
      switch (nwrite) {
      case NGTCP2_ERR_STREAM_DATA_BLOCKED:
        nghttp3_conn_block_stream(qc->h3, stream_id);
        continue;
      case NGTCP2_ERR_STREAM_SHUT_WR:
        nghttp3_conn_shutdown_stream_write(qc->h3, stream_id);
        continue;
      case NGTCP2_ERR_WRITE_MORE:
        if (nghttp3_conn_add_write_offset(qc->h3, stream_id, (size_t)ndatalen) != 0)
          goto fail;
        continue;
      default:
        ngtcp2_ccerr_set_liberr(&qc->ccerr, (int)nwrite, nullptr, 0);
        goto fail;
      }
    }
    if (ndatalen >= 0 && stream_id >= 0 &&
        nghttp3_conn_add_write_offset(qc->h3, stream_id, (size_t)ndatalen) != 0)
      goto fail;
    if (nwrite == 0)
      break;
    udp_send(session->server, qc->udp, (const struct sockaddr *)&qc->remote, buf, (size_t)nwrite);
  }
  ngtcp2_conn_update_pkt_tx_time(qc->conn, ts);
  qc_arm_timer(qc);
  return;

fail:
  session_close(session);
}

/* Creates the QUIC connection for the first Initial packet `hd` from `remote`, in a new session
 * slot. odcid is the original destination CID recovered from a Retry token (nullptr without
 * Retry). Returns nullptr when out of slots or on failure (the slot is returned). */
static grpc_quic_conn *qc_new(grpc_h3_server *h3s, uv_udp_t *udp, const struct sockaddr *remote,
                              socklen_t remotelen, const ngtcp2_pkt_hd *hd,
                              const ngtcp2_cid *odcid) {
  grpc_server *server = h3s->server;
  if (server->nb_free_sessions == 0) {
    LOG_WARNING(server, "max_connections (%zu) reached, dropping an HTTP/3 connection\n",
                server->nb_sessions);
    return nullptr;
  }
  grpc_quic_conn *qc = grpc_calloc(1, sizeof(*qc));
  if (qc == nullptr)
    return nullptr;
  grpc_session *session = session_slot_acquire(server);
  qc->session = session;
  qc->h3s = h3s;
  qc->udp = udp;
  session->quic = qc;
  session->ops = &grpc_h3_transport_ops;
  session->is_tls = true;
  memcpy(&qc->remote, remote, remotelen);
  qc->remotelen = remotelen;
  memcpy(&session->sockaddr, remote, remotelen);
  session->socklen = remotelen;
  format_peer(remote, session->peer, sizeof(session->peer));
  if (remote->sa_family == AF_INET6) {
    memcpy(&qc->local, &h3s->local6, sizeof(qc->local));
    qc->locallen = sizeof(struct sockaddr_in6);
  } else {
    memcpy(&qc->local, &h3s->local4, sizeof(qc->local));
    qc->locallen = sizeof(struct sockaddr_in);
  }
  ngtcp2_ccerr_default(&qc->ccerr);
  uv_timer_init(&server->loop, &qc->timer);
  qc->timer.data = qc;
  /* from here on, session_close() frees everything and returns the slot */

  const grpc_config *cfg = &server->config;
  uint8_t scid_buf[H3_SCID_LEN];
  h3_random(scid_buf, sizeof(scid_buf));
  ngtcp2_cid scid;
  ngtcp2_cid_init(&scid, scid_buf, sizeof(scid_buf));

  ngtcp2_path path = {
      .local = {.addr = (ngtcp2_sockaddr *)&qc->local, .addrlen = qc->locallen},
      .remote = {.addr = (ngtcp2_sockaddr *)&qc->remote, .addrlen = qc->remotelen},
  };
  ngtcp2_callbacks callbacks = {
      .recv_client_initial = ngtcp2_crypto_recv_client_initial_cb,
      .recv_crypto_data = ngtcp2_crypto_recv_crypto_data_cb,
      .handshake_completed = cb_handshake_completed,
      .encrypt = ngtcp2_crypto_encrypt_cb,
      .decrypt = ngtcp2_crypto_decrypt_cb,
      .hp_mask = ngtcp2_crypto_hp_mask_cb,
      .recv_stream_data = cb_recv_stream_data,
      .acked_stream_data_offset = cb_acked_stream_data_offset,
      .stream_close = cb_stream_close,
      .rand = cb_rand,
      .get_new_connection_id = cb_get_new_connection_id,
      .remove_connection_id = cb_remove_connection_id,
      .update_key = ngtcp2_crypto_update_key_cb,
      .stream_reset = cb_stream_reset,
      .extend_max_remote_streams_bidi = cb_extend_max_remote_streams_bidi,
      .extend_max_stream_data = cb_extend_max_stream_data,
      .delete_crypto_aead_ctx = ngtcp2_crypto_delete_crypto_aead_ctx_cb,
      .delete_crypto_cipher_ctx = ngtcp2_crypto_delete_crypto_cipher_ctx_cb,
      .get_path_challenge_data = ngtcp2_crypto_get_path_challenge_data_cb,
      .stream_stop_sending = cb_stream_stop_sending,
      .version_negotiation = ngtcp2_crypto_version_negotiation_cb,
      .recv_tx_key = cb_recv_tx_key,
  };
  ngtcp2_settings settings;
  ngtcp2_settings_default(&settings);
  settings.initial_ts = h3_now();
  settings.max_tx_udp_payload_size = H3_MAX_UDP_PAYLOAD;
  if (hd->tokenlen > 0) {
    settings.token = hd->token;
    settings.tokenlen = hd->tokenlen;
    settings.token_type = NGTCP2_TOKEN_TYPE_RETRY;
  }

  /* Windows: as in HTTP/2, a whole message must fit in a stream's window; the connection window
   * leaves room for several concurrent messages. */
  uint64_t window = cfg->max_message_size;
  ngtcp2_transport_params params;
  ngtcp2_transport_params_default(&params);
  params.initial_max_stream_data_bidi_local = window;
  params.initial_max_stream_data_bidi_remote = window;
  params.initial_max_stream_data_uni = window;
  params.initial_max_data = window * 4 > 16 * 1024 * 1024 ? window * 4 : 16 * 1024 * 1024;
  params.initial_max_streams_bidi = cfg->max_concurrent_streams;
  params.initial_max_streams_uni = 3;
  uint32_t idle = cfg->http3.max_idle_timeout_ms ? cfg->http3.max_idle_timeout_ms
                  : cfg->max_connection_idle_ms  ? cfg->max_connection_idle_ms
                                                 : 30000;
  params.max_idle_timeout = (ngtcp2_duration)idle * NGTCP2_MILLISECONDS;
  params.active_connection_id_limit = 7;
  params.disable_active_migration = 1;
  params.stateless_reset_token_present = 1;
  if (odcid != nullptr) {
    params.original_dcid = *odcid;
    params.retry_scid = hd->dcid;
    params.retry_scid_present = 1;
  } else {
    params.original_dcid = hd->dcid;
  }
  params.original_dcid_present = 1;
  if (ngtcp2_crypto_generate_stateless_reset_token(params.stateless_reset_token, h3s->secret,
                                                   sizeof(h3s->secret), &scid) != 0)
    goto fail;

  if (ngtcp2_conn_server_new(&qc->conn, &hd->scid, &scid, &path, hd->version, &callbacks, &settings,
                             &params, nullptr, qc) != 0)
    goto fail;
  qc->conn_ref = (ngtcp2_crypto_conn_ref){.get_conn = get_conn, .user_data = qc};
  qc->tls = quic_tls_session_new(h3s->tls_ctx, &qc->conn_ref, true, nullptr);
  if (qc->tls == nullptr)
    goto fail;
  ngtcp2_conn_set_tls_native_handle(qc->conn, quic_tls_session_native(qc->tls));
  if (cfg->keepalive_time_ms > 0)
    ngtcp2_conn_set_keep_alive_timeout(qc->conn, (ngtcp2_duration)cfg->keepalive_time_ms *
                                                     NGTCP2_MILLISECONDS);
  /* retransmitted Initials still carry the client-chosen DCID */
  if (cid_add(qc, &scid) != 0 || cid_add(qc, &hd->dcid) != 0)
    goto fail;
  LOG_DEBUG(server, "http3: connection from %s\n", session->peer);
  return qc;

fail:
  LOG_ERROR(server, "http3: cannot set up the connection from %s\n", session->peer);
  qc->ccerr.type = NGTCP2_CCERR_TYPE_IDLE_CLOSE; /* nothing to send */
  session_close(session);
  return nullptr;
}

/* Answers a packet of an unsupported QUIC version with a Version Negotiation packet. */
static void send_version_negotiation(grpc_h3_server *h3s, uv_udp_t *udp, const struct sockaddr *to,
                                     const ngtcp2_version_cid *vc) {
  uint32_t versions[] = {NGTCP2_PROTO_VER_V1, NGTCP2_PROTO_VER_V2};
  uint8_t unused;
  h3_random(&unused, 1);
  ngtcp2_ssize n = ngtcp2_pkt_write_version_negotiation(
      h3s->sbuf, sizeof(h3s->sbuf), unused, vc->scid, vc->scidlen, vc->dcid, vc->dcidlen, versions,
      sizeof(versions) / sizeof(versions[0]));
  if (n > 0)
    udp_send(h3s->server, udp, to, h3s->sbuf, (size_t)n);
}

/* Sends a Retry for an Initial without a token: a fresh server CID plus a token binding the
 * client address and original DCID, so only a client that can receive at its address proceeds. */
static void send_retry(grpc_h3_server *h3s, uv_udp_t *udp, const struct sockaddr *to,
                       socklen_t tolen, const ngtcp2_pkt_hd *hd) {
  uint8_t scid_buf[H3_SCID_LEN];
  h3_random(scid_buf, sizeof(scid_buf));
  ngtcp2_cid scid;
  ngtcp2_cid_init(&scid, scid_buf, sizeof(scid_buf));
  uint8_t token[NGTCP2_CRYPTO_MAX_RETRY_TOKENLEN2];
  ngtcp2_ssize tokenlen = ngtcp2_crypto_generate_retry_token2(
      token, h3s->secret, sizeof(h3s->secret), hd->version, (const ngtcp2_sockaddr *)to, tolen,
      &scid, &hd->dcid, h3_now());
  if (tokenlen < 0)
    return;
  ngtcp2_ssize n = ngtcp2_crypto_write_retry(h3s->sbuf, sizeof(h3s->sbuf), hd->version, &hd->scid,
                                             &scid, &hd->dcid, token, (size_t)tokenlen);
  if (n > 0)
    udp_send(h3s->server, udp, to, h3s->sbuf, (size_t)n);
}

/* Handles a packet for no known connection: version negotiation, then (with require_retry) the
 * Retry exchange, then a new connection. Returns the connection to feed the packet to, or
 * nullptr to drop it. */
static grpc_quic_conn *accept_packet(grpc_h3_server *h3s, uv_udp_t *udp, const uint8_t *data,
                                     size_t len, const struct sockaddr *from, socklen_t fromlen) {
  if (!h3s->accepting)
    return nullptr;
  ngtcp2_pkt_hd hd;
  if (ngtcp2_accept(&hd, data, len) != 0)
    return nullptr;
  ngtcp2_cid odcid;
  bool retried = false;
  if (h3s->server->config.http3.require_retry) {
    if (hd.tokenlen == 0) {
      send_retry(h3s, udp, from, fromlen, &hd);
      return nullptr;
    }
    if (ngtcp2_crypto_verify_retry_token2(&odcid, hd.token, hd.tokenlen, h3s->secret,
                                          sizeof(h3s->secret), hd.version,
                                          (const ngtcp2_sockaddr *)from, fromlen, &hd.dcid,
                                          H3_RETRY_TOKEN_TIMEOUT, h3_now()) != 0)
      return nullptr;
    retried = true;
  } else {
    hd.token = nullptr;
    hd.tokenlen = 0;
  }
  return qc_new(h3s, udp, from, fromlen, &hd, retried ? &odcid : nullptr);
}

/* libuv alloc callback for the UDP sockets: the server-wide datagram buffer. */
static void on_udp_alloc(uv_handle_t *handle, size_t suggested, uv_buf_t *buf) {
  (void)suggested;
  grpc_h3_server *h3s = handle->data;
  *buf = uv_buf_init((char *)h3s->rbuf, sizeof(h3s->rbuf));
}

/* UDP read callback: routes the datagram to its connection by destination CID (creating one for a
 * new client) and feeds it to ngtcp2. Errors close that connection only. */
static void on_udp_read(uv_udp_t *udp, ssize_t nread, const uv_buf_t *buf,
                        const struct sockaddr *addr, unsigned flags) {
  (void)flags;
  grpc_h3_server *h3s = udp->data;
  if (nread <= 0 || addr == nullptr)
    return;
  grpc_server *server = h3s->server;
  atomic_fetch_add(&server->stat_bytes_received, (uint64_t)nread);
  server->ready_events++;
  const uint8_t *data = (const uint8_t *)buf->base;
  size_t len = (size_t)nread;
  socklen_t addrlen =
      addr->sa_family == AF_INET6 ? sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in);

  ngtcp2_version_cid vc;
  int rv = ngtcp2_pkt_decode_version_cid(&vc, data, len, H3_SCID_LEN);
  if (rv == NGTCP2_ERR_VERSION_NEGOTIATION) {
    send_version_negotiation(h3s, udp, addr, &vc);
    return;
  }
  if (rv != 0)
    return;

  grpc_quic_conn *qc = cid_lookup(h3s, vc.dcid, vc.dcidlen);
  if (qc == nullptr) {
    qc = accept_packet(h3s, udp, data, len, addr, addrlen);
    if (qc == nullptr)
      return;
  }
  if (qc->session->is_closing || qc->conn == nullptr)
    return;

  ngtcp2_path path = {
      .local = {.addr = (ngtcp2_sockaddr *)&qc->local, .addrlen = qc->locallen},
      .remote = {.addr = (ngtcp2_sockaddr *)addr, .addrlen = addrlen},
  };
  ngtcp2_pkt_info pi = {0};
  rv = ngtcp2_conn_read_pkt(qc->conn, &path, &pi, data, len, h3_now());
  if (rv != 0) {
    switch (rv) {
    case NGTCP2_ERR_DRAINING:
    case NGTCP2_ERR_DROP_CONN:
      qc->ccerr.type = NGTCP2_CCERR_TYPE_IDLE_CLOSE; /* nothing more to send */
      break;
    case NGTCP2_ERR_CRYPTO:
      if (qc->ccerr.error_code == 0)
        ngtcp2_ccerr_set_tls_alert(&qc->ccerr, ngtcp2_conn_get_tls_alert(qc->conn), nullptr, 0);
      break;
    default:
      if (qc->ccerr.error_code == 0)
        ngtcp2_ccerr_set_liberr(&qc->ccerr, rv, nullptr, 0);
      break;
    }
    LOG_DEBUG(server, "http3: %s: %s\n", qc->session->peer, ngtcp2_strerror(rv));
    session_close(qc->session);
    return;
  }
  qc->session->last_activity_ms = grpc_time_now_ms();
  session_schedule_flush(qc->session);
}

/* ---- transport ops ---- */

/* submit_headers op. nghttp3 sends headers and body together, so headers that do not end the
 * stream are parked until start_data() or submit_trailers() supplies the body. */
static int h3_submit_headers(grpc_session *session, grpc_stream *stream, const grpc_nv *nva,
                             size_t nvlen, bool end_stream) {
  grpc_quic_conn *qc = session->quic;
  h3_stream *hs = stream->transport;
  if (qc->h3 == nullptr || hs == nullptr || hs->response_submitted)
    return -1;
  if (end_stream) {
    hs->response_submitted = true;
    hs->body_done = true;
    return nghttp3_conn_submit_response(qc->h3, stream->stream_id, (const nghttp3_nv *)nva, nvlen,
                                        nullptr) == 0
               ? 0
               : -1;
  }
  grpc_free(hs->headers);
  hs->headers = nv_copy(nva, nvlen);
  hs->nb_headers = nvlen;
  return hs->headers != nullptr ? 0 : -1;
}

/* Submits the parked response headers with the body data reader. */
static int h3_submit_parked_response(grpc_quic_conn *qc, grpc_stream *stream, h3_stream *hs) {
  if (hs->headers == nullptr)
    return -1;
  int rv = nghttp3_conn_submit_response(qc->h3, stream->stream_id, (const nghttp3_nv *)hs->headers,
                                        hs->nb_headers, &h3_data_reader);
  grpc_free(hs->headers);
  hs->headers = nullptr;
  hs->nb_headers = 0;
  if (rv != 0)
    return -1;
  hs->response_submitted = true;
  return 0;
}

/* start_data op: submits the parked headers with the data reader, or resumes a reader that
 * returned WOULDBLOCK. */
static int h3_start_data(grpc_session *session, grpc_stream *stream) {
  grpc_quic_conn *qc = session->quic;
  h3_stream *hs = stream->transport;
  if (qc->h3 == nullptr || hs == nullptr || hs->body_done)
    return -1;
  if (!hs->response_submitted)
    return h3_submit_parked_response(qc, stream, hs);
  return nghttp3_conn_resume_stream(qc->h3, stream->stream_id) == 0 ? 0 : -1;
}

/* submit_trailers op: ends the body (the reader then returns EOF without END_STREAM) and queues
 * the trailers. */
static int h3_submit_trailers(grpc_session *session, grpc_stream *stream, const grpc_nv *nva,
                              size_t nvlen) {
  grpc_quic_conn *qc = session->quic;
  h3_stream *hs = stream->transport;
  if (qc->h3 == nullptr || hs == nullptr)
    return -1;
  if (!hs->response_submitted) {
    hs->body_done = true;
    if (h3_submit_parked_response(qc, stream, hs) != 0)
      return -1;
  } else if (!hs->body_done) {
    hs->body_done = true;
    (void)nghttp3_conn_resume_stream(qc->h3, stream->stream_id);
  }
  return nghttp3_conn_submit_trailers(qc->h3, stream->stream_id, (const nghttp3_nv *)nva, nvlen) ==
                 0
             ? 0
             : -1;
}

/* reset_stream op: RESET_STREAM and STOP_SENDING with H3_GENERAL_PROTOCOL_ERROR or
 * H3_INTERNAL_ERROR. */
static void h3_reset_stream(grpc_session *session, grpc_stream *stream, grpc_reset_reason reason) {
  grpc_quic_conn *qc = session->quic;
  if (qc->conn == nullptr)
    return;
  (void)ngtcp2_conn_shutdown_stream(qc->conn, 0, stream->stream_id,
                                    reason == GRPC_RESET_PROTOCOL_ERROR
                                        ? NGHTTP3_H3_GENERAL_PROTOCOL_ERROR
                                        : NGHTTP3_H3_INTERNAL_ERROR);
}

/* hdr_incref / hdr_decref ops over nghttp3_rcbuf. */
static void h3_hdr_incref(void *ref) {
  nghttp3_rcbuf_incref(ref);
}
static void h3_hdr_decref(void *ref) {
  nghttp3_rcbuf_decref(ref);
}

/* goaway op: HTTP/3 GOAWAY naming the last accepted request stream. */
static void h3_goaway(grpc_session *session) {
  grpc_quic_conn *qc = session->quic;
  if (qc->h3 != nullptr)
    (void)nghttp3_conn_shutdown(qc->h3);
}

const grpc_transport_ops grpc_h3_transport_ops = {
    .name = "h3",
    .submit_headers = h3_submit_headers,
    .start_data = h3_start_data,
    .submit_trailers = h3_submit_trailers,
    .reset_stream = h3_reset_stream,
    .hdr_incref = h3_hdr_incref,
    .hdr_decref = h3_hdr_decref,
    .goaway = h3_goaway,
    .ping = nullptr, /* QUIC keepalive: ngtcp2_conn_set_keep_alive_timeout() */
    .flush = h3_session_flush,
    .close = h3_session_close,
};

/* ---- server lifecycle ---- */

/* Binds a UDP socket for `family` at `addr`:port and starts reading. Records the bound address
 * (and the port, when it was 0). Returns 0 or a libuv error. */
static int open_udp(grpc_h3_server *h3s, uv_udp_t *udp, struct sockaddr_storage *local, int family,
                    const char *addr, uint16_t *port) {
  grpc_server *server = h3s->server;
  struct sockaddr_storage ss = {0};
  int rv = family == AF_INET ? uv_ip4_addr(addr, *port, (struct sockaddr_in *)&ss)
                             : uv_ip6_addr(addr, *port, (struct sockaddr_in6 *)&ss);
  if (rv != 0)
    return rv;
  if ((rv = uv_udp_init_ex(&server->loop, udp, (unsigned)family)) != 0)
    return rv;
  udp->data = h3s;
#if defined(SO_REUSEPORT) && !defined(_WIN32)
  /* like the TCP listeners: one server per thread can share the port. The kernel spreads datagrams
   * by address 4-tuple, so a connection stays on one loop (migration is disabled) */
  uv_os_fd_t fd;
  if (uv_fileno((uv_handle_t *)udp, &fd) == 0)
    (void)setsockopt((int)fd, SOL_SOCKET, SO_REUSEPORT, &(int){1}, sizeof(int));
#endif
  unsigned flags = UV_UDP_REUSEADDR | (family == AF_INET6 ? UV_UDP_IPV6ONLY : 0);
  if ((rv = uv_udp_bind(udp, (const struct sockaddr *)&ss, flags)) != 0)
    return rv;
  int namelen = sizeof(*local);
  if ((rv = uv_udp_getsockname(udp, (struct sockaddr *)local, &namelen)) != 0)
    return rv;
  *port = family == AF_INET ? ntohs(((struct sockaddr_in *)local)->sin_port)
                            : ntohs(((struct sockaddr_in6 *)local)->sin6_port);
  return uv_udp_recv_start(udp, on_udp_alloc, on_udp_read);
}

grpc_err http3_server_start(grpc_server *server, const grpc_tls_config *tls) {
  if (!server->is_tls) {
    LOG_ERROR(server, "http3 requires TLS (config.tls)\n");
    return GRPC_ERR_INVALID;
  }
  if (server->ipv4_addr == nullptr && server->ipv6_addr == nullptr) {
    LOG_ERROR(server, "http3 needs an IPv4 or IPv6 address (it cannot use a Unix socket)\n");
    return GRPC_ERR_INVALID;
  }
  grpc_h3_server *h3s = grpc_calloc(1, sizeof(*h3s));
  if (h3s == nullptr)
    return GRPC_ERR_NOMEM;
  h3s->server = server;
  h3s->accepting = true;
  server->h3 = h3s;
  h3_random(h3s->secret, sizeof(h3s->secret));

  h3s->tls_ctx = quic_tls_server_ctx_new(tls);
  if (h3s->tls_ctx == nullptr) {
    LOG_ERROR(server, "http3: cannot load the TLS configuration for QUIC\n");
    return GRPC_ERR_INVALID;
  }

  uint16_t port = server->config.http3.port ? server->config.http3.port : server->config.port;
  int rv;
  if (server->ipv4_addr != nullptr) {
    if ((rv = open_udp(h3s, &h3s->udp4, &h3s->local4, AF_INET, server->ipv4_addr, &port)) != 0) {
      h3s->udp4_active = h3s->udp4.loop != nullptr;
      LOG_ERROR(server, "http3: udp %s:%u: %s\n", server->ipv4_addr, port, uv_strerror(rv));
      return GRPC_ERR_IO;
    }
    h3s->udp4_active = true;
  }
  if (server->ipv6_addr != nullptr) {
    if ((rv = open_udp(h3s, &h3s->udp6, &h3s->local6, AF_INET6, server->ipv6_addr, &port)) != 0) {
      h3s->udp6_active = h3s->udp6.loop != nullptr;
      LOG_ERROR(server, "http3: udp [%s]:%u: %s\n", server->ipv6_addr, port, uv_strerror(rv));
      return GRPC_ERR_IO;
    }
    h3s->udp6_active = true;
  }
  h3s->port = port;
  if (server->config.http3.advertise_alt_svc)
    snprintf(server->alt_svc, sizeof(server->alt_svc), "h3=\":%u\"", port);
  LOG_INFO(server, "listening for HTTP/3 on udp port %u (%s)\n", port, quic_tls_crypto_name());
  return GRPC_OK;
}

void http3_server_stop_accepting(grpc_server *server) {
  if (server->h3 != nullptr)
    server->h3->accepting = false;
}

void http3_server_close_handles(grpc_server *server) {
  grpc_h3_server *h3s = server->h3;
  if (h3s == nullptr)
    return;
  h3s->accepting = false;
  if (h3s->udp4_active)
    uv_close((uv_handle_t *)&h3s->udp4, nullptr);
  if (h3s->udp6_active)
    uv_close((uv_handle_t *)&h3s->udp6, nullptr);
  h3s->udp4_active = h3s->udp6_active = false;
}

void http3_server_free(grpc_server *server) {
  grpc_h3_server *h3s = server->h3;
  if (h3s == nullptr)
    return;
  for (size_t i = 0; i < H3_CID_BUCKETS; i++) {
    while (h3s->cids[i] != nullptr) {
      cid_entry *e = h3s->cids[i];
      h3s->cids[i] = e->next;
      grpc_free(e);
    }
  }
  if (h3s->tls_ctx != nullptr)
    quic_tls_ctx_free(h3s->tls_ctx);
  for (size_t i = 0; i < h3s->nb_retired_tls; i++)
    quic_tls_ctx_free(h3s->retired_tls[i]);
  grpc_free(h3s->retired_tls);
  grpc_free(h3s);
  server->h3 = nullptr;
}

grpc_err http3_server_reload_tls(grpc_server *server, const grpc_tls_config *cfg) {
  grpc_h3_server *h3s = server->h3;
  if (h3s == nullptr)
    return GRPC_OK;
  grpc_quic_tls_ctx *ctx = quic_tls_server_ctx_new(cfg);
  if (ctx == nullptr)
    return GRPC_ERR_INVALID;
  /* keep the old context for the connections created from it (not every backend refcounts) */
  grpc_quic_tls_ctx **retired =
      grpc_realloc(h3s->retired_tls, (h3s->nb_retired_tls + 1) * sizeof(*retired));
  if (retired == nullptr) {
    quic_tls_ctx_free(ctx);
    return GRPC_ERR_NOMEM;
  }
  h3s->retired_tls = retired;
  h3s->retired_tls[h3s->nb_retired_tls++] = h3s->tls_ctx;
  h3s->tls_ctx = ctx;
  return GRPC_OK;
}

uint16_t http3_server_port(const grpc_server *server) {
  return server->h3 != nullptr ? server->h3->port : 0;
}
