/* grpc_http3_server.c - the HTTP/3 (QUIC) server transport
 *
 * Split over grpc_http3_server.c (helpers, transport ops, server lifecycle),
 * grpc_http3_server_callbacks.c (nghttp3 / ngtcp2 callbacks) and grpc_http3_server_conn.c
 * (connections and incoming packets). This comment describes the whole.
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
#include "internal_http3_server.h"
#include "internal_quic_tls.h"

/* The grpc_nv used by the call layer must be usable as an nghttp3_nv array as is. */
static_assert(sizeof(grpc_nv) == sizeof(nghttp3_nv) &&
                  offsetof(grpc_nv, name) == offsetof(nghttp3_nv, name) &&
                  offsetof(grpc_nv, value) == offsetof(nghttp3_nv, value) &&
                  offsetof(grpc_nv, namelen) == offsetof(nghttp3_nv, namelen) &&
                  offsetof(grpc_nv, valuelen) == offsetof(nghttp3_nv, valuelen) &&
                  offsetof(grpc_nv, flags) == offsetof(nghttp3_nv, flags),
              "grpc_nv must match nghttp3_nv");

/* ---- helpers ---- */

ngtcp2_tstamp h3_now(void) {
  return uv_hrtime();
}

void h3_random(uint8_t *dest, size_t len) {
  if (uv_random(nullptr, nullptr, dest, len, 0, nullptr) != 0) {
    /* uv_random only fails if the OS has no entropy source; never ship zeroes silently */
    for (size_t i = 0; i < len; i++)
      dest[i] = (uint8_t)(uv_hrtime() >> (i % 8));
  }
}

/* FNV-1a over a connection ID, for the table. */
static size_t cid_hash(const ngtcp2_cid *cid) {
  uint32_t h = 2166136261u;
  for (size_t i = 0; i < cid->datalen; i++)
    h = (h ^ cid->data[i]) * 16777619u;
  return h & (H3_CID_BUCKETS - 1);
}

grpc_quic_conn *h3_cid_lookup(grpc_h3_server *h3s, const uint8_t *data, size_t len) {
  ngtcp2_cid cid;
  ngtcp2_cid_init(&cid, data, len);
  for (cid_entry *e = h3s->cids[cid_hash(&cid)]; e != nullptr; e = e->next) {
    if (ngtcp2_cid_eq(&e->cid, &cid))
      return e->qc;
  }
  return nullptr;
}

int h3_cid_add(grpc_quic_conn *qc, const ngtcp2_cid *cid) {
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

void h3_cid_remove(grpc_quic_conn *qc, const ngtcp2_cid *cid) {
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

void h3_udp_send(grpc_server *server, uv_udp_t *udp, const struct sockaddr *to, const uint8_t *data,
                 size_t len) {
  uv_buf_t buf = uv_buf_init((char *)data, (unsigned int)len);
  int rv = uv_udp_try_send(udp, &buf, 1, to);
  if (rv >= 0)
    atomic_fetch_add(&server->stat_bytes_sent, (uint64_t)len);
  else
    LOG_DEBUG(server, "http3: udp send: %s\n", uv_strerror(rv));
}

void qc_send(grpc_quic_conn *qc, const ngtcp2_path *path, const uint8_t *data, size_t len) {
  grpc_h3_server *h3s = qc->h3s;
  if (path != nullptr && path->remote.addrlen > 0 &&
      (size_t)path->remote.addrlen <= sizeof(qc->remote)) {
    memcpy(&qc->remote, path->remote.addr, (size_t)path->remote.addrlen);
    qc->remotelen = (socklen_t)path->remote.addrlen;
  }
  uv_udp_t *udp = qc->udp;
  if (path != nullptr && path->local.addrlen > 0) {
    if (path->local.addr->sa_family == AF_INET6 && h3s->udp6_active)
      udp = &h3s->udp6;
    else if (path->local.addr->sa_family == AF_INET && h3s->udp4_active)
      udp = &h3s->udp4;
  }
  h3_udp_send(h3s->server, udp, (const struct sockaddr *)&qc->remote, data, len);
}

/* ---- transport ops ---- */

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

/* Maps an nghttp3 result to the transport-op convention: 0, or a grpc_err. */
static int h3_err(int rv) {
  return rv == 0 ? 0 : rv == NGHTTP3_ERR_NOMEM ? GRPC_ERR_NOMEM : GRPC_ERR_PROTOCOL;
}

/* submit_headers op. nghttp3 sends headers and body together, so headers that do not end the
 * stream are parked until start_data() or submit_trailers() supplies the body. */
static int h3_submit_headers(grpc_session *session, grpc_stream *stream, const grpc_nv *nva,
                             size_t nvlen, bool end_stream) {
  grpc_quic_conn *qc = session->quic;
  h3_stream *hs = stream->transport;
  if (qc->h3 == nullptr || hs == nullptr || hs->response_submitted)
    return GRPC_ERR_PROTOCOL;
  if (end_stream) {
    hs->response_submitted = true;
    hs->body_done = true;
    return h3_err(nghttp3_conn_submit_response(qc->h3, stream->stream_id, (const nghttp3_nv *)nva,
                                               nvlen, nullptr));
  }
  grpc_free(hs->headers);
  hs->headers = nv_copy(nva, nvlen);
  hs->nb_headers = nvlen;
  return hs->headers != nullptr ? 0 : GRPC_ERR_NOMEM;
}

/* Submits the parked response headers with the body data reader. */
static int h3_submit_parked_response(grpc_quic_conn *qc, grpc_stream *stream, h3_stream *hs) {
  if (hs->headers == nullptr)
    return GRPC_ERR_PROTOCOL;
  int rv = nghttp3_conn_submit_response(qc->h3, stream->stream_id, (const nghttp3_nv *)hs->headers,
                                        hs->nb_headers, &h3_data_reader);
  grpc_free(hs->headers);
  hs->headers = nullptr;
  hs->nb_headers = 0;
  if (rv != 0)
    return h3_err(rv);
  hs->response_submitted = true;
  return 0;
}

/* start_data op: submits the parked headers with the data reader, or resumes a reader that
 * returned WOULDBLOCK. */
static int h3_start_data(grpc_session *session, grpc_stream *stream) {
  grpc_quic_conn *qc = session->quic;
  h3_stream *hs = stream->transport;
  if (qc->h3 == nullptr || hs == nullptr || hs->body_done)
    return GRPC_ERR_PROTOCOL;
  if (!hs->response_submitted)
    return h3_submit_parked_response(qc, stream, hs);
  return h3_err(nghttp3_conn_resume_stream(qc->h3, stream->stream_id));
}

/* submit_trailers op: ends the body (the reader then returns EOF without END_STREAM) and queues
 * the trailers. */
static int h3_submit_trailers(grpc_session *session, grpc_stream *stream, const grpc_nv *nva,
                              size_t nvlen) {
  grpc_quic_conn *qc = session->quic;
  h3_stream *hs = stream->transport;
  if (qc->h3 == nullptr || hs == nullptr)
    return GRPC_ERR_PROTOCOL;
  if (!hs->response_submitted) {
    hs->body_done = true;
    int rv = h3_submit_parked_response(qc, stream, hs);
    if (rv != 0)
      return rv;
  } else if (!hs->body_done) {
    hs->body_done = true;
    (void)nghttp3_conn_resume_stream(qc->h3, stream->stream_id);
  }
  return h3_err(
      nghttp3_conn_submit_trailers(qc->h3, stream->stream_id, (const nghttp3_nv *)nva, nvlen));
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

/* goaway op: HTTP/3 GOAWAY naming the last accepted request stream. Sent once per connection:
 * nghttp3 asserts on a second shutdown (whose stream id would be larger, which the protocol
 * forbids). */
static void h3_goaway(grpc_session *session) {
  grpc_quic_conn *qc = session->quic;
  if (qc->h3 != nullptr && !qc->goaway_sent) {
    qc->goaway_sent = true;
    (void)nghttp3_conn_shutdown(qc->h3);
  }
}

/* shutdown_notice op: nghttp3's GOAWAY with the largest stream id; h3_goaway() follows. */
static void h3_shutdown_notice(grpc_session *session) {
  grpc_quic_conn *qc = session->quic;
  if (qc->h3 != nullptr && !qc->goaway_sent)
    (void)nghttp3_conn_submit_shutdown_notice(qc->h3);
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
    .shutdown_notice = h3_shutdown_notice,
    .ping = nullptr, /* QUIC keepalive: ngtcp2_conn_set_keep_alive_timeout() */
    .flush = h3_session_flush,
    .close = h3_session_close,
};

/* ---- server lifecycle ---- */

static void free_quic_ctx(void *ctx) {
  quic_tls_ctx_free(ctx);
}

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
  bool reuse = server->config.reuse_port;
#if defined(SO_REUSEPORT) && !defined(_WIN32)
  /* like the TCP listeners, with config.reuse_port: one server per thread can share the port.
   * The kernel spreads datagrams by address 4-tuple, so a connection stays on one loop */
  uv_os_fd_t fd;
  if (reuse && uv_fileno((uv_handle_t *)udp, &fd) == 0)
    (void)setsockopt((int)fd, SOL_SOCKET, SO_REUSEPORT, &(int){1}, sizeof(int));
#endif
  /* without reuse_port no other socket may hold the port (UV_UDP_REUSEADDR would allow that for
   * UDP), so a port already in use fails to bind */
  unsigned flags = (reuse ? UV_UDP_REUSEADDR : 0) | (family == AF_INET6 ? UV_UDP_IPV6ONLY : 0);
  if ((rv = uv_udp_bind(udp, (const struct sockaddr *)&ss, flags)) != 0)
    return rv;
  int namelen = sizeof(*local);
  if ((rv = uv_udp_getsockname(udp, (struct sockaddr *)local, &namelen)) != 0)
    return rv;
  *port = family == AF_INET ? ntohs(((struct sockaddr_in *)local)->sin_port)
                            : ntohs(((struct sockaddr_in6 *)local)->sin6_port);
  return h3_server_udp_recv_start(udp);
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

  grpc_quic_tls_ctx *tls_ctx = quic_tls_server_ctx_new(tls);
  if (tls_ctx == nullptr) {
    LOG_ERROR(server, "http3: cannot load the TLS configuration for QUIC\n");
    return GRPC_ERR_INVALID;
  }
  tls_ctx_pool_install(&h3s->tls_pool, tls_ctx, free_quic_ctx);

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
  tls_ctx_pool_free(&h3s->tls_pool);
  grpc_free(h3s);
  server->h3 = nullptr;
}

grpc_err http3_server_tls_prepare(grpc_server *server, const grpc_tls_config *cfg, void **out) {
  *out = nullptr;
  grpc_h3_server *h3s = server->h3;
  if (h3s == nullptr)
    return GRPC_OK;
  if (tls_ctx_pool_reserve(&h3s->tls_pool) != GRPC_OK)
    return GRPC_ERR_NOMEM;
  grpc_quic_tls_ctx *ctx = quic_tls_server_ctx_new(cfg);
  if (ctx == nullptr) {
    LOG_ERROR(server, "http3: cannot load the TLS configuration for QUIC\n");
    return GRPC_ERR_INVALID;
  }
  *out = ctx;
  return GRPC_OK;
}

void http3_server_tls_install(grpc_server *server, void *ctx) {
  if (server->h3 != nullptr && ctx != nullptr)
    tls_ctx_pool_install(&server->h3->tls_pool, ctx, free_quic_ctx);
}

void http3_server_tls_discard(grpc_server *server, void *ctx) {
  (void)server;
  if (ctx != nullptr)
    quic_tls_ctx_free(ctx);
}

uint16_t http3_server_port(const grpc_server *server) {
  return server->h3 != nullptr ? server->h3->port : 0;
}
