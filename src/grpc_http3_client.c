/* grpc_http3_client.c - the HTTP/3 (QUIC) client transport
 *
 * Split over grpc_http3_client.c (helpers, I/O, the hooks the client calls) and
 * grpc_http3_client_callbacks.c (nghttp3 / ngtcp2 callbacks). This comment describes the whole.
 *
 * A client created with GRPC_TRANSPORT_HTTP3 gives each subchannel a QUIC connection (ngtcp2) on a
 * connected UDP socket instead of a TCP/TLS/nghttp2 connection; HTTP/3 framing runs in nghttp3.
 * Everything else is shared with grpc_client.c: the subchannel's private libuv loop (run only
 * while the caller waits), the connect and RPC deadlines on s->timer, one active RPC per
 * subchannel, retries, round-robin, and response handling through client_resp_header() /
 * client_resp_data().
 *
 * Request bodies are copied from the stream's send_buf into chunks that stay alive until QUIC
 * acknowledges them, since nghttp3 sends from the caller's memory. A stream freed by the
 * application before its data is acknowledged hands its chunks to the connection (orphans), so
 * a retransmission never reads freed memory.
 */
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include <nghttp3/nghttp3.h>
#include <ngtcp2/ngtcp2.h>
#include <ngtcp2/ngtcp2_crypto.h>

#include "core.h"
#include "internal_client.h"
#include "internal_helpers.h"
#include "internal_http3.h"
#include "internal_http3_client.h"
#include "internal_quic_tls.h"

/* ---- helpers ---- */

static ngtcp2_tstamp h3c_now(void) {
  return uv_hrtime();
}

void h3c_random(uint8_t *dest, size_t len) {
  if (uv_random(nullptr, nullptr, dest, len, 0, nullptr) != 0) {
    for (size_t i = 0; i < len; i++)
      dest[i] = (uint8_t)(uv_hrtime() >> (i % 8));
  }
}

grpc_client_stream *h3c_active(h3c_conn *hc, int64_t stream_id) {
  grpc_client_stream *st = hc->s->active_stream;
  return st != nullptr && st->stream_id == stream_id ? st : nullptr;
}

/* Marks the connection broken and fails the active RPC with `err`. */
static void h3c_fail(h3c_conn *hc, grpc_err err) {
  hc->s->closed = true;
  grpc_client_stream *st = hc->s->active_stream;
  if (st != nullptr && !st->stream_done) {
    st->err = err;
    st->stream_done = true;
  }
}

void h3c_consume(h3c_conn *hc, int64_t stream_id, size_t n) {
  ngtcp2_conn_extend_max_stream_offset(hc->conn, stream_id, n);
  ngtcp2_conn_extend_max_offset(hc->conn, n);
}

/* Frees a chunk list. */
static void free_chunks(h3c_chunk *c) {
  while (c != nullptr) {
    h3c_chunk *next = c->next;
    grpc_free(c);
    c = next;
  }
}

void h3c_orphan_free(h3c_conn *hc, h3c_stream *hs) {
  if (hs->orphan_prev != nullptr)
    hs->orphan_prev->orphan_next = hs->orphan_next;
  else
    hc->orphans = hs->orphan_next;
  if (hs->orphan_next != nullptr)
    hs->orphan_next->orphan_prev = hs->orphan_prev;
  free_chunks(hs->head);
  grpc_free(hs);
}

/* ---- I/O ---- */

/* ngtcp2_crypto_conn_ref::get_conn: the TLS callbacks reach the ngtcp2_conn through this. */
static ngtcp2_conn *get_conn(ngtcp2_crypto_conn_ref *ref) {
  return ((h3c_conn *)ref->user_data)->conn;
}

static void on_h3c_timer(uv_timer_t *timer);

static void h3c_arm_timer(h3c_conn *hc) {
  ngtcp2_tstamp expiry = ngtcp2_conn_get_expiry(hc->conn);
  if (expiry == UINT64_MAX) {
    uv_timer_stop(&hc->timer);
    return;
  }
  ngtcp2_tstamp now = h3c_now();
  uint64_t ms = expiry > now ? (expiry - now + NGTCP2_MILLISECONDS - 1) / NGTCP2_MILLISECONDS : 0;
  /* the loop clock stops between RPCs: refresh it so the timer fires `ms` from now */
  uv_update_time(hc->timer.loop);
  uv_timer_start(&hc->timer, on_h3c_timer, ms, 0);
}

/* Sends one datagram on the connected socket; a full buffer drops it (QUIC retransmits). */
/* A datagram waiting in libuv's send queue, with its own copy of the bytes. */
typedef struct h3c_send_req {
  uv_udp_send_t req;
  h3c_conn *hc;
  size_t len;
  uint8_t data[];
} h3c_send_req;

/* uv_udp_send() completion, or cancellation when the socket closes (before the connection is
 * freed in on_h3c_handle_closed()). */
static void on_h3c_send_done(uv_udp_send_t *req, int status) {
  (void)status;
  h3c_send_req *sr = (h3c_send_req *)req;
  sr->hc->send_queued_bytes -= sr->len;
  grpc_free(sr);
}

/* Sends a datagram on the connected socket. When the socket buffer is full (or libuv still holds
 * queued datagrams, which keeps them in order) a copy is queued instead of left to loss recovery,
 * up to H3C_SEND_QUEUE_MAX bytes. */
static void h3c_send(h3c_conn *hc, const uint8_t *data, size_t len) {
  uv_buf_t buf = uv_buf_init(grpc_unconst(data), (unsigned int)len);
  int rv = uv_udp_try_send(&hc->udp, &buf, 1, nullptr);
  if (rv >= 0 || (rv != UV_EAGAIN && rv != UV_ENOBUFS))
    return;
  h3c_send_req *sr =
      len <= H3C_SEND_QUEUE_MAX - hc->send_queued_bytes ? grpc_malloc(sizeof(*sr) + len) : nullptr;
  if (sr == nullptr) {
    hc->stat_send_dropped++;
    return;
  }
  sr->hc = hc;
  sr->len = len;
  memcpy(sr->data, data, len);
  buf = uv_buf_init((char *)sr->data, (unsigned int)len);
  if (uv_udp_send(&sr->req, &hc->udp, &buf, 1, nullptr, on_h3c_send_done) != 0) {
    grpc_free(sr);
    hc->stat_send_dropped++;
    return;
  }
  hc->send_queued_bytes += len;
  hc->stat_send_queued++;
}

/* ngtcp2_write_pkt for ngtcp2_conn_write_aggregate_pkt(): writes one packet with whatever
 * nghttp3 has to send (or only QUIC frames). Returns its length, 0 when there is nothing to send,
 * or a negative ngtcp2 error, which fails the connection. */
static ngtcp2_ssize h3c_write_pkt(ngtcp2_conn *conn, ngtcp2_path *path, ngtcp2_pkt_info *pi,
                                  uint8_t *dest, size_t destlen, ngtcp2_tstamp ts,
                                  void *user_data) {
  h3c_conn *hc = user_data;
  for (;;) {
    int64_t stream_id = -1;
    int fin = 0;
    nghttp3_vec vec[16];
    nghttp3_ssize sveccnt = 0;
    if (hc->h3 != nullptr && ngtcp2_conn_get_max_data_left(conn)) {
      sveccnt = nghttp3_conn_writev_stream(hc->h3, &stream_id, &fin, vec, 16);
      if (sveccnt < 0)
        return NGTCP2_ERR_CALLBACK_FAILURE;
    }
    ngtcp2_ssize ndatalen = -1;
    uint32_t flags = NGTCP2_WRITE_STREAM_FLAG_MORE | (fin ? NGTCP2_WRITE_STREAM_FLAG_FIN : 0);
    ngtcp2_ssize nwrite =
        ngtcp2_conn_writev_stream(conn, path, pi, dest, destlen, &ndatalen, flags, stream_id,
                                  (const ngtcp2_vec *)vec, (size_t)sveccnt, ts);
    if (nwrite < 0) {
      switch (nwrite) {
      case NGTCP2_ERR_STREAM_DATA_BLOCKED:
        nghttp3_conn_block_stream(hc->h3, stream_id);
        continue;
      case NGTCP2_ERR_STREAM_SHUT_WR:
        nghttp3_conn_shutdown_stream_write(hc->h3, stream_id);
        continue;
      case NGTCP2_ERR_WRITE_MORE:
        if (nghttp3_conn_add_write_offset(hc->h3, stream_id, (size_t)ndatalen) != 0)
          return NGTCP2_ERR_CALLBACK_FAILURE;
        continue;
      default:
        return nwrite;
      }
    }
    if (ndatalen >= 0 && stream_id >= 0 &&
        nghttp3_conn_add_write_offset(hc->h3, stream_id, (size_t)ndatalen) != 0)
      return NGTCP2_ERR_CALLBACK_FAILURE;
    return nwrite;
  }
}

/* Sends `len` bytes of packets of `gsolen` bytes each (the last may be shorter): with one GSO call
 * where possible (and libuv holds no queued datagrams, which must go first), else packet by
 * packet. */
static void h3c_send_batch(h3c_conn *hc, const uint8_t *data, size_t len, size_t gsolen) {
  if (len > gsolen && !hc->gso_off && hc->send_queued_bytes == 0) {
    int rv = grpc_h3_send_gso(&hc->udp, nullptr, data, len, gsolen);
    if (rv == 0) {
      hc->stat_gso_sends++;
      return;
    }
    if (rv == UV_ENOTSUP)
      hc->gso_off = true;
    else if (rv != UV_EAGAIN && rv != UV_ENOBUFS)
      return; /* lost like any datagram; loss recovery resends */
  }
  for (size_t off = 0; off < len; off += gsolen)
    h3c_send(hc, data + off, len - off < gsolen ? len - off : gsolen);
}

void h3c_flush(client_subchannel *s) {
  h3c_conn *hc = s->h3c;
  if (hc == nullptr || hc->conn == nullptr || s->closed)
    return;
  ngtcp2_tstamp ts = h3c_now();
  size_t cap = hc->gso_off ? H3C_MAX_UDP_PAYLOAD : sizeof(hc->gsobuf);
  for (;;) {
    ngtcp2_path_storage ps;
    ngtcp2_path_storage_zero(&ps);
    ngtcp2_pkt_info pi;
    size_t gsolen = 0;
    ngtcp2_ssize n = ngtcp2_conn_write_aggregate_pkt(hc->conn, &ps.path, &pi, hc->gsobuf, cap,
                                                     &gsolen, h3c_write_pkt, ts);
    if (n < 0) {
      h3c_fail(hc, n == NGTCP2_ERR_CALLBACK_FAILURE ? GRPC_ERR_PROTOCOL : GRPC_ERR_IO);
      return;
    }
    if (n == 0)
      break;
    h3c_send_batch(hc, hc->gsobuf, (size_t)n, gsolen);
  }
  ngtcp2_conn_update_pkt_tx_time(hc->conn, ts);
  h3c_arm_timer(hc);
}

/* Connection timer: ngtcp2's expiry handling (retransmission, ACKs, idle timeout), then write. */
static void on_h3c_timer(uv_timer_t *timer) {
  h3c_conn *hc = timer->data;
  if (hc->conn == nullptr)
    return;
  if (ngtcp2_conn_handle_expiry(hc->conn, h3c_now()) != 0) {
    h3c_fail(hc, GRPC_ERR_CLOSED);
    return;
  }
  h3c_flush(hc->s);
}

static void on_h3c_alloc(uv_handle_t *handle, size_t suggested, uv_buf_t *buf) {
  (void)suggested;
  h3c_conn *hc = handle->data;
  *buf = uv_buf_init((char *)hc->rbuf, sizeof(hc->rbuf));
}

/* UDP read callback: feeds the datagram to ngtcp2 and writes whatever it answers. */
static void on_h3c_read(uv_udp_t *udp, ssize_t nread, const uv_buf_t *buf,
                        const struct sockaddr *addr, unsigned flags) {
  (void)flags;
  h3c_conn *hc = udp->data;
  if (nread <= 0 || hc->conn == nullptr)
    return;
  ngtcp2_path path = {
      .local = {.addr = (ngtcp2_sockaddr *)&hc->local, .addrlen = hc->locallen},
      .remote = {.addr =
                     grpc_unconst(addr != nullptr ? (const void *)addr : (const void *)&hc->remote),
                 .addrlen = hc->remotelen},
  };
  ngtcp2_pkt_info pi = {0};
  ngtcp2_tstamp now = h3c_now();
  int rv =
      ngtcp2_conn_read_pkt(hc->conn, &path, &pi, (const uint8_t *)buf->base, (size_t)nread, now);
  if (rv != 0) {
    h3c_fail(hc, rv == NGTCP2_ERR_CRYPTO ? GRPC_ERR_IO : GRPC_ERR_CLOSED);
    return;
  }
  hc->last_rx = now;
  h3c_flush(hc->s);
}

/* ---- transport hooks used by grpc_client.c ---- */

void *h3c_tls_ctx_new(const grpc_client_config *config) {
  grpc_quic_tls_client_config cfg = {
      .ca_file = config->tls.ca_file,
      .ca_pem = config->tls.ca_pem,
      .ca_pem_len = config->tls.ca_pem_len,
      .cert_file = config->tls.cert_file,
      .key_file = config->tls.key_file,
      .insecure_skip_verify = config->tls.insecure_skip_verify,
  };
  return quic_tls_client_ctx_new(&cfg);
}

void h3c_tls_ctx_free(void *ctx) {
  quic_tls_ctx_free(ctx);
}

/* Resolves s->host:s->port for UDP into *out (IP literals avoid getaddrinfo). */
static bool resolve(client_subchannel *s, struct sockaddr_storage *out, socklen_t *outlen) {
  if (uv_ip4_addr(s->host, s->port, (struct sockaddr_in *)out) == 0) {
    *outlen = sizeof(struct sockaddr_in);
    return true;
  }
  if (uv_ip6_addr(s->host, s->port, (struct sockaddr_in6 *)out) == 0) {
    *outlen = sizeof(struct sockaddr_in6);
    return true;
  }
  struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_DGRAM};
  struct addrinfo *res = nullptr;
  char port_str[16];
  snprintf(port_str, sizeof(port_str), "%u", s->port);
  if (getaddrinfo(s->host, port_str, &hints, &res) != 0 || res == nullptr)
    return false;
  memcpy(out, res->ai_addr, res->ai_addrlen);
  *outlen = (socklen_t)res->ai_addrlen;
  freeaddrinfo(res);
  return true;
}

grpc_err h3c_connect(client_subchannel *s) {
  grpc_client *c = s->parent;
  h3c_conn *hc = grpc_calloc(1, sizeof(*hc));
  if (hc == nullptr)
    return GRPC_ERR_NOMEM;
  hc->s = s;
  s->h3c = hc;
  ngtcp2_ccerr_default(&hc->ccerr);

  if (!resolve(s, &hc->remote, &hc->remotelen))
    return GRPC_ERR_IO;
  int family = hc->remote.ss_family;
  if (uv_udp_init_ex(&s->loop, &hc->udp, (unsigned)family) != 0)
    return GRPC_ERR_IO;
  hc->udp.data = hc;
  hc->open_handles++;
  if (uv_timer_init(&s->loop, &hc->timer) != 0)
    return GRPC_ERR_IO;
  hc->timer.data = hc;
  hc->open_handles++;
  if (uv_udp_connect(&hc->udp, (const struct sockaddr *)&hc->remote) != 0)
    return GRPC_ERR_IO;
  int namelen = sizeof(hc->local);
  if (uv_udp_getsockname(&hc->udp, (struct sockaddr *)&hc->local, &namelen) != 0)
    return GRPC_ERR_IO;
  hc->locallen = (socklen_t)namelen;
  if (uv_udp_recv_start(&hc->udp, on_h3c_alloc, on_h3c_read) != 0)
    return GRPC_ERR_IO;

  uint8_t buf[H3C_CID_LEN];
  ngtcp2_cid dcid, scid;
  h3c_random(buf, sizeof(buf));
  ngtcp2_cid_init(&dcid, buf, sizeof(buf));
  h3c_random(buf, sizeof(buf));
  ngtcp2_cid_init(&scid, buf, sizeof(buf));
  ngtcp2_path path = {
      .local = {.addr = (ngtcp2_sockaddr *)&hc->local, .addrlen = hc->locallen},
      .remote = {.addr = (ngtcp2_sockaddr *)&hc->remote, .addrlen = hc->remotelen},
  };
  ngtcp2_callbacks callbacks = {
      .client_initial = ngtcp2_crypto_client_initial_cb,
      .recv_crypto_data = ngtcp2_crypto_recv_crypto_data_cb,
      .encrypt = ngtcp2_crypto_encrypt_cb,
      .decrypt = ngtcp2_crypto_decrypt_cb,
      .hp_mask = ngtcp2_crypto_hp_mask_cb,
      .recv_retry = ngtcp2_crypto_recv_retry_cb,
      .update_key = ngtcp2_crypto_update_key_cb,
      .delete_crypto_aead_ctx = ngtcp2_crypto_delete_crypto_aead_ctx_cb,
      .delete_crypto_cipher_ctx = ngtcp2_crypto_delete_crypto_cipher_ctx_cb,
      .get_path_challenge_data = ngtcp2_crypto_get_path_challenge_data_cb,
      .version_negotiation = ngtcp2_crypto_version_negotiation_cb,
  };
  h3c_set_quic_callbacks(&callbacks);
  ngtcp2_settings settings;
  ngtcp2_settings_default(&settings);
  settings.initial_ts = h3c_now();
  settings.max_tx_udp_payload_size = H3C_MAX_UDP_PAYLOAD;
  ngtcp2_transport_params params;
  ngtcp2_transport_params_default(&params);
  uint64_t window = (uint64_t)c->config.max_message_size + 5;
  params.initial_max_stream_data_bidi_local = window;
  params.initial_max_stream_data_bidi_remote = window;
  params.initial_max_stream_data_uni = window;
  params.initial_max_data = window * 4 > 16 * 1024 * 1024 ? window * 4 : 16 * 1024 * 1024;
  params.initial_max_streams_bidi = 0;
  params.initial_max_streams_uni = 3;
  params.max_idle_timeout = 30 * NGTCP2_SECONDS;
  if (ngtcp2_conn_client_new(&hc->conn, &dcid, &scid, &path, NGTCP2_PROTO_VER_V1, &callbacks,
                             &settings, &params, &grpc_quic_mem, hc) != 0)
    return GRPC_ERR_IO;

  const char *sni = c->config.tls.server_name != nullptr ? c->config.tls.server_name : s->host;
  hc->conn_ref = (ngtcp2_crypto_conn_ref){.get_conn = get_conn, .user_data = hc};
  hc->tls = quic_tls_session_new(c->quic_tls_ctx, &hc->conn_ref, false, sni);
  if (hc->tls == nullptr)
    return GRPC_ERR_IO;
  ngtcp2_conn_set_tls_native_handle(hc->conn, quic_tls_session_native(hc->tls));

  h3c_flush(s);
  while (!hc->handshake_done && !s->closed && !s->connect_deadline_hit)
    uv_run(&s->loop, UV_RUN_ONCE);
  if (!hc->handshake_done || s->closed)
    return GRPC_ERR_IO;
  s->connected = true;
  return GRPC_OK;
}

/* Close callback of the UDP socket and timer; frees the connection after the last one. */
static void on_h3c_handle_closed(uv_handle_t *handle) {
  h3c_conn *hc = handle->data;
  if (--hc->open_handles == 0)
    grpc_free(hc);
}

void h3c_disconnect(client_subchannel *s) {
  h3c_conn *hc = s->h3c;
  if (hc == nullptr)
    return;
  s->h3c = nullptr;
  if (hc->conn != nullptr && hc->handshake_done && !ngtcp2_conn_in_closing_period(hc->conn) &&
      !ngtcp2_conn_in_draining_period(hc->conn)) {
    ngtcp2_path_storage ps;
    ngtcp2_path_storage_zero(&ps);
    ngtcp2_pkt_info pi;
    ngtcp2_ssize n = ngtcp2_conn_write_connection_close(hc->conn, &ps.path, &pi, hc->sbuf,
                                                        sizeof(hc->sbuf), &hc->ccerr, h3c_now());
    if (n > 0)
      h3c_send(hc, hc->sbuf, (size_t)n);
  }
  /* a stream still open loses its transport now */
  if (s->active_stream != nullptr && s->active_stream->h3 != nullptr) {
    h3c_stream *hs = s->active_stream->h3;
    free_chunks(hs->head);
    hs->head = hs->tail = nullptr;
    hs->closed = true;
  }
  if (hc->h3 != nullptr)
    nghttp3_conn_del(hc->h3);
  hc->h3 = nullptr;
  if (hc->conn != nullptr)
    ngtcp2_conn_del(hc->conn);
  hc->conn = nullptr;
  quic_tls_session_free(hc->tls);
  hc->tls = nullptr;
  while (hc->orphans != nullptr)
    h3c_orphan_free(hc, hc->orphans);
  int handles = hc->open_handles;
  if (handles == 0) {
    grpc_free(hc);
    return;
  }
  /* the subchannel then runs its loop until these close callbacks have fired */
  if (hc->open_handles >= 1 && hc->udp.loop != nullptr)
    uv_close((uv_handle_t *)&hc->udp, on_h3c_handle_closed);
  if (hc->open_handles >= 2)
    uv_close((uv_handle_t *)&hc->timer, on_h3c_handle_closed);
}

bool h3c_usable(client_subchannel *s) {
  h3c_conn *hc = s->h3c;
  if (hc == nullptr || hc->conn == nullptr || hc->h3 == nullptr)
    return false;
  /* timers only run while an RPC drives the loop: catch up on an expiry that passed meanwhile
   * (an idle timeout makes ngtcp2_conn_handle_expiry() fail) */
  ngtcp2_tstamp now = h3c_now();
  if (ngtcp2_conn_get_expiry(hc->conn) <= now && ngtcp2_conn_handle_expiry(hc->conn, now) != 0)
    return false;
  if (hc->goaway_received || ngtcp2_conn_in_closing_period(hc->conn) ||
      ngtcp2_conn_in_draining_period(hc->conn))
    return false;
  /* ngtcp2's own idle timer allows at least three probe timeouts, which can be well past the
   * idle timeout the server announced and enforces: a connection silent for that long is gone
   * on the server's side, so do not send the next request into it */
  const ngtcp2_transport_params *peer = ngtcp2_conn_get_remote_transport_params(hc->conn);
  if (peer != nullptr && peer->max_idle_timeout > 0 && hc->last_rx > 0 &&
      now - hc->last_rx >= peer->max_idle_timeout)
    return false;
  return true;
}

int h3c_submit_request(client_subchannel *s, grpc_client_stream *st, const grpc_nv *nva,
                       size_t nvlen) {
  h3c_conn *hc = s->h3c;
  if (hc == nullptr || hc->h3 == nullptr)
    return -1;
  h3c_stream *hs = grpc_calloc(1, sizeof(*hs));
  if (hs == nullptr)
    return -1;
  int64_t stream_id;
  if (ngtcp2_conn_open_bidi_stream(hc->conn, &stream_id, nullptr) != 0) {
    grpc_free(hs);
    return -1;
  }
  hs->st = st;
  st->h3 = hs;
  st->stream_id = stream_id;
  if (nghttp3_conn_submit_request(hc->h3, stream_id, (const nghttp3_nv *)nva, nvlen,
                                  &h3c_data_reader, hs) != 0) {
    (void)ngtcp2_conn_shutdown_stream(hc->conn, 0, stream_id, NGHTTP3_H3_INTERNAL_ERROR);
    /* nghttp3 may have created the stream before failing: then it holds hs as stream data and
     * its close frees hs (h3c_stream_free() parks it as an orphan until then). If nghttp3 never
     * knew the stream, no close will come: free hs now. */
    if (nghttp3_conn_set_stream_user_data(hc->h3, stream_id, hs) != 0) {
      hs->st = nullptr;
      st->h3 = nullptr;
      grpc_free(hs);
    }
    return -1;
  }
  return 0;
}

void h3c_resume(client_subchannel *s, grpc_client_stream *st) {
  if (s->h3c != nullptr && s->h3c->h3 != nullptr)
    (void)nghttp3_conn_resume_stream(s->h3c->h3, st->stream_id);
}

void h3c_reset(client_subchannel *s, grpc_client_stream *st) {
  if (s->h3c != nullptr && s->h3c->conn != nullptr)
    (void)ngtcp2_conn_shutdown_stream(s->h3c->conn, 0, st->stream_id, NGHTTP3_H3_REQUEST_CANCELLED);
}

void h3c_send_counters(const client_subchannel *s, uint64_t *queued, uint64_t *dropped,
                       uint64_t *gso_sends) {
  *queued = s->h3c != nullptr ? s->h3c->stat_send_queued : 0;
  *dropped = s->h3c != nullptr ? s->h3c->stat_send_dropped : 0;
  *gso_sends = s->h3c != nullptr ? s->h3c->stat_gso_sends : 0;
}

size_t h3c_orphan_count(const client_subchannel *s) {
  size_t n = 0;
  for (const h3c_stream *hs = s->h3c != nullptr ? s->h3c->orphans : nullptr; hs != nullptr;
       hs = hs->orphan_next)
    n++;
  return n;
}

void h3c_stream_free(grpc_client_stream *st) {
  h3c_stream *hs = st->h3;
  if (hs == nullptr)
    return;
  st->h3 = nullptr;
  hs->st = nullptr;
  h3c_conn *hc = st->sub != nullptr ? st->sub->h3c : nullptr;
  if (hc != nullptr && hc->h3 != nullptr && !hs->closed) {
    /* the RPC ends before the stream did (the server answered early, or a local error): tell the
     * server to stop. nghttp3 may still retransmit the request bytes, so keep the state until it
     * closes the stream. */
    if (hc->conn != nullptr) {
      (void)ngtcp2_conn_shutdown_stream(hc->conn, 0, st->stream_id, NGHTTP3_H3_REQUEST_CANCELLED);
      h3c_flush(st->sub);
    }
    hs->orphan_prev = nullptr;
    hs->orphan_next = hc->orphans;
    if (hc->orphans != nullptr)
      hc->orphans->orphan_prev = hs;
    hc->orphans = hs;
    return;
  }
  free_chunks(hs->head);
  grpc_free(hs);
}
