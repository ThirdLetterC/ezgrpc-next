/* grpc_http3_server_conn.c - QUIC connections of the server: creation, timers, flushing, closing, incoming packets
 *
 * Part of the HTTP/3 server transport described in grpc_http3_server.c.
 */

#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include "core.h"
#include "grpc_stream_core.h"
#include "grpc_timeout.h"
#include "internal_helpers.h"
#include "internal_http3.h"
#include "internal_http3_server.h"
#include "internal_quic_tls.h"

/* ---- connection lifecycle ---- */

/* ngtcp2_crypto_conn_ref::get_conn: the TLS callbacks reach the ngtcp2_conn through this. */
static ngtcp2_conn *get_conn(ngtcp2_crypto_conn_ref *ref) {
  return ((grpc_quic_conn *)ref->user_data)->conn;
}

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

void h3_session_close(grpc_session *session) {
  grpc_quic_conn *qc = session->quic;
  session->is_closing = true;
  LOG_DEBUG(session->server, "http3: closing connection from %s\n", session->peer);
  if (qc->unvalidated) {
    qc->unvalidated = false;
    qc->h3s->nb_unvalidated--;
  }

  if (qc->conn != nullptr && qc->ccerr.type != NGTCP2_CCERR_TYPE_IDLE_CLOSE &&
      !ngtcp2_conn_in_draining_period(qc->conn) && !ngtcp2_conn_in_closing_period(qc->conn)) {
    ngtcp2_path_storage ps;
    ngtcp2_path_storage_zero(&ps);
    ngtcp2_pkt_info pi;
    ngtcp2_ssize n = ngtcp2_conn_write_connection_close(
        qc->conn, &ps.path, &pi, qc->h3s->sbuf, sizeof(qc->h3s->sbuf), &qc->ccerr, h3_now());
    if (n > 0)
      qc_send(qc, &ps.path, qc->h3s->sbuf, (size_t)n);
  }

  while (qc->nb_cids > 0)
    h3_cid_remove(qc, &qc->cids[qc->nb_cids - 1]);

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
  tls_ctx_pool_release(&qc->h3s->tls_pool, qc->tls_ctx_used);
  qc->tls_ctx_used = nullptr;

  uv_timer_stop(&qc->timer);
  uv_close((uv_handle_t *)&qc->timer, on_qc_timer_closed);
}

void h3_session_flush(grpc_session *session) {
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
    qc_send(qc, &ps.path, buf, (size_t)nwrite);
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
      .encrypt = ngtcp2_crypto_encrypt_cb,
      .decrypt = ngtcp2_crypto_decrypt_cb,
      .hp_mask = ngtcp2_crypto_hp_mask_cb,
      .update_key = ngtcp2_crypto_update_key_cb,
      .delete_crypto_aead_ctx = ngtcp2_crypto_delete_crypto_aead_ctx_cb,
      .delete_crypto_cipher_ctx = ngtcp2_crypto_delete_crypto_cipher_ctx_cb,
      .get_path_challenge_data = ngtcp2_crypto_get_path_challenge_data_cb,
      .version_negotiation = ngtcp2_crypto_version_negotiation_cb,
  };
  h3_server_set_quic_callbacks(&callbacks);
  ngtcp2_settings settings;
  ngtcp2_settings_default(&settings);
  settings.initial_ts = h3_now();
  settings.max_tx_udp_payload_size = H3_MAX_UDP_PAYLOAD;
  settings.handshake_timeout = H3_HANDSHAKE_TIMEOUT;
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
                             &params, &grpc_quic_mem, qc) != 0)
    goto fail;
  qc->conn_ref = (ngtcp2_crypto_conn_ref){.get_conn = get_conn, .user_data = qc};
  qc->tls_ctx_used = tls_ctx_pool_acquire(&h3s->tls_pool);
  qc->tls = quic_tls_session_new(qc->tls_ctx_used, &qc->conn_ref, true, nullptr);
  if (qc->tls == nullptr)
    goto fail;
  ngtcp2_conn_set_tls_native_handle(qc->conn, quic_tls_session_native(qc->tls));
  if (cfg->keepalive_time_ms > 0)
    ngtcp2_conn_set_keep_alive_timeout(qc->conn, (ngtcp2_duration)cfg->keepalive_time_ms *
                                                     NGTCP2_MILLISECONDS);
  /* retransmitted Initials still carry the client-chosen DCID */
  if (h3_cid_add(qc, &scid) != 0 || h3_cid_add(qc, &hd->dcid) != 0)
    goto fail;
  if (odcid == nullptr) { /* no Retry: the address is unproven until the handshake completes */
    qc->unvalidated = true;
    h3s->nb_unvalidated++;
  }
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
    h3_udp_send(h3s->server, udp, to, h3s->sbuf, (size_t)n);
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
    h3_udp_send(h3s->server, udp, to, h3s->sbuf, (size_t)n);
}

/* Handles a packet for no known connection: version negotiation, then the Retry exchange, then a
 * new connection. A Retry is required with http3.require_retry, and also whenever unvalidated
 * connections already hold half of the free connection slots (so a flood of spoofed Initials
 * cannot lock TCP and QUIC clients out). Returns the connection to feed the packet to, or nullptr
 * to drop it. */
static grpc_quic_conn *accept_packet(grpc_h3_server *h3s, uv_udp_t *udp, const uint8_t *data,
                                     size_t len, const struct sockaddr *from, socklen_t fromlen) {
  if (!h3s->accepting)
    return nullptr;
  ngtcp2_pkt_hd hd;
  if (ngtcp2_accept(&hd, data, len) != 0)
    return nullptr;
  ngtcp2_cid odcid;
  bool retried = hd.tokenlen > 0 &&
                 ngtcp2_crypto_verify_retry_token2(&odcid, hd.token, hd.tokenlen, h3s->secret,
                                                   sizeof(h3s->secret), hd.version,
                                                   (const ngtcp2_sockaddr *)from, fromlen, &hd.dcid,
                                                   H3_RETRY_TOKEN_TIMEOUT, h3_now()) == 0;
  if (!retried) {
    bool require_retry = h3s->server->config.http3.require_retry;
    if (require_retry && hd.tokenlen > 0)
      return nullptr; /* an invalid or expired token */
    hd.token = nullptr;
    hd.tokenlen = 0;
    if (require_retry || h3s->nb_unvalidated >= h3s->server->nb_free_sessions / 2) {
      send_retry(h3s, udp, from, fromlen, &hd);
      return nullptr;
    }
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

  grpc_quic_conn *qc = h3_cid_lookup(h3s, vc.dcid, vc.dcidlen);
  if (qc == nullptr) {
    qc = accept_packet(h3s, udp, data, len, addr, addrlen);
    if (qc == nullptr)
      return;
  }
  if (qc->session->is_closing || qc->conn == nullptr)
    return;

  /* the local side is the socket the datagram actually arrived on */
  bool on_v6 = udp == &h3s->udp6;
  ngtcp2_path path = {
      .local = {.addr = (ngtcp2_sockaddr *)(on_v6 ? &h3s->local6 : &h3s->local4),
                .addrlen = on_v6 ? sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in)},
      .remote = {.addr = grpc_unconst(addr), .addrlen = addrlen},
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

int h3_server_udp_recv_start(uv_udp_t *udp) {
  return uv_udp_recv_start(udp, on_udp_alloc, on_udp_read);
}
