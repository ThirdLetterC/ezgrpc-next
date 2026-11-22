/* HTTP/3 server regression tests driven by a raw ngtcp2 client, for behaviour the library's own
 * client cannot produce: streams reset before any data, a client address that changes mid
 * connection (NAT rebinding), Initials that are never answered, and a request opened after the
 * server's GOAWAY followed by grpc_server_drain(). POSIX only; run from the directory holding
 * test_cert.pem / test_key.pem. */
#include <arpa/inet.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <nghttp3/nghttp3.h>
#include <ngtcp2/ngtcp2.h>
#include <ngtcp2/ngtcp2_crypto.h>

#include "grpc.h"
#include "internal_quic_tls.h"
#include "test.h"

/* ---- fixture: one server thread, one raw client ---------------------------------------- */

typedef struct raw_client {
  ngtcp2_conn *conn;
  ngtcp2_crypto_conn_ref ref;
  grpc_quic_tls_ctx *tls_ctx;
  grpc_quic_tls_session *tls;
  int fd;      /* the socket ngtcp2 believes it uses */
  int send_fd; /* the socket actually used for sending (differs after a rebinding) */
  struct sockaddr_in local, remote;
  bool handshake_done;
  /* HTTP/3 on top (h3_request()): nullptr for the QUIC-only tests */
  nghttp3_conn *h3;
  int http_status;
  bool stream_closed;
} raw_client;

static struct {
  grpc_server *server;
  pthread_t thread;
  bool running;
  raw_client client;
  int extra_fds[2];
} fx;

static uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void sleep_ms(unsigned ms) {
  struct timespec ts = {.tv_sec = ms / 1000, .tv_nsec = (long)(ms % 1000) * 1000000L};
  nanosleep(&ts, nullptr);
}

static void *run_server(void *arg) {
  (void)grpc_server_run(arg);
  return nullptr;
}

static void ok_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                       [[maybe_unused]] void *ud) {
  (void)grpc_call_reply_data(call, "ok", 2);
}

/* Starts a TLS + HTTP/3 server on 127.0.0.1 with `cfg`'s limits. */
static grpc_server *start_server(grpc_config cfg) {
  cfg.port = 0;
  cfg.ipv4_addr = "127.0.0.1";
  cfg.tls = (grpc_tls_config){.cert_file = "test_cert.pem", .key_file = "test_key.pem"};
  cfg.http3.enable = true;
  cfg.log_level = GRPC_LOG_OFF;
  fx.server = grpc_server_new(&cfg);
  TEST_ASSERT_NOT_NULL(fx.server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(fx.server, "/raw.Svc/Ok", ok_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&fx.thread, nullptr, run_server, fx.server));
  fx.running = true;
  return fx.server;
}

static void stop_server(void) {
  if (fx.running) {
    grpc_server_stop(fx.server);
    pthread_join(fx.thread, nullptr);
    fx.running = false;
  }
}

static void client_free(raw_client *k) {
  if (k->h3 != nullptr)
    nghttp3_conn_del(k->h3);
  if (k->conn != nullptr)
    ngtcp2_conn_del(k->conn);
  quic_tls_session_free(k->tls);
  quic_tls_ctx_free(k->tls_ctx);
  if (k->fd >= 0)
    close(k->fd);
  if (k->send_fd >= 0 && k->send_fd != k->fd)
    close(k->send_fd);
  *k = (raw_client){.fd = -1, .send_fd = -1};
}

/* ---- a minimal HTTP/3 client on top, for requests the library's client never sends ----- */

static void client_flush(raw_client *k);
static void client_run(raw_client *k, unsigned ms, int fd2, int *on_fd, int *on_fd2);

static int h3_on_recv_header(nghttp3_conn *conn, int64_t stream_id, int32_t token,
                             nghttp3_rcbuf *name, nghttp3_rcbuf *value, uint8_t flags,
                             void *user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_id;
  (void)token;
  (void)flags;
  (void)stream_user_data;
  raw_client *k = user_data;
  nghttp3_vec n = nghttp3_rcbuf_get_buf(name), v = nghttp3_rcbuf_get_buf(value);
  if (n.len == 7 && memcmp(n.base, ":status", 7) == 0 && v.len == 3)
    k->http_status = (v.base[0] - '0') * 100 + (v.base[1] - '0') * 10 + (v.base[2] - '0');
  return 0;
}

static int h3_on_stream_close(nghttp3_conn *conn, int64_t stream_id, uint64_t app_error_code,
                              void *user_data, void *stream_user_data) {
  (void)conn;
  (void)app_error_code;
  (void)stream_user_data;
  raw_client *k = user_data;
  if (ngtcp2_is_bidi_stream(stream_id))
    k->stream_closed = true;
  return 0;
}

static int h3_on_deferred_consume(nghttp3_conn *conn, int64_t stream_id, size_t consumed,
                                  void *user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  raw_client *k = user_data;
  ngtcp2_conn_extend_max_stream_offset(k->conn, stream_id, consumed);
  ngtcp2_conn_extend_max_offset(k->conn, consumed);
  return 0;
}

static nghttp3_ssize h3_body(nghttp3_conn *conn, int64_t stream_id, nghttp3_vec *vec, size_t veccnt,
                             uint32_t *pflags, void *user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_id;
  (void)veccnt;
  (void)user_data;
  (void)stream_user_data;
  static const uint8_t frame[] = {0, 0, 0, 0, 2, 'h', 'i'};
  vec[0] = (nghttp3_vec){(uint8_t *)frame, sizeof(frame)};
  *pflags = NGHTTP3_DATA_FLAG_EOF;
  return 1;
}

/* Sets up HTTP/3 on the handshaken connection and sends one POST to /raw.Svc/Ok with
 * `content_type` and a one-message body. Returns the response's :status (0 if none). */
static int h3_request(raw_client *k, const char *content_type) {
  nghttp3_callbacks cb = {
      .recv_header = h3_on_recv_header,
      .stream_close = h3_on_stream_close,
      .deferred_consume = h3_on_deferred_consume,
  };
  nghttp3_settings settings;
  nghttp3_settings_default(&settings);
  TEST_ASSERT_EQUAL_INT(0, nghttp3_conn_client_new(&k->h3, &cb, &settings, nullptr, k));
  int64_t ctrl, qenc, qdec, sid;
  TEST_ASSERT_EQUAL_INT(0, ngtcp2_conn_open_uni_stream(k->conn, &ctrl, nullptr));
  TEST_ASSERT_EQUAL_INT(0, ngtcp2_conn_open_uni_stream(k->conn, &qenc, nullptr));
  TEST_ASSERT_EQUAL_INT(0, ngtcp2_conn_open_uni_stream(k->conn, &qdec, nullptr));
  TEST_ASSERT_EQUAL_INT(0, nghttp3_conn_bind_control_stream(k->h3, ctrl));
  TEST_ASSERT_EQUAL_INT(0, nghttp3_conn_bind_qpack_streams(k->h3, qenc, qdec));
  TEST_ASSERT_EQUAL_INT(0, ngtcp2_conn_open_bidi_stream(k->conn, &sid, nullptr));
#define H3NV(n, v)                                                             \
  {                                                                            \
    (uint8_t *)(n), (uint8_t *)(v), strlen(n), strlen(v), NGHTTP3_NV_FLAG_NONE \
  }
  const nghttp3_nv nva[] = {H3NV(":method", "POST"),
                            H3NV(":scheme", "https"),
                            H3NV(":authority", "localhost"),
                            H3NV(":path", "/raw.Svc/Ok"),
                            H3NV("content-type", content_type),
                            H3NV("te", "trailers")};
#undef H3NV
  nghttp3_data_reader body = {.read_data = h3_body};
  TEST_ASSERT_EQUAL_INT(0, nghttp3_conn_submit_request(
                               k->h3, sid, nva, sizeof(nva) / sizeof(nva[0]), &body, nullptr));
  k->http_status = 0;
  k->stream_closed = false;
  client_flush(k);
  uint64_t end = now_ns() + 3000000000ull;
  while (!k->stream_closed && now_ns() < end)
    client_run(k, 10, -1, nullptr, nullptr);
  return k->http_status;
}

void setUp(void) {
  memset(&fx, 0, sizeof(fx));
  fx.client = (raw_client){.fd = -1, .send_fd = -1};
  fx.extra_fds[0] = fx.extra_fds[1] = -1;
}

void tearDown(void) {
  client_free(&fx.client);
  for (int i = 0; i < 2; i++) {
    if (fx.extra_fds[i] >= 0)
      close(fx.extra_fds[i]);
  }
  stop_server();
  grpc_server_free(fx.server);
  fx.server = nullptr;
}

/* ---- ngtcp2 client callbacks ---------------------------------------------------------- */

static ngtcp2_conn *get_conn(ngtcp2_crypto_conn_ref *ref) {
  return ((raw_client *)ref->user_data)->conn;
}

static void rand_bytes(uint8_t *dest, size_t len, [[maybe_unused]] const ngtcp2_rand_ctx *ctx) {
  for (size_t i = 0; i < len; i++)
    dest[i] = (uint8_t)rand();
}

static int new_cid(ngtcp2_conn *conn, ngtcp2_cid *cid, uint8_t *token, size_t cidlen,
                   void *user_data) {
  (void)conn;
  (void)user_data;
  uint8_t buf[NGTCP2_MAX_CIDLEN];
  rand_bytes(buf, cidlen, nullptr);
  ngtcp2_cid_init(cid, buf, cidlen);
  rand_bytes(token, NGTCP2_STATELESS_RESET_TOKENLEN, nullptr);
  return 0;
}

static int on_handshake_done(ngtcp2_conn *conn, void *user_data) {
  (void)conn;
  ((raw_client *)user_data)->handshake_done = true;
  return 0;
}

static int on_recv_stream_data(ngtcp2_conn *conn, uint32_t flags, int64_t stream_id,
                               uint64_t offset, const uint8_t *data, size_t datalen,
                               void *user_data, void *stream_user_data) {
  (void)offset;
  (void)stream_user_data;
  raw_client *k = user_data;
  if (k->h3 != nullptr) {
    nghttp3_ssize n = nghttp3_conn_read_stream(k->h3, stream_id, data, datalen,
                                               (flags & NGTCP2_STREAM_DATA_FLAG_FIN) != 0);
    if (n < 0)
      return NGTCP2_ERR_CALLBACK_FAILURE;
  }
  ngtcp2_conn_extend_max_stream_offset(conn, stream_id, datalen);
  ngtcp2_conn_extend_max_offset(conn, datalen);
  return 0;
}

static int on_acked_stream_data(ngtcp2_conn *conn, int64_t stream_id, uint64_t offset,
                                uint64_t datalen, void *user_data, void *stream_user_data) {
  (void)conn;
  (void)offset;
  (void)stream_user_data;
  raw_client *k = user_data;
  if (k->h3 != nullptr && nghttp3_conn_add_ack_offset(k->h3, stream_id, datalen) != 0)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}

static int on_quic_stream_close(ngtcp2_conn *conn, uint32_t flags, int64_t stream_id,
                                uint64_t app_error_code, void *user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  raw_client *k = user_data;
  if (k->h3 == nullptr)
    return 0;
  if (!(flags & NGTCP2_STREAM_CLOSE_FLAG_APP_ERROR_CODE_SET))
    app_error_code = NGHTTP3_H3_NO_ERROR;
  int rv = nghttp3_conn_close_stream(k->h3, stream_id, app_error_code);
  return rv == 0 || rv == NGHTTP3_ERR_STREAM_NOT_FOUND ? 0 : NGTCP2_ERR_CALLBACK_FAILURE;
}

static const ngtcp2_callbacks client_callbacks = {
    .client_initial = ngtcp2_crypto_client_initial_cb,
    .recv_crypto_data = ngtcp2_crypto_recv_crypto_data_cb,
    .handshake_completed = on_handshake_done,
    .encrypt = ngtcp2_crypto_encrypt_cb,
    .decrypt = ngtcp2_crypto_decrypt_cb,
    .hp_mask = ngtcp2_crypto_hp_mask_cb,
    .recv_stream_data = on_recv_stream_data,
    .acked_stream_data_offset = on_acked_stream_data,
    .stream_close = on_quic_stream_close,
    .recv_retry = ngtcp2_crypto_recv_retry_cb,
    .rand = rand_bytes,
    .get_new_connection_id = new_cid,
    .update_key = ngtcp2_crypto_update_key_cb,
    .delete_crypto_aead_ctx = ngtcp2_crypto_delete_crypto_aead_ctx_cb,
    .delete_crypto_cipher_ctx = ngtcp2_crypto_delete_crypto_cipher_ctx_cb,
    .get_path_challenge_data = ngtcp2_crypto_get_path_challenge_data_cb,
    .version_negotiation = ngtcp2_crypto_version_negotiation_cb,
};

/* Creates the client connection on a connected UDP socket (its first Initial is not sent yet). */
static void client_init(raw_client *k, uint16_t port) {
  k->fd = socket(AF_INET, SOCK_DGRAM, 0);
  TEST_ASSERT_TRUE(k->fd >= 0);
  k->send_fd = k->fd;
  k->remote = (struct sockaddr_in){.sin_family = AF_INET, .sin_port = htons(port)};
  k->remote.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  TEST_ASSERT_EQUAL_INT(0, connect(k->fd, (struct sockaddr *)&k->remote, sizeof(k->remote)));
  socklen_t len = sizeof(k->local);
  TEST_ASSERT_EQUAL_INT(0, getsockname(k->fd, (struct sockaddr *)&k->local, &len));

  k->tls_ctx =
      quic_tls_client_ctx_new(&(grpc_quic_tls_client_config){.insecure_skip_verify = true});
  TEST_ASSERT_NOT_NULL(k->tls_ctx);
  ngtcp2_settings settings;
  ngtcp2_settings_default(&settings);
  settings.initial_ts = now_ns();
  ngtcp2_transport_params params;
  ngtcp2_transport_params_default(&params);
  params.initial_max_stream_data_bidi_local = 1 << 20;
  params.initial_max_stream_data_bidi_remote = 1 << 20;
  params.initial_max_stream_data_uni = 1 << 20;
  params.initial_max_data = 1 << 24;
  params.initial_max_streams_uni = 3;
  params.max_idle_timeout = 10 * NGTCP2_SECONDS;
  uint8_t dbuf[18], sbuf[18];
  rand_bytes(dbuf, sizeof(dbuf), nullptr);
  rand_bytes(sbuf, sizeof(sbuf), nullptr);
  ngtcp2_cid dcid, scid;
  ngtcp2_cid_init(&dcid, dbuf, sizeof(dbuf));
  ngtcp2_cid_init(&scid, sbuf, sizeof(sbuf));
  ngtcp2_path path = {{(ngtcp2_sockaddr *)&k->local, sizeof(k->local)},
                      {(ngtcp2_sockaddr *)&k->remote, sizeof(k->remote)}};
  TEST_ASSERT_EQUAL_INT(0,
                        ngtcp2_conn_client_new(&k->conn, &dcid, &scid, &path, NGTCP2_PROTO_VER_V1,
                                               &client_callbacks, &settings, &params, nullptr, k));
  k->ref = (ngtcp2_crypto_conn_ref){.get_conn = get_conn, .user_data = k};
  k->tls = quic_tls_session_new(k->tls_ctx, &k->ref, false, "localhost");
  TEST_ASSERT_NOT_NULL(k->tls);
  ngtcp2_conn_set_tls_native_handle(k->conn, quic_tls_session_native(k->tls));
}

static void client_flush(raw_client *k) {
  uint8_t buf[1452];
  for (;;) {
    int64_t stream_id = -1;
    int fin = 0;
    nghttp3_vec vec[16];
    nghttp3_ssize sveccnt = 0;
    if (k->h3 != nullptr && ngtcp2_conn_get_max_data_left(k->conn)) {
      sveccnt = nghttp3_conn_writev_stream(k->h3, &stream_id, &fin, vec, 16);
      if (sveccnt < 0)
        return;
    }
    ngtcp2_ssize ndatalen = -1;
    ngtcp2_path_storage ps;
    ngtcp2_path_storage_zero(&ps);
    ngtcp2_pkt_info pi;
    uint32_t flags = NGTCP2_WRITE_STREAM_FLAG_MORE | (fin ? NGTCP2_WRITE_STREAM_FLAG_FIN : 0);
    ngtcp2_ssize n =
        ngtcp2_conn_writev_stream(k->conn, &ps.path, &pi, buf, sizeof(buf), &ndatalen, flags,
                                  stream_id, (const ngtcp2_vec *)vec, (size_t)sveccnt, now_ns());
    if (n < 0) {
      if (n == NGTCP2_ERR_WRITE_MORE) {
        (void)nghttp3_conn_add_write_offset(k->h3, stream_id, (size_t)ndatalen);
        continue;
      }
      if (n == NGTCP2_ERR_STREAM_DATA_BLOCKED) {
        (void)nghttp3_conn_block_stream(k->h3, stream_id);
        continue;
      }
      return;
    }
    if (k->h3 != nullptr && ndatalen >= 0 && stream_id >= 0)
      (void)nghttp3_conn_add_write_offset(k->h3, stream_id, (size_t)ndatalen);
    if (n == 0)
      break;
    (void)send(k->send_fd, buf, (size_t)n, 0);
  }
}

/* Feeds every datagram waiting on fd to the connection; counts them in *counter. */
static void client_read(raw_client *k, int fd, int *counter) {
  uint8_t buf[65536];
  for (;;) {
    ssize_t r = recv(fd, buf, sizeof(buf), MSG_DONTWAIT);
    if (r <= 0)
      break;
    if (counter != nullptr)
      (*counter)++;
    ngtcp2_path path = {{(ngtcp2_sockaddr *)&k->local, sizeof(k->local)},
                        {(ngtcp2_sockaddr *)&k->remote, sizeof(k->remote)}};
    ngtcp2_pkt_info pi = {0};
    (void)ngtcp2_conn_read_pkt(k->conn, &path, &pi, buf, (size_t)r, now_ns());
  }
}

/* Runs the client for ms milliseconds, reading k->fd (and fd2 when >= 0, counting the datagrams
 * that arrive on each). */
static void client_run(raw_client *k, unsigned ms, int fd2, int *on_fd, int *on_fd2) {
  uint64_t end = now_ns() + (uint64_t)ms * 1000000ull;
  while (now_ns() < end) {
    struct pollfd p[2] = {{k->fd, POLLIN, 0}, {fd2, POLLIN, 0}};
    (void)poll(p, fd2 >= 0 ? 2 : 1, 5);
    client_read(k, k->fd, on_fd);
    if (fd2 >= 0)
      client_read(k, fd2, on_fd2);
    if (ngtcp2_conn_get_expiry(k->conn) <= now_ns())
      (void)ngtcp2_conn_handle_expiry(k->conn, now_ns());
    client_flush(k);
    if (ngtcp2_conn_in_closing_period(k->conn) || ngtcp2_conn_in_draining_period(k->conn))
      return;
  }
}

static void client_handshake(raw_client *k, uint16_t port) {
  client_init(k, port);
  client_flush(k);
  for (int i = 0; i < 300 && !k->handshake_done; i++)
    client_run(k, 10, -1, nullptr, nullptr);
  TEST_ASSERT_TRUE_MESSAGE(k->handshake_done, "QUIC handshake did not complete");
  client_run(k, 100, -1, nullptr, nullptr);
}

/* Writes `len` bytes on stream id right away. */
static void stream_write(raw_client *k, int64_t id, const void *data, size_t len) {
  uint8_t buf[1452];
  ngtcp2_vec v = {(uint8_t *)data, len};
  ngtcp2_ssize consumed = -1;
  ngtcp2_path_storage ps;
  ngtcp2_path_storage_zero(&ps);
  ngtcp2_ssize n = ngtcp2_conn_writev_stream(k->conn, &ps.path, nullptr, buf, sizeof(buf),
                                             &consumed, 0, id, &v, 1, now_ns());
  if (n > 0)
    (void)send(k->send_fd, buf, (size_t)n, 0);
}

/* ---- tests ---------------------------------------------------------------------------- */

/* Streams the client resets before sending a byte still give their stream credit back: the
 * connection keeps its max_concurrent_streams instead of running out of streams. */
void test_h3raw_stream_credit_for_unseen_streams(void) {
  grpc_server *server = start_server((grpc_config){.max_concurrent_streams = 4});
  raw_client *k = &fx.client;
  client_handshake(k, grpc_server_http3_port(server));
  TEST_ASSERT_EQUAL_UINT64(4, ngtcp2_conn_get_streams_bidi_left(k->conn));
  for (int round = 0; round < 3; round++) {
    int opened = 0;
    int64_t id;
    while (opened < 4 && ngtcp2_conn_open_bidi_stream(k->conn, &id, nullptr) == 0) {
      (void)ngtcp2_conn_shutdown_stream(k->conn, 0, id, 0x10c /* H3_REQUEST_CANCELLED */);
      opened++;
    }
    TEST_ASSERT_EQUAL_INT(4, opened);
    uint64_t end = now_ns() + 3000000000ull;
    while (ngtcp2_conn_get_streams_bidi_left(k->conn) < 4 && now_ns() < end)
      client_run(k, 20, -1, nullptr, nullptr);
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(4, ngtcp2_conn_get_streams_bidi_left(k->conn),
                                     "stream credit was not returned");
  }
}

/* After the client's address changes (NAT rebinding), the server answers at the new address
 * and the connection keeps working. */
void test_h3raw_nat_rebinding(void) {
  grpc_server *server = start_server((grpc_config){0});
  raw_client *k = &fx.client;
  client_handshake(k, grpc_server_http3_port(server));
  int fd2 = fx.extra_fds[0] = socket(AF_INET, SOCK_DGRAM, 0);
  TEST_ASSERT_TRUE(fd2 >= 0);
  TEST_ASSERT_EQUAL_INT(0, connect(fd2, (struct sockaddr *)&k->remote, sizeof(k->remote)));
  k->send_fd = fd2; /* the client's packets now come from a new port */
  int64_t id;
  TEST_ASSERT_EQUAL_INT(0, ngtcp2_conn_open_bidi_stream(k->conn, &id, nullptr));
  stream_write(k, id, "\x01", 1);
  int on_old = 0, on_new = 0;
  client_run(k, 1500, fd2, &on_old, &on_new);
  TEST_ASSERT_TRUE_MESSAGE(on_new > 0, "the server kept sending to the old address");
  /* the old mapping expires: everything now flows over the new one */
  close(k->fd);
  k->fd = fd2;
  fx.extra_fds[0] = -1;
  (void)ngtcp2_conn_shutdown_stream(k->conn, 0, id, 0x10c);
  int64_t id2;
  TEST_ASSERT_EQUAL_INT(0, ngtcp2_conn_open_bidi_stream(k->conn, &id2, nullptr));
  stream_write(k, id2, "\x01", 1);
  client_run(k, 1500, -1, nullptr, nullptr);
  ngtcp2_conn_info ci;
  ngtcp2_conn_get_conn_info(k->conn, &ci);
  TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, ci.bytes_in_flight, "data stayed unacknowledged");
  TEST_ASSERT_FALSE(ngtcp2_conn_in_closing_period(k->conn) ||
                    ngtcp2_conn_in_draining_period(k->conn));
}

/* Initials that are never answered (e.g. from spoofed addresses) cannot take every connection
 * slot: past half of the free slots, new clients must answer a Retry first, so TCP clients
 * still get in. */
void test_h3raw_unanswered_initials_do_not_lock_out(void) {
  grpc_server *server = start_server((grpc_config){.max_connections = 4});
  int ffd = fx.extra_fds[0] = socket(AF_INET, SOCK_DGRAM, 0);
  struct sockaddr_in rem = {.sin_family = AF_INET,
                            .sin_port = htons(grpc_server_http3_port(server))};
  rem.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  TEST_ASSERT_EQUAL_INT(0, connect(ffd, (struct sockaddr *)&rem, sizeof(rem)));
  for (int i = 0; i < 4; i++) {
    raw_client one = {.fd = -1, .send_fd = -1};
    client_init(&one, grpc_server_http3_port(server));
    one.send_fd = ffd; /* all from one socket, never read: the handshakes never progress */
    uint8_t buf[1452];
    ngtcp2_path_storage ps;
    ngtcp2_path_storage_zero(&ps);
    ngtcp2_pkt_info pi;
    ngtcp2_ssize n = ngtcp2_conn_write_pkt(one.conn, &ps.path, &pi, buf, sizeof(buf), now_ns());
    TEST_ASSERT_TRUE(n > 0);
    (void)send(ffd, buf, (size_t)n, 0);
    one.send_fd = -1;
    client_free(&one);
  }
  sleep_ms(200);
  /* a TCP (HTTP/2) client where the TLS backend has HTTP/2 client support, otherwise an HTTP/3
   * one: both need one of the same connection slots */
  grpc_err err = GRPC_OK;
  grpc_client_config cc = {.host = "127.0.0.1",
                           .port = grpc_server_port(server),
                           .timeout_ms = 2000,
                           .tls = {.enable = true, .insecure_skip_verify = true}};
  grpc_client *c = grpc_client_new(&cc, &err);
  if (c == nullptr && err == GRPC_ERR_UNSUPPORTED) {
    cc.transport = GRPC_TRANSPORT_HTTP3;
    cc.port = grpc_server_http3_port(server);
    c = grpc_client_new(&cc, &err);
  }
  TEST_ASSERT_NOT_NULL_MESSAGE(c, "a new client was locked out by unanswered QUIC Initials");
  grpc_client_response resp = {0};
  grpc_err rc = grpc_client_unary(c, "/raw.Svc/Ok", "", 0, nullptr, 0, 2000, &resp);
  grpc_status status = resp.status;
  grpc_client_response_free(&resp);
  grpc_client_free(c);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, rc);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, status);
}

/* A request stream opened after the max_connection_age GOAWAY, then grpc_server_drain(): the
 * server must not send a second GOAWAY (nghttp3 aborts the process on one). */
void test_h3raw_drain_after_age_goaway(void) {
  grpc_server *server = start_server(
      (grpc_config){.max_connection_age_ms = 300, .max_connection_age_grace_ms = 20000});
  raw_client *k = &fx.client;
  client_handshake(k, grpc_server_http3_port(server));
  for (int i = 0; i < 2; i++) {
    int64_t id;
    TEST_ASSERT_EQUAL_INT(0, ngtcp2_conn_open_bidi_stream(k->conn, &id, nullptr));
    stream_write(k, id, "\x01\x10", 2); /* an unfinished HEADERS frame keeps the stream open */
    client_run(k, 800, -1, nullptr, nullptr); /* the first wait covers the age GOAWAY */
  }
  stop_server(); /* this thread now owns the server */
  grpc_err rc = grpc_server_drain(server, 0);
  TEST_ASSERT_TRUE(rc == GRPC_OK || rc == GRPC_ERR_TIMEOUT);
}

/* content-type must be application/grpc itself (or with +codec / parameters) over HTTP/3 too:
 * a longer name that merely starts with it is refused with HTTP 415. */
void test_h3raw_content_type_exact(void) {
  grpc_server *server = start_server((grpc_config){0});
  raw_client *k = &fx.client;
  client_handshake(k, grpc_server_http3_port(server));
  TEST_ASSERT_EQUAL_INT(415, h3_request(k, "application/grpcXYZ"));
  client_free(k);
  k = &fx.client;
  client_handshake(k, grpc_server_http3_port(server));
  TEST_ASSERT_EQUAL_INT(200, h3_request(k, "application/grpc+proto"));
}

int main(void) {
  srand((unsigned)now_ns());
  UNITY_BEGIN();
  RUN_TEST(test_h3raw_stream_credit_for_unseen_streams);
  RUN_TEST(test_h3raw_nat_rebinding);
  RUN_TEST(test_h3raw_unanswered_initials_do_not_lock_out);
  RUN_TEST(test_h3raw_drain_after_age_goaway);
  RUN_TEST(test_h3raw_content_type_exact);
  return UNITY_END();
}
