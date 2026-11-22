/* HTTP/3 end-to-end tests: a TLS server with config.http3 and the client with
 * GRPC_TRANSPORT_HTTP3, using the generated test certificate (CN=localhost). */
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif
#include "internal/grpc_threads.h"
#include "grpc.h"
#include "unity.h"

/* A started server on its own thread, the clients of the running test and what the handlers
 * record. Static, not on a test's stack: tearDown() stops, joins and frees whatever a failed
 * assertion left behind, so no thread outlives the test that started it. Handlers run on the
 * server thread, so they record through atomics or under `lock`. */
enum { MAX_CLIENTS = 4 };
static struct {
  grpc_server *server;
  pthread_t th;
  bool running;
  /* a second loop-owning thread (grpc_server_drain()) while `running` is false */
  pthread_t aux;
  bool aux_running;
  grpc_client *clients[MAX_CLIENTS];
  pthread_mutex_t lock;
  char peer_subject[256];
  char req_encoding[32];
  atomic_int stream_count;
  atomic_bool task_done;
  atomic_int task_result;
} fx = {.lock = PTHREAD_MUTEX_INITIALIZER};
static uint8_t *big_data;

static void fixture_cleanup(void) {
  if (fx.aux_running) {
    pthread_join(fx.aux, nullptr);
    fx.aux_running = false;
  }
  if (fx.running) {
    grpc_server_stop(fx.server);
    pthread_join(fx.th, nullptr);
    fx.running = false;
  }
  for (size_t i = 0; i < MAX_CLIENTS; i++) {
    grpc_client_free(fx.clients[i]);
    fx.clients[i] = nullptr;
  }
  grpc_server_free(fx.server);
  fx.server = nullptr;
  pthread_mutex_lock(&fx.lock);
  fx.peer_subject[0] = '\0';
  fx.req_encoding[0] = '\0';
  pthread_mutex_unlock(&fx.lock);
  atomic_store(&fx.stream_count, 0);
  atomic_store(&fx.task_done, false);
  atomic_store(&fx.task_result, GRPC_ERR_STATE);
}

void setUp(void) {
  fixture_cleanup();
}
void tearDown(void) {
  fixture_cleanup();
  free(big_data);
  big_data = nullptr;
}

/* Remembers `c` so tearDown() frees it. Returns `c`. */
static grpc_client *track(grpc_client *c) {
  for (size_t i = 0; c != nullptr && i < MAX_CLIENTS; i++) {
    if (fx.clients[i] == nullptr) {
      fx.clients[i] = c;
      return c;
    }
  }
  if (c != nullptr)
    TEST_FAIL_MESSAGE("too many clients");
  return c;
}

/* Frees a tracked client now. */
static void client_free(grpc_client *c) {
  for (size_t i = 0; i < MAX_CLIENTS; i++) {
    if (fx.clients[i] == c)
      fx.clients[i] = nullptr;
  }
  grpc_client_free(c);
}

static uint64_t now_ms(void) {
  struct timespec ts;
  (void)timespec_get(&ts, TIME_UTC);
  return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

static void sleep_ms(unsigned ms) {
#ifdef _WIN32
  Sleep(ms);
#else
  struct timespec ts = {.tv_sec = ms / 1000, .tv_nsec = (long)(ms % 1000) * 1000000L};
  (void)nanosleep(&ts, nullptr);
#endif
}

/* Waits up to timeout_ms for `flag`. */
static bool wait_flag(atomic_bool *flag, unsigned timeout_ms) {
  uint64_t deadline = now_ms() + timeout_ms;
  while (!atomic_load(flag)) {
    if (now_ms() >= deadline)
      return false;
    sleep_ms(2);
  }
  return true;
}

/* Copies a string a handler recorded, under the lock. */
static void fx_read(const char *src, char *out, size_t out_len) {
  pthread_mutex_lock(&fx.lock);
  snprintf(out, out_len, "%s", src);
  pthread_mutex_unlock(&fx.lock);
}

static void *run_server(void *arg) {
  (void)grpc_server_run((grpc_server *)arg);
  return nullptr;
}

static void echo(grpc_call *call, const grpc_message *req, void *userdata) {
  (void)userdata;
  (void)grpc_call_reply_data(call, req->data, req->len);
}

/* echo that records the request's grpc-encoding */
static void echo_enc(grpc_call *call, const grpc_message *req, void *userdata) {
  (void)userdata;
  const char *enc = grpc_call_metadata(call, "grpc-encoding", nullptr);
  pthread_mutex_lock(&fx.lock);
  snprintf(fx.req_encoding, sizeof(fx.req_encoding), "%s", enc != nullptr ? enc : "(none)");
  pthread_mutex_unlock(&fx.lock);
  (void)grpc_call_reply_data(call, req->data, req->len);
}

static void count_message(const grpc_message *msg, void *userdata) {
  (void)msg;
  (*(int *)userdata)++;
}

static grpc_client *new_h3_client(grpc_server *server, grpc_err *err) {
  return track(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_http3_port(server),
          .timeout_ms = 3000,
          .transport = GRPC_TRANSPORT_HTTP3,
          .tls = {.enable = true, .ca_file = "test_cert.pem", .server_name = "localhost"},
      },
      err));
}

void test_http3_available(void) {
  TEST_ASSERT_TRUE(grpc_http3_available());
  printf("HTTP/3: %s\n", grpc_http3_versions());
}

static void stream_three(grpc_call *call, const grpc_message *req, void *userdata) {
  (void)req;
  (void)userdata;
  for (int i = 0; i < 3; i++) {
    char msg[16];
    int n = snprintf(msg, sizeof(msg), "item-%d", i);
    (void)grpc_call_send_data(call, msg, (size_t)n);
  }
  (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

static void sum_on_start(grpc_call *call, void *userdata) {
  (void)userdata;
  size_t *total = grpc_calloc(1, sizeof(*total));
  grpc_call_set_userdata(call, total, grpc_free);
}
static void sum_on_message(grpc_call *call, const grpc_message *msg, void *userdata) {
  (void)userdata;
  size_t *total = grpc_call_userdata(call);
  if (total != nullptr)
    *total += msg->len;
}
static void sum_on_end(grpc_call *call, void *userdata) {
  (void)userdata;
  size_t *total = grpc_call_userdata(call);
  char out[32];
  int n = snprintf(out, sizeof(out), "%zu", total != nullptr ? *total : (size_t)0);
  (void)grpc_call_reply_data(call, out, (size_t)n);
}

static void bidi_on_message(grpc_call *call, const grpc_message *msg, void *userdata) {
  (void)userdata;
  (void)grpc_call_send_data(call, msg->data, msg->len);
}
static void bidi_on_end(grpc_call *call, void *userdata) {
  (void)userdata;
  (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

/* never answers: the client's deadline has to fire */
static void stall(grpc_call *call, const grpc_message *req, void *userdata) {
  (void)req;
  (void)userdata;
  (void)call;
}

static void whoami(grpc_call *call, const grpc_message *req, void *userdata) {
  (void)req;
  (void)userdata;
  const char *subj = grpc_call_peer_cert_subject(call);
  pthread_mutex_lock(&fx.lock);
  snprintf(fx.peer_subject, sizeof(fx.peer_subject), "%s", subj != nullptr ? subj : "");
  pthread_mutex_unlock(&fx.lock);
  (void)grpc_call_reply_data(call, "ok", 2);
}

/* Replies "<x-key value>|<deadline set>". Header values from nghttp3 must be NUL-terminated like
 * nghttp2's (grpc_call_metadata() and the grpc-timeout parser rely on it): strlen() must agree
 * with the reported length. */
static void meta(grpc_call *call, const grpc_message *req, void *userdata) {
  (void)req;
  (void)userdata;
  size_t len = 0;
  const char *v = grpc_call_metadata(call, "x-key", &len);
  char out[128];
  int n = snprintf(out, sizeof(out), "%s|%d", v != nullptr && strlen(v) == len ? v : "BAD",
                   grpc_call_deadline(call) > 0);
  (void)grpc_call_reply_data(call, out, (size_t)n);
}

/* Creates fx.server with the test handlers and runs it on fx.th. */
static grpc_server *fixture_start(grpc_config cfg) {
  TEST_ASSERT_NULL(fx.server);
  fx.server = grpc_server_new(&cfg);
  TEST_ASSERT_NOT_NULL(fx.server);
  grpc_server *s = fx.server;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(s, "/h3.Svc/Echo", echo, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(s, "/h3.Svc/Three", stream_three, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(s, "/h3.Svc/Stall", stall, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(s, "/h3.Svc/Meta", meta, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(s, "/h3.Svc/WhoAmI", whoami, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(s, "/h3.Svc/Enc", echo_enc, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle_stream(s, "/h3.Svc/Sum",
                                                           &(grpc_stream_handler){
                                                               .on_start = sum_on_start,
                                                               .on_message = sum_on_message,
                                                               .on_end = sum_on_end,
                                                           },
                                                           nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle_stream(s, "/h3.Svc/Bidi",
                                                           &(grpc_stream_handler){
                                                               .on_message = bidi_on_message,
                                                               .on_end = bidi_on_end,
                                                           },
                                                           nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_enable(s));
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&fx.th, nullptr, run_server, s));
  fx.running = true;
  return s;
}

/* Stops the server thread: the test thread then owns the loop. */
static void fixture_stop_thread(void) {
  if (fx.running) {
    grpc_server_stop(fx.server);
    pthread_join(fx.th, nullptr);
    fx.running = false;
  }
}

static void fixture_stop(void) {
  fixture_cleanup();
}

static grpc_config h3_config(void) {
  return (grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .tls = {.cert_file = "test_cert.pem", .key_file = "test_key.pem"},
      .http3 = {.enable = true},
      .log_level = GRPC_LOG_OFF,
  };
}

void test_http3_unary(void) {
  grpc_server *server = fixture_start(h3_config());
  TEST_ASSERT_TRUE(grpc_server_http3_port(server) != 0);

  grpc_err err = GRPC_OK;
  grpc_client *client = new_h3_client(server, &err);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
  TEST_ASSERT_NOT_NULL(client);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/h3.Svc/Echo", "hello", 5, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32(5, resp.message->len);
  TEST_ASSERT_EQUAL_MEMORY("hello", resp.message->data, 5);
  grpc_client_response_free(&resp);

  /* many RPCs on one connection: stream IDs advance and stream credit is returned */
  for (int i = 0; i < 300; i++) {
    char msg[32];
    int n = snprintf(msg, sizeof(msg), "msg-%d", i);
    TEST_ASSERT_EQUAL_INT(
        GRPC_OK, grpc_client_unary(client, "/h3.Svc/Echo", msg, (size_t)n, nullptr, 0, 0, &resp));
    TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
    TEST_ASSERT_NOT_NULL(resp.message);
    TEST_ASSERT_EQUAL_MEMORY(msg, resp.message->data, (size_t)n);
    grpc_client_response_free(&resp);
  }

  /* request metadata and grpc-timeout reach the handler intact */
  grpc_metadata md[] = {{.key = "x-key", .value = "value-123"}};
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(client, "/h3.Svc/Meta", "", 0, md, 1, 5000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32(11, resp.message->len);
  TEST_ASSERT_EQUAL_MEMORY("value-123|1", resp.message->data, 11);
  grpc_client_response_free(&resp);

  /* unknown method: trailers-only UNIMPLEMENTED */
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(client, "/h3.Svc/Nope", "x", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNIMPLEMENTED, resp.status);
  TEST_ASSERT_TRUE(resp.nb_trailers > 0);
  grpc_client_response_free(&resp);
}

void test_http3_streaming(void) {
  grpc_server *server = fixture_start(h3_config());
  grpc_err err = GRPC_OK;
  grpc_client *client = new_h3_client(server, &err);
  TEST_ASSERT_NOT_NULL(client);

  /* server streaming */
  int seen = 0;
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_server_stream(client, "/h3.Svc/Three", "", 0, nullptr,
                                                           0, 0, count_message, &seen, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_EQUAL_INT(3, seen);
  grpc_client_response_free(&resp);

  /* client streaming */
  grpc_client_stream *cs = grpc_client_stream_open(client, "/h3.Svc/Sum", nullptr, 0, 0, &err);
  TEST_ASSERT_NOT_NULL(cs);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(cs, "ab", 2));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(cs, "cde", 3));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_finish(cs, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32(1, resp.message->len);
  TEST_ASSERT_EQUAL_MEMORY("5", resp.message->data, 1);
  grpc_client_response_free(&resp);

  /* bidi: interleaved send and receive */
  grpc_client_stream *bs = grpc_client_stream_open(client, "/h3.Svc/Bidi", nullptr, 0, 0, &err);
  TEST_ASSERT_NOT_NULL(bs);
  for (int i = 0; i < 5; i++) {
    char msg[16];
    int n = snprintf(msg, sizeof(msg), "ping-%d", i);
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(bs, msg, (size_t)n));
    grpc_message *reply = nullptr;
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(bs, &reply));
    TEST_ASSERT_NOT_NULL(reply);
    TEST_ASSERT_EQUAL_MEMORY(msg, reply->data, (size_t)n);
    grpc_message_free(reply);
  }
  /* a second RPC on the busy connection is refused, not interleaved */
  TEST_ASSERT_NULL(grpc_client_stream_open(client, "/h3.Svc/Echo", nullptr, 0, 0, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_STATE, err);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_finish(bs, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* cancel mid-stream, then the connection still serves */
  cs = grpc_client_stream_open(client, "/h3.Svc/Sum", nullptr, 0, 0, &err);
  TEST_ASSERT_NOT_NULL(cs);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(cs, "abc", 3));
  grpc_client_stream_cancel(cs);
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/h3.Svc/Echo", "after", 5, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
}

void test_http3_large_and_limits(void) {
  grpc_config cfg = h3_config();
  cfg.max_message_size = 8u << 20;
  grpc_server *server = fixture_start(cfg);
  grpc_err err = GRPC_OK;
  grpc_client *client = new_h3_client(server, &err);
  TEST_ASSERT_NOT_NULL(client);

  /* 4 MiB each way: many packets, flow-control window updates, chunk acknowledgement */
  size_t big = 4u << 20;
  free(big_data);
  big_data = malloc(big);
  uint8_t *data = big_data;
  TEST_ASSERT_NOT_NULL(data);
  for (size_t i = 0; i < big; i++)
    data[i] = (uint8_t)(i * 31 + 7);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/h3.Svc/Echo", data, big, nullptr, 0, 10000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32((uint32_t)big, resp.message->len);
  TEST_ASSERT_EQUAL_MEMORY(data, resp.message->data, big);
  grpc_client_response_free(&resp);
  fixture_stop();

  /* over the server's limit: RESOURCE_EXHAUSTED, the connection survives */
  cfg = h3_config();
  cfg.max_message_size = 64 * 1024;
  server = fixture_start(cfg);
  client = new_h3_client(server, &err);
  TEST_ASSERT_NOT_NULL(client);
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/h3.Svc/Echo", data, 256 * 1024, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_RESOURCE_EXHAUSTED, resp.status);
  grpc_client_response_free(&resp);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(client, "/h3.Svc/Echo", "ok", 2, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  free(big_data);
  big_data = nullptr;

  /* deadline: the client gives up and resets the stream */
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_TIMEOUT,
                        grpc_client_unary(client, "/h3.Svc/Stall", "x", 1, nullptr, 0, 200, &resp));
  grpc_client_response_free(&resp);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(client, "/h3.Svc/Echo", "ok", 2, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
}

/* Returns the value of response header `key`, or nullptr. */
static const char *resp_header(const grpc_client_response *resp, const char *key) {
  for (size_t i = 0; i < resp->nb_headers; i++) {
    if (strcmp(resp->headers[i].key, key) == 0)
      return resp->headers[i].value;
  }
  return nullptr;
}

void test_http3_compression_and_mtls(void) {
  grpc_config cfg = h3_config();
  cfg.tls.ca_file = "test_cert.pem";
  cfg.tls.require_client_cert = true;
  cfg.default_compression = GRPC_COMPRESS_GZIP;
  grpc_server *server = fixture_start(cfg);

  grpc_err err = GRPC_OK;
  grpc_client *client = track(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_http3_port(server),
          .timeout_ms = 3000,
          .transport = GRPC_TRANSPORT_HTTP3,
          .compression = GRPC_COMPRESS_GZIP,
          .tls = {.enable = true,
                  .ca_file = "test_cert.pem",
                  .cert_file = "test_cert.pem",
                  .key_file = "test_key.pem",
                  .server_name = "localhost"},
      },
      &err));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
  TEST_ASSERT_NOT_NULL(client);
  grpc_client_response resp = {0};
  char text[2048];
  memset(text, 'z', sizeof(text));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/h3.Svc/Enc", text, sizeof(text), nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32(sizeof(text), resp.message->len);
  TEST_ASSERT_EQUAL_MEMORY(text, resp.message->data, sizeof(text));
  /* the request went out gzip-compressed and the response came back gzip-compressed */
  char seen[256];
  fx_read(fx.req_encoding, seen, sizeof(seen));
  TEST_ASSERT_EQUAL_STRING("gzip", seen);
  const char *enc = resp_header(&resp, "grpc-encoding");
  TEST_ASSERT_NOT_NULL(enc);
  TEST_ASSERT_EQUAL_STRING("gzip", enc);
  grpc_client_response_free(&resp);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(client, "/h3.Svc/WhoAmI", "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  fx_read(fx.peer_subject, seen, sizeof(seen));
  TEST_ASSERT_NOT_NULL(strstr(seen, "localhost"));
  grpc_client_response_free(&resp);
  client_free(client);

  /* without a client certificate the server refuses the connection. With TLS 1.3 the client may
   * finish its handshake first, so the refusal can surface on the first RPC instead */
  client = new_h3_client(server, &err);
  if (client != nullptr) {
    grpc_err rpc = grpc_client_unary(client, "/h3.Svc/Echo", "x", 1, nullptr, 0, 1000, &resp);
    TEST_ASSERT_TRUE(rpc != GRPC_OK || resp.status != GRPC_STATUS_OK);
    grpc_client_response_free(&resp);
  }
}

#ifndef _WIN32
/* Sends one tokenless QUIC v1 Initial (header only, padded to the 1200-byte minimum; the payload
 * is never decrypted) from a fresh UDP socket and returns whether a Retry came back for it. */
static bool probe_retry(uint16_t port) {
  int fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0)
    return false;
  static const uint8_t dcid[8] = {1, 2, 3, 4, 5, 6, 7, 8};
  static const uint8_t scid[8] = {'r', 'e', 't', 'r', 'y', 'c', 'i', 'd'};
  uint8_t pkt[1200] = {0};
  size_t off = 0;
  pkt[off++] = 0xC0; /* long header, fixed bit, Initial, 1-byte packet number */
  pkt[off++] = 0;
  pkt[off++] = 0;
  pkt[off++] = 0;
  pkt[off++] = 1; /* QUIC v1 */
  pkt[off++] = sizeof(dcid);
  memcpy(pkt + off, dcid, sizeof(dcid));
  off += sizeof(dcid);
  pkt[off++] = sizeof(scid);
  memcpy(pkt + off, scid, sizeof(scid));
  off += sizeof(scid);
  pkt[off++] = 0; /* token length */
  size_t rest = sizeof(pkt) - off - 2;
  pkt[off++] = (uint8_t)(0x40 | (rest >> 8)); /* 2-byte varint: packet number + payload */
  pkt[off++] = (uint8_t)(rest & 0xFF);

  struct sockaddr_in sin = {.sin_family = AF_INET, .sin_port = htons(port)};
  inet_pton(AF_INET, "127.0.0.1", &sin.sin_addr);
  bool got_retry = false;
  for (int attempt = 0; attempt < 3 && !got_retry; attempt++) {
    if (sendto(fd, pkt, sizeof(pkt), 0, (struct sockaddr *)&sin, sizeof(sin)) !=
        (ssize_t)sizeof(pkt))
      break;
    struct pollfd pfd = {.fd = fd, .events = POLLIN};
    if (poll(&pfd, 1, 2000) <= 0)
      continue;
    uint8_t buf[1500];
    ssize_t n = recv(fd, buf, sizeof(buf), 0);
    /* Retry: long header type 3, version 1, DCID = our SCID */
    got_retry = n > 6 + (ssize_t)sizeof(scid) && (buf[0] & 0xF0) == 0xF0 && buf[1] == 0 &&
                buf[2] == 0 && buf[3] == 0 && buf[4] == 1 && buf[5] == sizeof(scid) &&
                memcmp(buf + 6, scid, sizeof(scid)) == 0;
  }
  close(fd);
  return got_retry;
}
#endif

/* require_retry: a tokenless Initial is answered with a Retry and creates no connection, and the
 * client completes the Retry exchange */
void test_http3_retry(void) {
  grpc_config cfg = h3_config();
  cfg.http3.require_retry = true;
  grpc_server *server = fixture_start(cfg);
  grpc_server_stats stats = {0};
#ifndef _WIN32
  TEST_ASSERT_TRUE(probe_retry(grpc_server_http3_port(server)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_get_stats(server, &stats));
  TEST_ASSERT_EQUAL_UINT64(0, stats.total_connections);
#endif
  grpc_err err = GRPC_OK;
  grpc_client *client = new_h3_client(server, &err);
  TEST_ASSERT_NOT_NULL(client);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/h3.Svc/Echo", "retry", 5, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_get_stats(server, &stats));
  TEST_ASSERT_EQUAL_UINT64(1, stats.total_connections);
  fixture_stop();

#ifndef _WIN32
  /* without require_retry the same probe gets no Retry */
  server = fixture_start(h3_config());
  TEST_ASSERT_FALSE(probe_retry(grpc_server_http3_port(server)));
#endif
}

/* grpc_server_reload_tls() belongs on the server thread: run it there with grpc_server_post() */
static void reload_task(void *arg) {
  atomic_store(
      &fx.task_result,
      grpc_server_reload_tls((grpc_server *)arg, &(grpc_tls_config){.cert_file = "test_cert.pem",
                                                                    .key_file = "test_key.pem"}));
  atomic_store(&fx.task_done, true);
}

/* h2 and h3 on one server, with Alt-Svc advertised over h2 */
void test_http3_alongside_http2(void) {
  grpc_config cfg = h3_config();
  cfg.http3.advertise_alt_svc = true;
  grpc_server *server = fixture_start(cfg);
  TEST_ASSERT_EQUAL_UINT16(grpc_server_port(server), grpc_server_http3_port(server));

  grpc_err err = GRPC_OK;
  grpc_client *h3 = new_h3_client(server, &err);
  TEST_ASSERT_NOT_NULL(h3);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(h3, "/h3.Svc/Echo", "b", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* the HTTP/2 client supports TLS only with OpenSSL-family backends */
  grpc_client *h2 = track(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .timeout_ms = 3000,
          .tls = {.enable = true, .ca_file = "test_cert.pem", .server_name = "localhost"},
      },
      &err));
  if (h2 != nullptr) {
    TEST_ASSERT_EQUAL_INT(GRPC_OK,
                          grpc_client_unary(h2, "/h3.Svc/Echo", "a", 1, nullptr, 0, 0, &resp));
    TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
    TEST_ASSERT_NOT_NULL(resp_header(&resp, "alt-svc"));
    grpc_client_response_free(&resp);
  } else {
    TEST_ASSERT_EQUAL_INT(GRPC_ERR_UNSUPPORTED, err);
  }

  /* hot reload swaps the QUIC context too; a new HTTP/3 connection works */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_post(server, reload_task, server));
  TEST_ASSERT_TRUE_MESSAGE(wait_flag(&fx.task_done, 10000), "reload task did not run");
  TEST_ASSERT_EQUAL_INT(GRPC_OK, atomic_load(&fx.task_result));
  grpc_client *h3b = new_h3_client(server, &err);
  TEST_ASSERT_NOT_NULL(h3b);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(h3b, "/h3.Svc/Echo", "c", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  grpc_server_stats stats = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_get_stats(server, &stats));
  TEST_ASSERT_EQUAL_UINT64(h2 != nullptr ? 3 : 2, stats.active_connections);
}

/* the hostile protobuf lengths of the health parser, over HTTP/3 */
void test_http3_hostile_health(void) {
  grpc_server *server = fixture_start(h3_config());
  grpc_err err = GRPC_OK;
  grpc_client *client = new_h3_client(server, &err);
  TEST_ASSERT_NOT_NULL(client);
  const uint8_t hostile[] = {0x0A, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 'x'};
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(client, "/grpc.health.v1.Health/Check", hostile,
                                                   sizeof(hostile), nullptr, 0, 0, &resp));
  /* the malformed service name is ignored: the overall status, HealthCheckResponse{SERVING} */
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32(2, resp.message->len);
  TEST_ASSERT_EQUAL_HEX8(0x08, resp.message->data[0]);
  TEST_ASSERT_EQUAL_HEX8(GRPC_HEALTH_SERVING, resp.message->data[1]);
  grpc_client_response_free(&resp);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(client, "/grpc.health.v1.Health/Check", "", 0,
                                                   nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
}

/* Runs grpc_server_drain() as the loop thread, recording its result. */
static void *drain_thread(void *arg) {
  atomic_store(&fx.task_result, grpc_server_drain((grpc_server *)arg, 10000));
  atomic_store(&fx.task_done, true);
  return nullptr;
}

/* Graceful drain with an RPC in flight: GOAWAY, the open RPC completes, new connections are not
 * accepted. */
void test_http3_drain(void) {
  grpc_server *server = fixture_start(h3_config());
  grpc_err err = GRPC_OK;
  grpc_client *client = new_h3_client(server, &err);
  TEST_ASSERT_NOT_NULL(client);

  /* an open bidi stream the server has seen (one round trip) */
  grpc_client_stream *bs = grpc_client_stream_open(client, "/h3.Svc/Bidi", nullptr, 0, 0, &err);
  TEST_ASSERT_NOT_NULL(bs);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(bs, "one", 3));
  grpc_message *reply = nullptr;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(bs, &reply));
  TEST_ASSERT_NOT_NULL(reply);
  TEST_ASSERT_EQUAL_MEMORY("one", reply->data, 3);
  grpc_message_free(reply);

  /* drain must run on the loop thread: stop the run thread and take the loop over. A
   * non-blocking drain stops accepting and sends GOAWAY; the stream is still open */
  fixture_stop_thread();
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_TIMEOUT, grpc_server_drain(server, 0));
  /* hand the loop to a thread that drains to completion */
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&fx.aux, nullptr, drain_thread, server));
  fx.aux_running = true;

  /* a new connection is not accepted while draining */
  grpc_client *late = track(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_http3_port(server),
          .timeout_ms = 500,
          .transport = GRPC_TRANSPORT_HTTP3,
          .tls = {.enable = true, .ca_file = "test_cert.pem", .server_name = "localhost"},
      },
      &err));
  TEST_ASSERT_NULL(late);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_IO, err);

  /* the RPC in flight still completes */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(bs, "two", 3));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(bs, &reply));
  TEST_ASSERT_NOT_NULL(reply);
  TEST_ASSERT_EQUAL_MEMORY("two", reply->data, 3);
  grpc_message_free(reply);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_finish(bs, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* the client received the GOAWAY: it refuses a new request on this connection itself, instead
   * of waiting for the deadline */
  grpc_err after = grpc_client_unary(client, "/h3.Svc/Echo", "x", 1, nullptr, 0, 2000, &resp);
  grpc_client_response_free(&resp);
  TEST_ASSERT_TRUE(after != GRPC_OK && after != GRPC_ERR_TIMEOUT);

  /* and then the drain finishes. The server counts the stream until the client acknowledges the
   * end of the response, which a client that is not inside a call never reads: closing the client
   * delivers that (CONNECTION_CLOSE ends every stream of the connection) */
  client_free(client);
  pthread_join(fx.aux, nullptr);
  fx.aux_running = false;
  TEST_ASSERT_TRUE(atomic_load(&fx.task_done));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, atomic_load(&fx.task_result));
}

void test_http3_config_errors(void) {
  /* HTTP/3 needs TLS */
  grpc_err err = GRPC_OK;
  grpc_server *s = grpc_server_new_ex(&(grpc_config){.port = 0,
                                                     .ipv4_addr = "127.0.0.1",
                                                     .http3 = {.enable = true},
                                                     .log_level = GRPC_LOG_OFF},
                                      &err);
  TEST_ASSERT_NULL(s);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  /* ... and an IP address: QUIC cannot listen on a Unix socket */
  s = grpc_server_new_ex(
      &(grpc_config){.unix_path = "/tmp/grpc-h3-test.sock",
                     .tls = {.cert_file = "test_cert.pem", .key_file = "test_key.pem"},
                     .http3 = {.enable = true},
                     .log_level = GRPC_LOG_OFF},
      &err);
  TEST_ASSERT_NULL(s);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);

  /* the client needs TLS, and no Unix socket */
  TEST_ASSERT_NULL(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = 1, .transport = GRPC_TRANSPORT_HTTP3},
      &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  TEST_ASSERT_NULL(grpc_client_new(&(grpc_client_config){.unix_path = "/tmp/x.sock",
                                                         .transport = GRPC_TRANSPORT_HTTP3,
                                                         .tls = {.enable = true}},
                                   &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  /* nobody listening: the handshake times out */
  TEST_ASSERT_NULL(
      grpc_client_new(&(grpc_client_config){.host = "127.0.0.1",
                                            .port = 9,
                                            .timeout_ms = 300,
                                            .transport = GRPC_TRANSPORT_HTTP3,
                                            .tls = {.enable = true, .insecure_skip_verify = true}},
                      &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_IO, err);
}

int main(void) {
  setvbuf(stdout, nullptr, _IONBF, 0);
  UNITY_BEGIN();
  RUN_TEST(test_http3_available);
  RUN_TEST(test_http3_unary);
  RUN_TEST(test_http3_streaming);
  RUN_TEST(test_http3_large_and_limits);
  RUN_TEST(test_http3_compression_and_mtls);
  RUN_TEST(test_http3_retry);
  RUN_TEST(test_http3_alongside_http2);
  RUN_TEST(test_http3_hostile_health);
  RUN_TEST(test_http3_drain);
  RUN_TEST(test_http3_config_errors);
  return UNITY_END();
}
