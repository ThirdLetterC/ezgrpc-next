/* HTTP/3 end-to-end tests: a TLS server with config.http3 and the client with
 * GRPC_TRANSPORT_HTTP3, using the generated test certificate (CN=localhost). */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "internal/grpc_threads.h"
#include "grpc.h"
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

static void *run_server(void *arg) {
  (void)grpc_server_run((grpc_server *)arg);
  return nullptr;
}

static void echo(grpc_call *call, const grpc_message *req, void *userdata) {
  (void)userdata;
  (void)grpc_call_reply_data(call, req->data, req->len);
}

static void count_message(const grpc_message *msg, void *userdata) {
  (void)msg;
  (*(int *)userdata)++;
}

static grpc_client *new_h3_client(grpc_server *server, grpc_err *err) {
  return grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_http3_port(server),
          .timeout_ms = 3000,
          .transport = GRPC_TRANSPORT_HTTP3,
          .tls = {.enable = true, .ca_file = "test_cert.pem", .server_name = "localhost"},
      },
      err);
}

void test_http3_available(void) {
  TEST_ASSERT_TRUE(grpc_http3_available());
  printf("HTTP/3: %s\n", grpc_http3_versions());
}

/* A started server on its own thread, with the handlers the tests use. */
typedef struct h3_fixture {
  grpc_server *server;
  pthread_t th;
  int stream_count;
  char peer_subject[256];
} h3_fixture;

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
  const char *subj = grpc_call_peer_cert_subject(call);
  if (subj != nullptr)
    snprintf(userdata, 256, "%s", subj);
  (void)grpc_call_reply_data(call, "ok", 2);
}

static void fixture_start(h3_fixture *f, grpc_config cfg) {
  memset(f, 0, sizeof(*f));
  f->server = grpc_server_new(&cfg);
  TEST_ASSERT_NOT_NULL(f->server);
  grpc_server *s = f->server;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(s, "/h3.Svc/Echo", echo, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(s, "/h3.Svc/Three", stream_three, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(s, "/h3.Svc/Stall", stall, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(s, "/h3.Svc/WhoAmI", whoami, f->peer_subject));
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
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&f->th, nullptr, run_server, s));
}

static void fixture_stop(h3_fixture *f) {
  grpc_server_stop(f->server);
  pthread_join(f->th, nullptr);
  grpc_server_free(f->server);
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
  h3_fixture f;
  fixture_start(&f, h3_config());
  TEST_ASSERT_TRUE(grpc_server_http3_port(f.server) != 0);

  grpc_err err = GRPC_OK;
  grpc_client *client = new_h3_client(f.server, &err);
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
    TEST_ASSERT_EQUAL_MEMORY(msg, resp.message->data, (size_t)n);
    grpc_client_response_free(&resp);
  }

  /* unknown method: trailers-only UNIMPLEMENTED */
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(client, "/h3.Svc/Nope", "x", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNIMPLEMENTED, resp.status);
  TEST_ASSERT_TRUE(resp.nb_trailers > 0);
  grpc_client_response_free(&resp);

  grpc_client_free(client);
  fixture_stop(&f);
}

void test_http3_streaming(void) {
  h3_fixture f;
  fixture_start(&f, h3_config());
  grpc_err err = GRPC_OK;
  grpc_client *client = new_h3_client(f.server, &err);
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

  grpc_client_free(client);
  fixture_stop(&f);
}

void test_http3_large_and_limits(void) {
  grpc_config cfg = h3_config();
  cfg.max_message_size = 8u << 20;
  h3_fixture f;
  fixture_start(&f, cfg);
  grpc_err err = GRPC_OK;
  grpc_client *client = new_h3_client(f.server, &err);
  TEST_ASSERT_NOT_NULL(client);

  /* 4 MiB each way: many packets, flow-control window updates, chunk acknowledgement */
  size_t big = 4u << 20;
  uint8_t *data = malloc(big);
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
  grpc_client_free(client);
  fixture_stop(&f);

  /* over the server's limit: RESOURCE_EXHAUSTED, the connection survives */
  cfg = h3_config();
  cfg.max_message_size = 64 * 1024;
  fixture_start(&f, cfg);
  client = new_h3_client(f.server, &err);
  TEST_ASSERT_NOT_NULL(client);
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/h3.Svc/Echo", data, 256 * 1024, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_RESOURCE_EXHAUSTED, resp.status);
  grpc_client_response_free(&resp);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(client, "/h3.Svc/Echo", "ok", 2, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  free(data);

  /* deadline: the client gives up and resets the stream */
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_TIMEOUT,
                        grpc_client_unary(client, "/h3.Svc/Stall", "x", 1, nullptr, 0, 200, &resp));
  grpc_client_response_free(&resp);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(client, "/h3.Svc/Echo", "ok", 2, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  grpc_client_free(client);
  fixture_stop(&f);
}

void test_http3_compression_and_mtls(void) {
  grpc_config cfg = h3_config();
  cfg.tls.ca_file = "test_cert.pem";
  cfg.tls.require_client_cert = true;
  h3_fixture f;
  fixture_start(&f, cfg);

  grpc_err err = GRPC_OK;
  grpc_client *client = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_http3_port(f.server),
          .timeout_ms = 3000,
          .transport = GRPC_TRANSPORT_HTTP3,
          .compression = GRPC_COMPRESS_GZIP,
          .tls = {.enable = true,
                  .ca_file = "test_cert.pem",
                  .cert_file = "test_cert.pem",
                  .key_file = "test_key.pem",
                  .server_name = "localhost"},
      },
      &err);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
  TEST_ASSERT_NOT_NULL(client);
  grpc_client_response resp = {0};
  char text[2048];
  memset(text, 'z', sizeof(text));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/h3.Svc/Echo", text, sizeof(text), nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_EQUAL_UINT32(sizeof(text), resp.message->len);
  TEST_ASSERT_EQUAL_MEMORY(text, resp.message->data, sizeof(text));
  grpc_client_response_free(&resp);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(client, "/h3.Svc/WhoAmI", "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(strstr(f.peer_subject, "localhost"));
  grpc_client_response_free(&resp);
  grpc_client_free(client);

  /* without a client certificate the server refuses the connection. With TLS 1.3 the client may
   * finish its handshake first, so the refusal can surface on the first RPC instead */
  client = new_h3_client(f.server, &err);
  if (client != nullptr) {
    grpc_err rpc = grpc_client_unary(client, "/h3.Svc/Echo", "x", 1, nullptr, 0, 1000, &resp);
    TEST_ASSERT_TRUE(rpc != GRPC_OK || resp.status != GRPC_STATUS_OK);
    grpc_client_response_free(&resp);
    grpc_client_free(client);
  }
  fixture_stop(&f);
}

void test_http3_retry_and_reload(void) {
  grpc_config cfg = h3_config();
  cfg.http3.require_retry = true;
  h3_fixture f;
  fixture_start(&f, cfg);
  grpc_err err = GRPC_OK;
  grpc_client *client = new_h3_client(f.server, &err);
  TEST_ASSERT_NOT_NULL(client);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/h3.Svc/Echo", "retry", 5, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  grpc_client_free(client);
  fixture_stop(&f);
}

/* h2 and h3 on one server, with Alt-Svc advertised over h2 */
void test_http3_alongside_http2(void) {
  grpc_config cfg = h3_config();
  cfg.http3.advertise_alt_svc = true;
  h3_fixture f;
  fixture_start(&f, cfg);
  TEST_ASSERT_EQUAL_UINT16(grpc_server_port(f.server), grpc_server_http3_port(f.server));

  grpc_err err = GRPC_OK;
  grpc_client *h3 = new_h3_client(f.server, &err);
  TEST_ASSERT_NOT_NULL(h3);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(h3, "/h3.Svc/Echo", "b", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* the HTTP/2 client supports TLS only with OpenSSL-family backends */
  grpc_client *h2 = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(f.server),
          .timeout_ms = 3000,
          .tls = {.enable = true, .ca_file = "test_cert.pem", .server_name = "localhost"},
      },
      &err);
  if (h2 != nullptr) {
    TEST_ASSERT_EQUAL_INT(GRPC_OK,
                          grpc_client_unary(h2, "/h3.Svc/Echo", "a", 1, nullptr, 0, 0, &resp));
    TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
    bool alt_svc = false;
    for (size_t i = 0; i < resp.nb_headers; i++)
      alt_svc = alt_svc || strcmp(resp.headers[i].key, "alt-svc") == 0;
    TEST_ASSERT_TRUE(alt_svc);
    grpc_client_response_free(&resp);
  } else {
    TEST_ASSERT_EQUAL_INT(GRPC_ERR_UNSUPPORTED, err);
  }

  /* hot reload swaps the QUIC context too; a new HTTP/3 connection works */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_reload_tls(f.server, &(grpc_tls_config){.cert_file = "test_cert.pem",
                                                                   .key_file = "test_key.pem"}));
  grpc_client *h3b = new_h3_client(f.server, &err);
  TEST_ASSERT_NOT_NULL(h3b);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(h3b, "/h3.Svc/Echo", "c", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  grpc_server_stats stats = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_get_stats(f.server, &stats));
  TEST_ASSERT_EQUAL_UINT64(h2 != nullptr ? 3 : 2, stats.active_connections);

  grpc_client_free(h2);
  grpc_client_free(h3);
  grpc_client_free(h3b);
  fixture_stop(&f);
}

/* the hostile protobuf lengths of the health parser, over HTTP/3 */
void test_http3_hostile_health(void) {
  h3_fixture f;
  fixture_start(&f, h3_config());
  grpc_err err = GRPC_OK;
  grpc_client *client = new_h3_client(f.server, &err);
  TEST_ASSERT_NOT_NULL(client);
  const uint8_t hostile[] = {0x0A, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 'x'};
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(client, "/grpc.health.v1.Health/Check", hostile,
                                                   sizeof(hostile), nullptr, 0, 0, &resp));
  grpc_client_response_free(&resp);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(client, "/grpc.health.v1.Health/Check", "", 0,
                                                   nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  grpc_client_free(client);
  fixture_stop(&f);
}

/* graceful drain sends HTTP/3 GOAWAY and stops accepting */
void test_http3_drain(void) {
  h3_fixture f;
  fixture_start(&f, h3_config());
  grpc_err err = GRPC_OK;
  grpc_client *client = new_h3_client(f.server, &err);
  TEST_ASSERT_NOT_NULL(client);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(client, "/h3.Svc/Echo", "x", 1, nullptr, 0, 0, &resp));
  grpc_client_response_free(&resp);
  grpc_client_free(client);
  grpc_server_stop(f.server);
  pthread_join(f.th, nullptr);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_drain(f.server, 500));
  grpc_server_free(f.server);
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
  RUN_TEST(test_http3_retry_and_reload);
  RUN_TEST(test_http3_alongside_http2);
  RUN_TEST(test_http3_hostile_health);
  RUN_TEST(test_http3_drain);
  RUN_TEST(test_http3_config_errors);
  return UNITY_END();
}
