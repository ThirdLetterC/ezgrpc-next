#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "internal/grpc_threads.h"
#include "grpc.h"
#include "test.h"

static struct {
  grpc_server *server;
  pthread_t thread;
  bool running;
  grpc_client *client;
} fx;

void setUp(void) {
  memset(&fx, 0, sizeof(fx));
}

void tearDown(void) {
  grpc_client_free(fx.client);
  if (fx.running) {
    grpc_server_stop(fx.server);
    pthread_join(fx.thread, nullptr);
  }
  grpc_server_free(fx.server);
  memset(&fx, 0, sizeof(fx));
}

static void *run_server(void *arg) {
  (void)grpc_server_run(arg);
  return nullptr;
}

static void noop_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                         [[maybe_unused]] void *ud) {
  (void)grpc_call_reply_data(call, "", 0);
}

/* Sends one ServerReflectionRequest of `len` bytes and returns the response message. */
static grpc_message *reflect(const uint8_t *req, size_t len) {
  grpc_err err = GRPC_OK;
  grpc_client_stream *st =
      grpc_client_stream_open(fx.client, GRPC_REFLECTION_V1_PATH, nullptr, 0, 3000, &err);
  TEST_ASSERT_NOT_NULL(st);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(st, req, len));
  grpc_message *msg = nullptr;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(st, &msg));
  grpc_client_response resp = {0};
  (void)grpc_client_stream_close_send(st);
  (void)grpc_client_stream_finish(st, &resp);
  grpc_client_response_free(&resp);
  TEST_ASSERT_NOT_NULL(msg);
  return msg;
}

/* Reads a varint at *off. */
static uint64_t varint_at(const uint8_t *p, size_t len, size_t *off) {
  uint64_t v = 0;
  for (unsigned shift = 0; *off < len && shift < 64; shift += 7) {
    uint8_t b = p[(*off)++];
    v |= (uint64_t)(b & 0x7F) << shift;
    if (!(b & 0x80))
      break;
  }
  return v;
}

/* The response field after the echoed original_request (field 2). */
static size_t skip_original_request(const grpc_message *m) {
  size_t off = 0;
  TEST_ASSERT_EQUAL_HEX8(0x12, m->data[off++]);
  size_t n = (size_t)varint_at(m->data, m->len, &off);
  return off + n;
}

void test_reflection(void) {
  grpc_server *server = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(server);

  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_reflection_enable(server));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_EXISTS, grpc_reflection_enable(server));

  /* FileDescriptorProto{name: "helloworld.proto"}: field 1, length 16 */
  const uint8_t descriptor[] = {0x0a, 0x10, 'h', 'e', 'l', 'l', 'o', 'w', 'o',
                                'r',  'l',  'd', '.', 'p', 'r', 'o', 't', 'o'};
  static_assert(sizeof(descriptor) == 2 + 16);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_reflection_add_file_descriptor(
                                     server, "helloworld.proto", descriptor, sizeof(descriptor)));
  /* each invalid argument on its own, the others valid */
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_reflection_add_file_descriptor(
                                              nullptr, "x", descriptor, sizeof(descriptor)));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_reflection_add_file_descriptor(
                                              server, nullptr, descriptor, sizeof(descriptor)));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID,
                        grpc_reflection_add_file_descriptor(server, "x", nullptr, 1));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID,
                        grpc_reflection_add_file_descriptor(server, "x", descriptor, 0));
  /* duplicates are accepted (the first one wins on lookup) */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_reflection_add_file_descriptor(
                                     server, "helloworld.proto", descriptor, sizeof(descriptor)));

  grpc_server_free(server);
}

/* Long symbols are matched at full length (they used to be cut at 127 bytes, so two keys sharing
 * that prefix collided), and list_services names every service, however many. */
void test_reflection_long_symbols_and_many_services(void) {
  fx.server = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(fx.server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_reflection_enable(fx.server));
  for (int i = 0; i < 70; i++) {
    char path[32];
    snprintf(path, sizeof(path), "/svc.S%02d/M", i);
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(fx.server, path, noop_handler, nullptr));
  }
  char long_key[141];
  memset(long_key, 'a', 140);
  long_key[140] = '\0';
  const uint8_t fdp[] = {0x0a, 0x02, 'l', 'k'};
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_reflection_add_file_descriptor(fx.server, long_key, fdp, sizeof(fdp)));
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&fx.thread, nullptr, run_server, fx.server));
  fx.running = true;
  grpc_err err = GRPC_OK;
  fx.client = grpc_client_new(&(grpc_client_config){.host = "127.0.0.1",
                                                    .port = grpc_server_port(fx.server),
                                                    .timeout_ms = 3000},
                              &err);
  TEST_ASSERT_NOT_NULL(fx.client);

  /* list_services: 70 services plus reflection v1 and v1alpha */
  const uint8_t list_req[] = {0x3A, 0x00};
  grpc_message *m = reflect(list_req, sizeof(list_req));
  size_t off = skip_original_request(m);
  TEST_ASSERT_EQUAL_HEX8(0x32, m->data[off++]);
  size_t end = off + (size_t)varint_at(m->data, m->len, &off);
  TEST_ASSERT_EQUAL_size_t(m->len, end);
  size_t services = 0;
  while (off < end) {
    TEST_ASSERT_EQUAL_HEX8(0x0A, m->data[off++]);
    off += (size_t)varint_at(m->data, m->len, &off);
    services++;
  }
  TEST_ASSERT_EQUAL_size_t(72, services);
  grpc_message_free(m);

  /* a symbol sharing only the first 127 bytes of the key is not found */
  uint8_t req[200];
  size_t qlen = 127 + 9;
  req[0] = 0x22; /* file_containing_symbol */
  req[1] = (uint8_t)(qlen & 0x7F) | 0x80;
  req[2] = (uint8_t)(qlen >> 7);
  memset(req + 3, 'a', 127);
  memcpy(req + 3 + 127, "Zzz.Other", 9);
  m = reflect(req, 3 + qlen);
  TEST_ASSERT_EQUAL_HEX8(0x3A, m->data[skip_original_request(m)]); /* error_response */
  grpc_message_free(m);
  /* the full key, and a name inside it, are */
  qlen = 140 + 7;
  req[1] = (uint8_t)(qlen & 0x7F) | 0x80;
  req[2] = (uint8_t)(qlen >> 7);
  memset(req + 3, 'a', 140);
  memcpy(req + 3 + 140, ".Method", 7);
  m = reflect(req, 3 + qlen);
  TEST_ASSERT_EQUAL_HEX8(0x22, m->data[skip_original_request(m)]); /* file_descriptor_response */
  grpc_message_free(m);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_reflection);
  RUN_TEST(test_reflection_long_symbols_and_many_services);
  return UNITY_END();
}
