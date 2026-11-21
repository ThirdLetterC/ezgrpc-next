#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <signal.h>
#endif
#include "internal/grpc_threads.h"
#include "grpc.h"
#include "grpc_list.h"
#include "unity.h"

/* Deterministic countdown allocator for OOM fault-injection testing. */
static atomic_int g_fail_after = -1;

static bool should_fail(void) {
  int cur = atomic_load(&g_fail_after);
  if (cur < 0)
    return false;
  if (cur == 0)
    return true;
  atomic_fetch_sub(&g_fail_after, 1);
  return false;
}

static void *oom_malloc(size_t sz) {
  return should_fail() ? nullptr : malloc(sz);
}

static void *oom_calloc(size_t n, size_t sz) {
  return should_fail() ? nullptr : calloc(n, sz);
}

static void *oom_realloc(void *p, size_t sz) {
  return should_fail() ? nullptr : realloc(p, sz);
}

static void oom_free(void *p) {
  free(p);
}

static void *oom_aligned_alloc(size_t align, size_t sz) {
  return should_fail() ? nullptr : aligned_alloc(align, sz);
}

static void oom_enable(int fail_after) {
  atomic_store(&g_fail_after, fail_after);
  grpc_allocator a = {
      .malloc = oom_malloc,
      .calloc = oom_calloc,
      .realloc = oom_realloc,
      .free = oom_free,
      .aligned_alloc = oom_aligned_alloc,
  };
  grpc_set_allocator(&a);
}

static void oom_disable(void) {
  atomic_store(&g_fail_after, -1);
  grpc_set_allocator(nullptr);
}

void setUp(void) { oom_disable(); }
void tearDown(void) { oom_disable(); }

static void dummy_handler([[maybe_unused]] grpc_call *call,
                          [[maybe_unused]] const grpc_message *req,
                          [[maybe_unused]] void *ud) {}

static void dummy_post([[maybe_unused]] void *arg) {}

void test_oom_primitives(void) {
  /* Messages, strdup, metadata base64, list */
  for (int n = 0; n < 4; n++) {
    oom_enable(n);
    grpc_message *m = grpc_message_new("hello", 5);
    grpc_message_free(m);
    m = grpc_message_alloc(16);
    grpc_message_free(m);
    char *s = grpc_strdup("abc");
    grpc_free(s);
    s = grpc_strndup("abcdef", 3);
    grpc_free(s);
    size_t out_len = 0;
    char *b64 = grpc_metadata_encode_bin((const uint8_t *)"hello", 5, &out_len);
    grpc_free(b64);
    uint8_t *raw = grpc_metadata_decode_bin("aGVsbG8=", 8, &out_len);
    grpc_free(raw);
    grpc_list *l = grpc_list_new();
    if (l != nullptr) {
      (void)grpc_list_push_back(l, (void *)"a");
      (void)grpc_list_push_front(l, (void *)"b");
      grpc_list_free(l);
    }
    oom_disable();
  }
}

void test_oom_compression_and_status_details(void) {
  const uint8_t data[] = "compressible payload payload payload payload payload";
  uint8_t *gz = nullptr;
  size_t gz_len = 0;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_compress_gzip(data, sizeof(data), &gz, &gz_len));

  const uint8_t raw_det[] = {0x01, 0x02};
  grpc_status_detail_entry det = {
      .type_url = "type.googleapis.com/google.rpc.ErrorInfo",
      .value = raw_det,
      .value_len = sizeof(raw_det),
  };
  size_t b64_len = 0;
  char *b64 = grpc_status_details_encode(3, "bad", &det, 1, &b64_len);
  TEST_ASSERT_NOT_NULL(b64);

  for (int n = 0; n < 10; n++) {
    oom_enable(n);
    uint8_t *out = nullptr;
    size_t out_len = 0;
    grpc_err rc = grpc_compress_gzip(data, sizeof(data), &out, &out_len);
    (void)rc;
    grpc_free(out);
    rc = grpc_decompress_gzip(gz, gz_len, &out, &out_len);
    (void)rc;
    grpc_free(out);
#if defined(GRPC_ENABLE_ZSTD)
    rc = grpc_compress(GRPC_COMPRESS_ZSTD, -1, data, sizeof(data), &out, &out_len);
    (void)rc;
    grpc_free(out);
#endif
#if defined(GRPC_ENABLE_SNAPPY)
    rc = grpc_compress(GRPC_COMPRESS_SNAPPY, -1, data, sizeof(data), &out, &out_len);
    (void)rc;
    grpc_free(out);
#endif
    char *enc = grpc_status_details_encode(3, "bad", &det, 1, &out_len);
    grpc_free(enc);
    grpc_status_details all = {0};
    if (grpc_status_details_decode_all(b64, &all) == GRPC_OK)
      grpc_status_details_free(&all);
    oom_disable();
  }

  grpc_free(b64);
  grpc_free(gz);
}

void test_oom_server_and_services(void) {
  /* Fail at every allocation step during server creation */
  for (int n = 0; n < 12; n++) {
    oom_enable(n);
    grpc_err err = GRPC_OK;
    grpc_server *s = grpc_server_new_ex(
        &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF}, &err);
    grpc_server_free(s);
    oom_disable();
  }

  /* Create a real server, then fault-inject routes, interceptors, health, reflection, post */
  grpc_server *s = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(s);

  for (int n = 0; n < 5; n++) {
    oom_enable(n);
    char path[32];
    snprintf(path, sizeof(path), "/oom.Svc/M%d", n);
    (void)grpc_server_handle(s, path, dummy_handler, nullptr);
    (void)grpc_server_set_fallback(s, &(grpc_stream_handler){.on_message = (void *)dummy_handler},
                                   nullptr);
    (void)grpc_server_add_interceptor(s, &(grpc_interceptor){.name = "x", .path_prefix = "/oom/"});
    (void)grpc_server_post(s, dummy_post, nullptr);
    (void)grpc_health_enable(s);
    (void)grpc_health_set_status(s, "oom.Svc", GRPC_HEALTH_SERVING);
    (void)grpc_reflection_enable(s);
    const uint8_t fdp[] = {0x0A, 0x01, 'a'};
    (void)grpc_reflection_add_file_descriptor(s, "oom.proto", fdp, sizeof(fdp));
    (void)grpc_channelz_enable(s);
    oom_disable();
  }

  grpc_server_free(s);
}

void test_oom_client(void) {
  for (int n = 0; n < 6; n++) {
    oom_enable(n);
    grpc_err err = GRPC_OK;
    const char *targets[] = {"127.0.0.1:1"};
    grpc_client *c =
        grpc_client_new(&(grpc_client_config){.targets = targets, .nb_targets = 1}, &err);
    grpc_client_free(c);
    oom_disable();
  }
}

static void *oom_srv_run(void *arg) {
  (void)grpc_server_run((grpc_server *)arg);
  return nullptr;
}

static void oom_call_handler(grpc_call *call, const grpc_message *req,
                             [[maybe_unused]] void *ud) {
  grpc_metadata md[] = {{.key = "x-k", .value = "v"}};
  const uint8_t raw[] = {0x01};
  grpc_status_detail_entry det = {
      .type_url = "type.googleapis.com/google.rpc.ErrorInfo",
      .value = raw,
      .value_len = sizeof(raw),
  };
  grpc_metadata bad_null[] = {{.key = nullptr, .value = "v"}};
  grpc_metadata bad_empty[] = {{.key = "", .value = "v"}};
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_send_metadata(call, bad_null, 1));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_send_metadata(call, bad_empty, 1));
  for (int n = 0; n < 5; n++) {
    oom_enable(n);
    (void)grpc_call_send_metadata(call, md, 1);
    (void)grpc_call_add_trailers(call, md, 1);
    (void)grpc_call_send_metadata_async(call, md, 1);
    (void)grpc_call_add_trailers_async(call, md, 1);
    (void)grpc_call_send_data_async(call, "a", 1);
    (void)grpc_call_finish_async(call, GRPC_STATUS_OK, "m");
    (void)grpc_call_finish_with_details_async(call, GRPC_STATUS_INTERNAL, "e", &det, 1);
    oom_disable();
    grpc_message *m1 = grpc_message_new("a", 1);
    grpc_message *m2 = grpc_message_new("a", 1);
    oom_enable(0);
    (void)grpc_call_send_async(call, m1);
    (void)grpc_call_reply_async(call, m2);
    oom_disable();
  }
  (void)grpc_call_reply_data(call, req->data, req->len);
}

void test_oom_live_call_and_client(void) {
  grpc_server *srv = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(srv);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(srv, "/oom.Svc/Call", oom_call_handler, nullptr));
  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, oom_srv_run, srv));

  grpc_err err = GRPC_OK;
  grpc_client *c = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(srv), .timeout_ms = 2000},
      &err);
  TEST_ASSERT_NOT_NULL(c);

  /* Run handler that exercises OOM paths on live grpc_call */
  grpc_client_response resp = {0};
  (void)grpc_client_unary(c, "/oom.Svc/Call", "hi", 2, nullptr, 0, 0, &resp);
  grpc_client_response_free(&resp);

  /* Fault-inject client stream_open / send / unary allocations & unix_path init */
  for (int n = 0; n < 4; n++) {
    oom_enable(n);
    grpc_client *uc = grpc_client_new(&(grpc_client_config){.unix_path = "/tmp/oom_u.sock"}, &err);
    grpc_client_free(uc);
    oom_disable();
  }
  for (int n = 0; n < 6; n++) {
    oom_enable(n);
    grpc_client_stream *st = grpc_client_stream_open(c, "/oom.Svc/Call", nullptr, 0, 1000, &err);
    if (st != nullptr) {
      (void)grpc_client_stream_send(st, "x", 1);
      grpc_client_stream_cancel(st);
    }
    oom_disable();
  }

  grpc_client_free(c);
  grpc_server_stop(srv);
  pthread_join(th, nullptr);
  grpc_server_free(srv);
}

int main(void) {
#ifndef _WIN32
  signal(SIGPIPE, SIG_IGN);
#endif
  UNITY_BEGIN();
  RUN_TEST(test_oom_primitives);
  RUN_TEST(test_oom_compression_and_status_details);
  RUN_TEST(test_oom_server_and_services);
  RUN_TEST(test_oom_client);
  RUN_TEST(test_oom_live_call_and_client);
  return UNITY_END();
}
