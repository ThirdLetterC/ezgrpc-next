#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <nghttp2/nghttp2.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>
#endif
#include "internal/grpc_threads.h"
#include "grpc.h"
#include "grpc_list.h"
#include "unity.h"

/* Deterministic countdown allocator for OOM fault-injection testing. It is installed once in
 * main(), before any server thread exists, and stays installed: grpc_set_allocator() is not
 * thread-safe, so tests switch failures on and off only through the atomic countdown
 * (-1 = never fail, 0 = fail every allocation, n = fail after n more successes). */
static atomic_int g_fail_after = -1;

static bool should_fail(void) {
  int cur = atomic_load(&g_fail_after);
  for (;;) {
    if (cur < 0)
      return false;
    if (cur == 0)
      return true;
    /* claim one successful allocation; retry if another thread took it first */
    if (atomic_compare_exchange_weak(&g_fail_after, &cur, cur - 1))
      return false;
  }
}

static grpc_allocator g_base_alloc;

static void *oom_malloc(size_t sz) {
  return should_fail() ? nullptr : g_base_alloc.malloc(sz);
}

static void *oom_calloc(size_t n, size_t sz) {
  return should_fail() ? nullptr : g_base_alloc.calloc(n, sz);
}

static void *oom_realloc(void *p, size_t sz) {
  return should_fail() ? nullptr : g_base_alloc.realloc(p, sz);
}

static void oom_free(void *p) {
  g_base_alloc.free(p);
}

static void *oom_aligned_alloc(size_t align, size_t sz) {
  return should_fail() ? nullptr : g_base_alloc.aligned_alloc(align, sz);
}

/* Installs the countdown allocator on top of the default one; called once from main(). */
static void oom_install(void) {
  grpc_set_allocator(nullptr);
  g_base_alloc = *grpc_get_allocator();
  grpc_set_allocator(&(grpc_allocator){
      .malloc = oom_malloc,
      .calloc = oom_calloc,
      .realloc = oom_realloc,
      .free = oom_free,
      .aligned_alloc = oom_aligned_alloc,
  });
}

static void oom_enable(int fail_after) {
  atomic_store(&g_fail_after, fail_after);
}

static void oom_disable(void) {
  atomic_store(&g_fail_after, -1);
}

void setUp(void) {
  oom_disable();
}
void tearDown(void) {
  oom_disable();
}

static void dummy_handler([[maybe_unused]] grpc_call *call,
                          [[maybe_unused]] const grpc_message *req, [[maybe_unused]] void *ud) {}

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

static void oom_call_handler(grpc_call *call, const grpc_message *req, [[maybe_unused]] void *ud) {
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

static void oom_echo_handler(grpc_call *call, const grpc_message *req, [[maybe_unused]] void *ud) {
  grpc_metadata md[] = {{.key = "x-r", .value = "1"}};
  (void)grpc_call_send_metadata(call, md, 1);
  (void)grpc_call_reply_data(call, req->data, req->len);
}

static void oom_gzip_handler(grpc_call *call, const grpc_message *req, [[maybe_unused]] void *ud) {
  (void)grpc_call_set_compression(call, GRPC_COMPRESS_GZIP, GRPC_COMPRESSION_LEVEL_DEFAULT);
  for (int n = 0; n < 4; n++) {
    oom_enable(n);
    grpc_err rc = grpc_call_reply_data(call, req->data, req->len);
    oom_disable();
    if (rc == GRPC_OK)
      return;
  }
  (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

void test_oom_live_call_and_client(void) {
  grpc_server *srv = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(srv);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv, "/oom.Svc/Call", oom_call_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv, "/oom.Svc/Echo", oom_echo_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv, "/oom.Svc/Gzip", oom_gzip_handler, nullptr));
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
  (void)grpc_client_unary(c, "/oom.Svc/Gzip", "hello-compress-oom", 18, nullptr, 0, 0, &resp);
  grpc_client_response_free(&resp);

  /* Fault-inject client unary response handling (headers, data, message queue) */
  char med_req[512];
  memset(med_req, 'R', sizeof(med_req));
  for (int n = 0; n < 28; n++) {
    oom_enable(n);
    grpc_client_response r = {0};
    (void)grpc_client_unary(c, "/oom.Svc/Echo", med_req, sizeof(med_req), nullptr, 0, 1000, &r);
    grpc_client_response_free(&r);
    oom_disable();
  }

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

#ifndef _WIN32
static atomic_int g_web_oom_step = 0;

static void oom_web_handler(grpc_call *call, const grpc_message *req, [[maybe_unused]] void *ud) {
  int step = atomic_fetch_add(&g_web_oom_step, 1);
  grpc_metadata tr[] = {{.key = "x-t", .value = "val"}};
  (void)grpc_call_add_trailers(call, tr, 1);
  oom_enable(step);
  grpc_err rc = grpc_call_reply_data(call, req->data, req->len);
  oom_disable();
  if (rc != GRPC_OK)
    (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

typedef struct oom_h2_buf {
  const uint8_t *data;
  size_t len;
  size_t off;
} oom_h2_buf;

static ssize_t oom_h2_read_cb([[maybe_unused]] nghttp2_session *s, [[maybe_unused]] int32_t sid,
                              uint8_t *buf, size_t len, uint32_t *flags, nghttp2_data_source *src,
                              [[maybe_unused]] void *ud) {
  oom_h2_buf *b = src->ptr;
  size_t rem = b->len - b->off;
  size_t n = rem < len ? rem : len;
  if (n > 0) {
    memcpy(buf, b->data + b->off, n);
    b->off += n;
  }
  if (b->off == b->len)
    *flags = NGHTTP2_DATA_FLAG_EOF;
  return (ssize_t)n;
}

static void oom_send_all(int fd, const uint8_t *p, size_t len) {
  size_t off = 0;
  while (off < len) {
    ssize_t n = send(fd, p + off, len - off, 0);
    if (n <= 0)
      break;
    off += (size_t)n;
  }
}

static uint8_t g_oom_resp_frame[520];
static oom_h2_buf g_oom_resp_src;

static int oom_raw_srv_on_frame(nghttp2_session *s, const nghttp2_frame *f,
                                [[maybe_unused]] void *ud) {
  if (f->hd.stream_id > 0 && (f->hd.flags & NGHTTP2_FLAG_END_STREAM)) {
    g_oom_resp_src =
        (oom_h2_buf){.data = g_oom_resp_frame, .len = sizeof(g_oom_resp_frame), .off = 0};
    nghttp2_data_provider dp = {.source.ptr = &g_oom_resp_src, .read_callback = oom_h2_read_cb};
    nghttp2_nv hdrs[] = {
        {(uint8_t *)":status", (uint8_t *)"200", 7, 3, NGHTTP2_NV_FLAG_NONE},
        {(uint8_t *)"content-type", (uint8_t *)"application/grpc", 12, 16, NGHTTP2_NV_FLAG_NONE},
        {(uint8_t *)"x-h1", (uint8_t *)"val1", 4, 4, NGHTTP2_NV_FLAG_NONE},
        {(uint8_t *)"x-h2", (uint8_t *)"val2", 4, 4, NGHTTP2_NV_FLAG_NONE},
        {(uint8_t *)"grpc-status", (uint8_t *)"0", 11, 1, NGHTTP2_NV_FLAG_NONE},
    };
    nghttp2_submit_response(s, f->hd.stream_id, hdrs, 5, &dp);
  }
  return 0;
}

static atomic_int g_oom_raw_stop = 0;

static void *oom_raw_h2_server(void *arg) {
  int lfd = *(int *)arg;
  struct timeval tv = {.tv_sec = 0, .tv_usec = 100000};
  setsockopt(lfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  while (!atomic_load(&g_oom_raw_stop)) {
    int cfd = accept(lfd, nullptr, nullptr);
    if (cfd < 0)
      continue;
    setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    nghttp2_session_callbacks *cbs = nullptr;
    nghttp2_session_callbacks_new(&cbs);
    nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, oom_raw_srv_on_frame);
    nghttp2_session *sess = nullptr;
    nghttp2_session_server_new(&sess, cbs, nullptr);
    nghttp2_session_callbacks_del(cbs);
    nghttp2_submit_settings(sess, NGHTTP2_FLAG_NONE, nullptr, 0);
    for (;;) {
      const uint8_t *out;
      ssize_t wn = nghttp2_session_mem_send(sess, &out);
      if (wn <= 0)
        break;
      oom_send_all(cfd, out, (size_t)wn);
    }
    uint8_t rbuf[4096];
    while (!atomic_load(&g_oom_raw_stop)) {
      ssize_t n = recv(cfd, rbuf, sizeof(rbuf), 0);
      if (n == 0)
        break;
      if (n < 0)
        continue;
      if (nghttp2_session_mem_recv(sess, rbuf, (size_t)n) < 0)
        break;
      for (;;) {
        const uint8_t *out;
        ssize_t wn = nghttp2_session_mem_send(sess, &out);
        if (wn <= 0)
          break;
        oom_send_all(cfd, out, (size_t)wn);
      }
    }
    nghttp2_session_del(sess);
    close(cfd);
  }
  return nullptr;
}

void test_oom_grpc_web_and_client_deterministic(void) {
  /* 1. Server route registration OOM */
  grpc_server *srv = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(srv);
  for (int n = 0; n < 4; n++) {
    oom_enable(n);
    (void)grpc_server_handle(srv, "/oom/Route", oom_web_handler, nullptr);
    oom_disable();
  }
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv, "/oom.Web/Call", oom_web_handler, nullptr));
  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, oom_srv_run, srv));

  /* 2. gRPC-Web-Text and gRPC-Web binary OOM inside grpc_call_reply_data / call_finish_internal */
  const uint8_t wframe[] = {0x00, 0x00, 0x00, 0x00, 0x02, 'o', 'k'};
  size_t wb64_len = 0;
  char *wb64 = grpc_metadata_encode_bin(wframe, sizeof(wframe), &wb64_len);
  const char *cts[] = {"application/grpc-web-text", "application/grpc-web"};
  for (int mode = 0; mode < 2; mode++) {
    atomic_store(&g_web_oom_step, 0);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd >= 0) {
      struct sockaddr_in sin = {0};
      sin.sin_family = AF_INET;
      sin.sin_port = htons(grpc_server_port(srv));
      inet_pton(AF_INET, "127.0.0.1", &sin.sin_addr);
      if (connect(fd, (struct sockaddr *)&sin, sizeof(sin)) == 0) {
        nghttp2_session_callbacks *cbs = nullptr;
        nghttp2_session_callbacks_new(&cbs);
        nghttp2_session *sess = nullptr;
        nghttp2_session_client_new(&sess, cbs, nullptr);
        nghttp2_session_callbacks_del(cbs);
        nghttp2_submit_settings(sess, NGHTTP2_FLAG_NONE, nullptr, 0);
        oom_h2_buf bufs[12];
        nghttp2_data_provider dps[12];
        nghttp2_nv hdrs[] = {
            {(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE},
            {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
            {(uint8_t *)":path", (uint8_t *)"/oom.Web/Call", 5, 13, NGHTTP2_NV_FLAG_NONE},
            {(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9, NGHTTP2_NV_FLAG_NONE},
            {(uint8_t *)"content-type", (uint8_t *)cts[mode], 12, strlen(cts[mode]),
             NGHTTP2_NV_FLAG_NONE},
        };
        for (int i = 0; i < 12; i++) {
          bufs[i] = (oom_h2_buf){
              .data = mode == 0 ? (const uint8_t *)wb64 : wframe,
              .len = mode == 0 ? wb64_len : sizeof(wframe),
              .off = 0,
          };
          dps[i] = (nghttp2_data_provider){.source.ptr = &bufs[i], .read_callback = oom_h2_read_cb};
          nghttp2_submit_request(sess, nullptr, hdrs, 5, &dps[i], nullptr);
        }
        for (;;) {
          const uint8_t *out;
          ssize_t wn = nghttp2_session_mem_send(sess, &out);
          if (wn <= 0)
            break;
          oom_send_all(fd, out, (size_t)wn);
        }
        struct timeval tv = {.tv_sec = 0, .tv_usec = 80000};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        uint8_t rbuf[4096];
        for (int r = 0; r < 10; r++) {
          ssize_t n = recv(fd, rbuf, sizeof(rbuf), 0);
          if (n <= 0)
            break;
          nghttp2_session_mem_recv(sess, rbuf, (size_t)n);
        }
        nghttp2_session_del(sess);
      }
      close(fd);
    }
  }
  grpc_free(wb64);
  grpc_server_stop(srv);
  pthread_join(th, nullptr);
  grpc_server_free(srv);

  /* 3. Deterministic client-only OOM against non-grpc_malloc raw HTTP/2 server */
  g_oom_resp_frame[0] = 0;
  uint32_t rlen = htonl(sizeof(g_oom_resp_frame) - 5);
  memcpy(g_oom_resp_frame + 1, &rlen, 4);
  memset(g_oom_resp_frame + 5, 'Z', sizeof(g_oom_resp_frame) - 5);

  int lfd = socket(AF_INET, SOCK_STREAM, 0);
  TEST_ASSERT_TRUE(lfd >= 0);
  int opt = 1;
  setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  struct sockaddr_in lsin = {0};
  lsin.sin_family = AF_INET;
  lsin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  lsin.sin_port = 0;
  TEST_ASSERT_EQUAL_INT(0, bind(lfd, (struct sockaddr *)&lsin, sizeof(lsin)));
  TEST_ASSERT_EQUAL_INT(0, listen(lfd, 8));
  socklen_t slen = sizeof(lsin);
  TEST_ASSERT_EQUAL_INT(0, getsockname(lfd, (struct sockaddr *)&lsin, &slen));
  uint16_t lport = ntohs(lsin.sin_port);

  atomic_store(&g_oom_raw_stop, 0);
  pthread_t rth;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&rth, nullptr, oom_raw_h2_server, &lfd));

  for (int n = 0; n < 24; n++) {
    grpc_err err = GRPC_OK;
    grpc_client *c = grpc_client_new(
        &(grpc_client_config){.host = "127.0.0.1", .port = lport, .timeout_ms = 500}, &err);
    if (c == nullptr)
      continue;
    grpc_client_response resp = {0};
    oom_enable(n);
    (void)grpc_client_unary(c, "/raw.Svc/Echo", "hi", 2, nullptr, 0, 500, &resp);
    oom_disable();
    grpc_client_response_free(&resp);
    grpc_client_free(c);
  }

  atomic_store(&g_oom_raw_stop, 1);
  pthread_join(rth, nullptr);
  close(lfd);
}
#endif

int main(void) {
#ifndef _WIN32
  signal(SIGPIPE, SIG_IGN);
#endif
  oom_install();
  UNITY_BEGIN();
  RUN_TEST(test_oom_primitives);
  RUN_TEST(test_oom_compression_and_status_details);
  RUN_TEST(test_oom_server_and_services);
  RUN_TEST(test_oom_client);
  RUN_TEST(test_oom_live_call_and_client);
#ifndef _WIN32
  RUN_TEST(test_oom_grpc_web_and_client_deterministic);
#endif
  return UNITY_END();
}
