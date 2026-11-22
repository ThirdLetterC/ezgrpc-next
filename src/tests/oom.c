#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <nghttp2/nghttp2.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>
#endif
#include "internal/grpc_threads.h"
#include "grpc.h"
#include "grpc_msg_queue.h"
#include "test.h"

/* Deterministic countdown allocator for OOM fault-injection testing. It is installed once in
 * main(), before any server thread exists, and stays installed: grpc_set_allocator() is not
 * thread-safe, so tests switch failures on and off only through atomics:
 *
 *   g_fail_after   -1 = never fail, 0 = fail every allocation, n = fail after n more successes
 *   g_target_tid   the only thread (oom_thread_id()) whose allocations count down and fail;
 *                  allocations of every other thread always succeed and do not consume the
 *                  countdown, so injection is deterministic even while another thread (the
 *                  server loop, or the client blocked in a call) allocates concurrently
 *   g_injected     number of allocations failed since the last oom_enable_on()
 *   g_one_shot     when set (oom_enable_once_on()), only that one allocation fails and the
 *                  countdown then disarms itself, like a transient allocation failure
 *
 * Every fault-injection loop runs with n = 0, 1, 2, ... until an iteration completes without an
 * injected failure, so each allocation point of the operation is failed exactly once with all
 * earlier ones succeeding. The final n is the number of allocation points covered. */
enum { OOM_MAX_STEPS = 2000 };

static atomic_int g_fail_after = -1;
static atomic_int g_target_tid = 0;
static atomic_int g_injected = 0;
static atomic_bool g_one_shot = false;
static atomic_int g_next_tid = 1;
static _Thread_local int t_oom_tid;

/* Small per-thread id (never 0), assigned on first use. */
static int oom_thread_id(void) {
  if (t_oom_tid == 0)
    t_oom_tid = atomic_fetch_add(&g_next_tid, 1);
  return t_oom_tid;
}

static bool should_fail(void) {
  int cur = atomic_load(&g_fail_after);
  if (cur < 0)
    return false;
  if (atomic_load(&g_target_tid) != oom_thread_id())
    return false;
  for (;;) {
    if (cur < 0)
      return false;
    if (cur == 0) {
      /* one-shot: only the thread that disarms the countdown fails */
      if (atomic_load(&g_one_shot) && !atomic_compare_exchange_strong(&g_fail_after, &cur, -1))
        continue;
      atomic_fetch_add(&g_injected, 1);
      return true;
    }
    /* claim one successful allocation; retry if the countdown changed meanwhile */
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

/* Arms the countdown for thread `tid` only: every allocation after the first fail_after fails. */
static void oom_enable_on(int tid, int fail_after) {
  atomic_store(&g_injected, 0);
  atomic_store(&g_one_shot, false);
  atomic_store(&g_target_tid, tid);
  atomic_store(&g_fail_after, fail_after);
}

/* Like oom_enable_on(), but only allocation number fail_after fails. */
static void oom_enable_once_on(int tid, int fail_after) {
  atomic_store(&g_injected, 0);
  atomic_store(&g_one_shot, true);
  atomic_store(&g_target_tid, tid);
  atomic_store(&g_fail_after, fail_after);
}

static void oom_enable_self(int fail_after) {
  oom_enable_on(oom_thread_id(), fail_after);
}

/* Disarms the countdown. Returns true if an allocation was failed since it was armed. */
static bool oom_disable(void) {
  atomic_store(&g_fail_after, -1);
  atomic_store(&g_target_tid, 0);
  return atomic_exchange(&g_injected, 0) != 0;
}

/* With OOM_VERBOSE set, prints how many allocation points a loop covered. */
static void oom_report(const char *loop, int points) {
  if (getenv("OOM_VERBOSE") != nullptr) {
    printf("[oom] %-34s %4d allocation points\n", loop, points);
    fflush(stdout);
  }
}

#define OOM_ASSERT_STEP(n) \
  TEST_ASSERT_LESS_THAN_INT_MESSAGE(OOM_MAX_STEPS, (n), "fault-injection loop did not converge")

#define ASSERT_ERR_ONE_OF2(rc, a, b)                                          \
  do {                                                                        \
    grpc_err rc_ = (rc);                                                      \
    if (rc_ != (a) && rc_ != (b)) {                                           \
      char m_[80];                                                            \
      snprintf(m_, sizeof(m_), "unexpected grpc_err %d (%s)", (int)rc_, #rc); \
      TEST_FAIL_MESSAGE(m_);                                                  \
    }                                                                         \
  } while (0)
#define ASSERT_OK_OR_NOMEM(rc) ASSERT_ERR_ONE_OF2(rc, GRPC_OK, GRPC_ERR_NOMEM)

/* Server threads must never run test assertions (a failure there aborts the process, see test.h).
 * Handlers record the first failed expectation here and the test thread checks it. */
static atomic_int g_srv_bad_line = 0;
#define SRV_EXPECT(cond)                                                       \
  do {                                                                         \
    if (!(cond)) {                                                             \
      int zero_ = 0;                                                           \
      (void)atomic_compare_exchange_strong(&g_srv_bad_line, &zero_, __LINE__); \
    }                                                                          \
  } while (0)

static void assert_server_expectations(void) {
  int line = atomic_load(&g_srv_bad_line);
  if (line != 0) {
    char msg[96];
    snprintf(msg, sizeof(msg), "server-side expectation failed at oom.c:%d", line);
    TEST_FAIL_MESSAGE(msg);
  }
}

/* ---- fixtures: torn down in tearDown() even when an assertion fails --------------------- */

static struct {
  grpc_server *srv;
  pthread_t srv_th;
  bool srv_running;
  grpc_client *client;
#ifndef _WIN32
  int raw_lfd;
  pthread_t raw_th;
  bool raw_running;
  int web_fd;
  nghttp2_session *web_sess;
#endif
} g_fx;
static atomic_int g_srv_rc = GRPC_OK;
#ifndef _WIN32
static atomic_int g_oom_raw_stop = 0;
#endif

static void *oom_srv_run(void *arg) {
  atomic_store(&g_srv_rc, grpc_server_run((grpc_server *)arg));
  return nullptr;
}

static grpc_server *fx_server_new(void) {
  g_fx.srv = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(g_fx.srv);
  return g_fx.srv;
}

static void fx_server_start(void) {
  atomic_store(&g_srv_rc, GRPC_OK);
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&g_fx.srv_th, nullptr, oom_srv_run, g_fx.srv));
  g_fx.srv_running = true;
}

static void fx_server_stop(void) {
  if (g_fx.srv_running) {
    grpc_server_stop(g_fx.srv);
    pthread_join(g_fx.srv_th, nullptr);
    g_fx.srv_running = false;
  }
  grpc_server_free(g_fx.srv);
  g_fx.srv = nullptr;
}

static grpc_client *fx_client_new(uint16_t port) {
  grpc_err err = GRPC_OK;
  g_fx.client = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = port, .timeout_ms = 2000}, &err);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
  TEST_ASSERT_NOT_NULL(g_fx.client);
  return g_fx.client;
}

static void fx_teardown(void) {
  grpc_client_free(g_fx.client);
  g_fx.client = nullptr;
  fx_server_stop();
#ifndef _WIN32
  if (g_fx.web_sess != nullptr)
    nghttp2_session_del(g_fx.web_sess);
  g_fx.web_sess = nullptr;
  if (g_fx.web_fd >= 0)
    close(g_fx.web_fd);
  g_fx.web_fd = -1;
  if (g_fx.raw_running) {
    atomic_store(&g_oom_raw_stop, 1);
    pthread_join(g_fx.raw_th, nullptr);
    g_fx.raw_running = false;
  }
  if (g_fx.raw_lfd >= 0)
    close(g_fx.raw_lfd);
  g_fx.raw_lfd = -1;
#endif
}

void setUp(void) {
  (void)oom_disable();
  atomic_store(&g_srv_bad_line, 0);
  memset(&g_fx, 0, sizeof(g_fx));
#ifndef _WIN32
  g_fx.raw_lfd = -1;
  g_fx.web_fd = -1;
#endif
}

void tearDown(void) {
  (void)oom_disable();
  fx_teardown();
}

static void dummy_handler([[maybe_unused]] grpc_call *call,
                          [[maybe_unused]] const grpc_message *req, [[maybe_unused]] void *ud) {}

static void dummy_post([[maybe_unused]] void *arg) {}

static bool response_is(const grpc_client_response *r, const void *data, size_t len) {
  return r->status == GRPC_STATUS_OK && r->message != nullptr && r->message->len == len &&
         memcmp(r->message->data, data, len) == 0;
}

/* ---- library primitives ----------------------------------------------------------------- */

void test_oom_primitives(void) {
  /* Messages, strdup, metadata base64, message queue */
  for (int n = 0;; n++) {
    OOM_ASSERT_STEP(n);
    grpc_message *qmsgs[6];
    for (size_t i = 0; i < 6; i++) {
      qmsgs[i] = grpc_message_new("q", 1);
      TEST_ASSERT_NOT_NULL(qmsgs[i]);
    }
    oom_enable_self(n);
    grpc_message *m = grpc_message_new("hello", 5);
    grpc_message *a = grpc_message_alloc(16);
    char *s = grpc_strdup("abc");
    char *sn = grpc_strndup("abcdef", 3);
    size_t b64_len = 0;
    char *b64 = grpc_metadata_encode_bin((const uint8_t *)"hello", 5, &b64_len);
    size_t raw_len = 0;
    uint8_t *raw = grpc_metadata_decode_bin("aGVsbG8=", 8, &raw_len);
    /* six pushes: the first allocates the 4-slot buffer and the fifth doubles it */
    grpc_msg_queue q = {0};
    grpc_err pushed[6];
    for (size_t i = 0; i < 6; i++)
      pushed[i] = msg_queue_push(&q, qmsgs[i]);
    bool injected = oom_disable();

    /* every result is either complete and correct, or the documented nullptr / nonzero */
    if (m != nullptr) {
      TEST_ASSERT_EQUAL_UINT32(5, m->len);
      TEST_ASSERT_EQUAL_MEMORY("hello", m->data, 5);
    }
    if (a != nullptr)
      TEST_ASSERT_EQUAL_UINT32(16, a->len);
    if (s != nullptr)
      TEST_ASSERT_EQUAL_STRING("abc", s);
    if (sn != nullptr)
      TEST_ASSERT_EQUAL_STRING("abc", sn);
    if (b64 != nullptr) {
      TEST_ASSERT_EQUAL_size_t(8, b64_len);
      TEST_ASSERT_EQUAL_STRING("aGVsbG8=", b64);
    }
    if (raw != nullptr) {
      TEST_ASSERT_EQUAL_size_t(5, raw_len);
      TEST_ASSERT_EQUAL_MEMORY("hello", raw, 5);
    }
    /* a failed push leaves the message with the caller and the queue intact: the queue holds
     * exactly the messages whose push succeeded, in order */
    size_t expect = 0;
    for (size_t i = 0; i < 6; i++) {
      ASSERT_OK_OR_NOMEM(pushed[i]);
      if (pushed[i] == GRPC_OK)
        expect++;
    }
    TEST_ASSERT_EQUAL_size_t(expect, msg_queue_count(&q));
    for (size_t i = 0; i < 6; i++) {
      if (pushed[i] == GRPC_OK)
        TEST_ASSERT_EQUAL_PTR(qmsgs[i], msg_queue_pop(&q));
    }
    TEST_ASSERT_NULL(msg_queue_pop(&q));
    msg_queue_clear(&q);
    for (size_t i = 0; i < 6; i++) /* popped or never queued: the test owns them all again */
      grpc_message_free(qmsgs[i]);
    if (!injected) {
      TEST_ASSERT_TRUE(m != nullptr && a != nullptr && s != nullptr && sn != nullptr &&
                       b64 != nullptr && raw != nullptr);
      for (size_t i = 0; i < 6; i++)
        TEST_ASSERT_EQUAL_INT(GRPC_OK, pushed[i]);
    }
    grpc_message_free(m);
    grpc_message_free(a);
    grpc_free(s);
    grpc_free(sn);
    grpc_free(b64);
    grpc_free(raw);
    if (!injected) {
      oom_report("primitives", n);
      break;
    }
  }
}

void test_oom_compression_and_status_details(void) {
  const uint8_t data[] = "compressible payload payload payload payload payload";
  uint8_t *gz = nullptr;
  size_t gz_len = 0;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_compress_gzip(data, sizeof(data), &gz, &gz_len));

  const uint8_t raw_det[] = {0x01, 0x02};
  const char *type_url = "type.googleapis.com/google.rpc.ErrorInfo";
  grpc_status_detail_entry det = {
      .type_url = type_url,
      .value = raw_det,
      .value_len = sizeof(raw_det),
  };
  size_t b64_len = 0;
  char *b64 = grpc_status_details_encode(3, "bad", &det, 1, &b64_len);
  TEST_ASSERT_NOT_NULL(b64);

  for (int n = 0;; n++) {
    OOM_ASSERT_STEP(n);
    oom_enable_self(n);
    uint8_t *cz = nullptr, *dz = nullptr;
    size_t cz_len = 0, dz_len = 0;
    grpc_err rc_c = grpc_compress_gzip(data, sizeof(data), &cz, &cz_len);
    grpc_err rc_d = grpc_decompress_gzip(gz, gz_len, &dz, &dz_len);
#if defined(GRPC_ENABLE_ZSTD)
    uint8_t *zs = nullptr;
    size_t zs_len = 0;
    grpc_err rc_zs = grpc_compress(GRPC_COMPRESS_ZSTD, -1, data, sizeof(data), &zs, &zs_len);
#endif
#if defined(GRPC_ENABLE_SNAPPY)
    uint8_t *sn = nullptr;
    size_t sn_len = 0;
    grpc_err rc_sn = grpc_compress(GRPC_COMPRESS_SNAPPY, -1, data, sizeof(data), &sn, &sn_len);
#endif
    size_t enc_len = 0;
    char *enc = grpc_status_details_encode(3, "bad", &det, 1, &enc_len);
    grpc_status_details all = {0};
    grpc_err rc_all = grpc_status_details_decode_all(b64, &all);
    bool injected = oom_disable();

    ASSERT_OK_OR_NOMEM(rc_c);
    if (rc_c == GRPC_OK) {
      /* the compressed output must round-trip */
      uint8_t *back = nullptr;
      size_t back_len = 0;
      TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_decompress_gzip(cz, cz_len, &back, &back_len));
      TEST_ASSERT_EQUAL_size_t(sizeof(data), back_len);
      TEST_ASSERT_EQUAL_MEMORY(data, back, sizeof(data));
      grpc_free(back);
    }
    ASSERT_OK_OR_NOMEM(rc_d);
    if (rc_d == GRPC_OK) {
      TEST_ASSERT_EQUAL_size_t(sizeof(data), dz_len);
      TEST_ASSERT_EQUAL_MEMORY(data, dz, sizeof(data));
    }
#if defined(GRPC_ENABLE_ZSTD)
    ASSERT_OK_OR_NOMEM(rc_zs);
    TEST_ASSERT_TRUE(injected || rc_zs == GRPC_OK);
    grpc_free(zs);
#endif
#if defined(GRPC_ENABLE_SNAPPY)
    ASSERT_OK_OR_NOMEM(rc_sn);
    TEST_ASSERT_TRUE(injected || rc_sn == GRPC_OK);
    grpc_free(sn);
#endif
    if (enc != nullptr) {
      TEST_ASSERT_EQUAL_size_t(b64_len, enc_len);
      TEST_ASSERT_EQUAL_STRING(b64, enc);
    }
    ASSERT_ERR_ONE_OF2(rc_all, GRPC_OK, GRPC_ERR_NOMEM);
    if (rc_all == GRPC_OK) {
      TEST_ASSERT_EQUAL_INT32(3, all.code);
      TEST_ASSERT_EQUAL_STRING("bad", all.message);
      TEST_ASSERT_EQUAL_size_t(1, all.nb_details);
      TEST_ASSERT_EQUAL_STRING(type_url, all.details[0].type_url);
      TEST_ASSERT_EQUAL_size_t(sizeof(raw_det), all.details[0].value_len);
      TEST_ASSERT_EQUAL_MEMORY(raw_det, all.details[0].value, sizeof(raw_det));
    } else {
      /* on error the struct is freed and zeroed */
      TEST_ASSERT_NULL(all.message);
      TEST_ASSERT_NULL(all.details);
      TEST_ASSERT_EQUAL_size_t(0, all.nb_details);
    }
    if (!injected) {
      TEST_ASSERT_EQUAL_INT(GRPC_OK, rc_c);
      TEST_ASSERT_EQUAL_INT(GRPC_OK, rc_d);
      TEST_ASSERT_NOT_NULL(enc);
      TEST_ASSERT_EQUAL_INT(GRPC_OK, rc_all);
    }
    grpc_free(cz);
    grpc_free(dz);
    grpc_free(enc);
    grpc_status_details_free(&all);
    if (!injected) {
      oom_report("compression + status details", n);
      break;
    }
  }

  grpc_free(b64);
  grpc_free(gz);
}

/* ---- server construction and registration ---------------------------------------------- */

void test_oom_server_and_services(void) {
  /* Fail at every allocation step during server creation */
  for (int n = 0;; n++) {
    OOM_ASSERT_STEP(n);
    oom_enable_self(n);
    grpc_err err = GRPC_OK;
    grpc_server *s = grpc_server_new_ex(
        &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF}, &err);
    bool injected = oom_disable();
    if (s == nullptr)
      TEST_ASSERT_EQUAL_INT(GRPC_ERR_NOMEM, err);
    else
      TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
    TEST_ASSERT_TRUE(injected || s != nullptr);
    if (s != nullptr) /* a server created despite a failed allocation must still work */
      TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(s, "/oom.Svc/New", dummy_handler, nullptr));
    grpc_server_free(s);
    if (!injected) {
      oom_report("grpc_server_new_ex", n);
      break;
    }
  }

  /* Fault-inject routes, fallback, interceptors, post, health, reflection and channelz on a
   * fresh server per step, then check each API either succeeded or rolled back: repeating it
   * with allocations working must give GRPC_ERR_EXISTS (registered) or GRPC_OK (rolled back). */
  const uint8_t fdp[] = {0x0A, 0x01, 'a'};
  const grpc_stream_handler fallback = {.on_message = (void *)dummy_handler};
  const grpc_interceptor interceptor = {.name = "x", .path_prefix = "/oom/"};
  for (int n = 0;; n++) {
    OOM_ASSERT_STEP(n);
    grpc_server *s = fx_server_new();
    oom_enable_self(n);
    grpc_err rc_handle = grpc_server_handle(s, "/oom.Svc/M", dummy_handler, nullptr);
    grpc_err rc_fallback = grpc_server_set_fallback(s, &fallback, nullptr);
    grpc_err rc_icpt = grpc_server_add_interceptor(s, &interceptor);
    grpc_err rc_post = grpc_server_post(s, dummy_post, nullptr);
    grpc_err rc_health = grpc_health_enable(s);
    grpc_err rc_status = grpc_health_set_status(s, "oom.Svc", GRPC_HEALTH_SERVING);
    grpc_err rc_refl = grpc_reflection_enable(s);
    grpc_err rc_fd = grpc_reflection_add_file_descriptor(s, "oom.proto", fdp, sizeof(fdp));
    grpc_err rc_cz = grpc_channelz_enable(s);
    bool injected = oom_disable();

    ASSERT_OK_OR_NOMEM(rc_handle);
    ASSERT_OK_OR_NOMEM(rc_fallback);
    ASSERT_OK_OR_NOMEM(rc_icpt);
    ASSERT_OK_OR_NOMEM(rc_post);
    ASSERT_OK_OR_NOMEM(rc_health);
    ASSERT_OK_OR_NOMEM(rc_status);
    ASSERT_OK_OR_NOMEM(rc_refl);
    ASSERT_OK_OR_NOMEM(rc_fd);
    ASSERT_OK_OR_NOMEM(rc_cz);
    if (!injected) {
      grpc_err all[] = {rc_handle, rc_fallback, rc_icpt, rc_post, rc_health,
                        rc_status, rc_refl,     rc_fd,   rc_cz};
      for (size_t i = 0; i < sizeof(all) / sizeof(all[0]); i++)
        TEST_ASSERT_EQUAL_INT_MESSAGE(GRPC_OK, all[i], "operation failed without OOM");
    }

    /* the server is still usable, and failed registrations left nothing behind */
    grpc_err again_ok_or_exists[][2] = {
        {rc_handle, grpc_server_handle(s, "/oom.Svc/M", dummy_handler, nullptr)},
        {rc_health, grpc_health_enable(s)},
        {rc_cz, grpc_channelz_enable(s)},
    };
    for (size_t i = 0; i < sizeof(again_ok_or_exists) / sizeof(again_ok_or_exists[0]); i++) {
      grpc_err first = again_ok_or_exists[i][0], again = again_ok_or_exists[i][1];
      TEST_ASSERT_EQUAL_INT(first == GRPC_OK ? GRPC_ERR_EXISTS : GRPC_OK, again);
    }
    /* grpc_reflection_enable() is all or nothing: after a failure a retry registers cleanly */
    grpc_err refl_again = grpc_reflection_enable(s);
    TEST_ASSERT_EQUAL_INT(rc_refl == GRPC_OK ? GRPC_ERR_EXISTS : GRPC_OK, refl_again);
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_set_fallback(s, &fallback, nullptr));
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_add_interceptor(s, &interceptor));
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_post(s, dummy_post, nullptr));
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_set_status(s, "oom.Svc", GRPC_HEALTH_SERVING));
    TEST_ASSERT_EQUAL_INT(GRPC_OK,
                          grpc_reflection_add_file_descriptor(s, "oom2.proto", fdp, sizeof(fdp)));
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(s, "/oom.Svc/After", dummy_handler, nullptr));
    fx_server_stop();
    if (!injected) {
      oom_report("server registration + services", n);
      break;
    }
  }
}

/* ---- live server and client ------------------------------------------------------------ */

static void oom_echo_handler(grpc_call *call, const grpc_message *req, [[maybe_unused]] void *ud) {
  grpc_metadata md[] = {{.key = "x-r", .value = "1"}};
  SRV_EXPECT(grpc_call_send_metadata(call, md, 1) == GRPC_OK);
  SRV_EXPECT(grpc_call_reply_data(call, req->data, req->len) == GRPC_OK);
}

static void assert_echo_works(grpc_client *c) {
  grpc_client_response r = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(c, "/oom.Svc/Echo", "ping", 4, nullptr, 0, 2000, &r));
  bool ok = response_is(&r, "ping", 4);
  grpc_client_response_free(&r);
  TEST_ASSERT_TRUE_MESSAGE(ok, "client or server unusable after OOM");
}

void test_oom_client(void) {
  grpc_server *srv = fx_server_new();
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv, "/oom.Svc/Echo", oom_echo_handler, nullptr));
  fx_server_start();
  uint16_t port = grpc_server_port(srv);
  char target[32];
  snprintf(target, sizeof(target), "127.0.0.1:%u", port);
  const char *targets[] = {target};

  /* single target and target list against the live server: every injected failure yields
   * nullptr and an error; without OOM the client connects and works. Only the test thread's
   * allocations fail, the server thread is unaffected. */
  for (int mode = 0; mode < 2; mode++) {
    const grpc_client_config cfg =
        mode == 0 ? (grpc_client_config){.host = "127.0.0.1", .port = port, .timeout_ms = 2000}
                  : (grpc_client_config){.targets = targets, .nb_targets = 1, .timeout_ms = 2000};
    for (int n = 0;; n++) {
      OOM_ASSERT_STEP(n);
      oom_enable_self(n);
      grpc_err err = GRPC_OK;
      grpc_client *c = grpc_client_new(&cfg, &err);
      bool injected = oom_disable();
      if (c == nullptr) {
        TEST_ASSERT_TRUE(injected);
        if (err != GRPC_ERR_NOMEM) {
          char m[64];
          snprintf(m, sizeof(m), "grpc_client_new: unexpected grpc_err %d", (int)err);
          TEST_FAIL_MESSAGE(m);
        }
      } else {
        TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
        g_fx.client = c;
        /* an allocation failure while connecting fails grpc_client_new() (a lost connection
         * preface included), so a client that was created is always usable */
        assert_echo_works(c);
        grpc_client_free(c);
        g_fx.client = nullptr;
      }
      if (!injected) {
        oom_report(mode == 0 ? "grpc_client_new (host/port)" : "grpc_client_new (targets)", n);
        break;
      }
    }
  }

  /* unix_path init: the path does not exist, so without OOM the result is GRPC_ERR_IO */
  for (int n = 0;; n++) {
    OOM_ASSERT_STEP(n);
    oom_enable_self(n);
    grpc_err err = GRPC_OK;
    grpc_client *uc =
        grpc_client_new(&(grpc_client_config){.unix_path = "oom-no-such-dir/none.sock"}, &err);
    bool injected = oom_disable();
    TEST_ASSERT_NULL(uc);
    if (injected)
      ASSERT_ERR_ONE_OF2(err, GRPC_ERR_NOMEM, GRPC_ERR_IO);
    else
      TEST_ASSERT_EQUAL_INT(GRPC_ERR_IO, err);
    if (!injected) {
      oom_report("grpc_client_new (unix_path)", n);
      break;
    }
  }
  assert_server_expectations();
  fx_server_stop();
  TEST_ASSERT_EQUAL_INT(GRPC_OK, atomic_load(&g_srv_rc));
}

/* Results of the live-call handlers, reported to the test thread. */
static atomic_int g_call_points = -1;
static atomic_int g_step = 0;
static atomic_int g_step_injected = -1; /* -1 = handler did not run */

/* Runs on the server thread: fault-injects every grpc_call API with the countdown restricted
 * to this thread, then echoes the request. */
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
  SRV_EXPECT(grpc_call_send_metadata(call, bad_null, 1) == GRPC_ERR_INVALID);
  SRV_EXPECT(grpc_call_send_metadata(call, bad_empty, 1) == GRPC_ERR_INVALID);

  int me = oom_thread_id();
  for (int n = 0;; n++) {
    if (n >= OOM_MAX_STEPS) {
      SRV_EXPECT(n < OOM_MAX_STEPS);
      break;
    }
    oom_enable_on(me, n);
    grpc_err rc[] = {
        grpc_call_send_metadata(call, md, 1),
        grpc_call_add_trailers(call, md, 1),
        grpc_call_send_metadata_async(call, md, 1),
        grpc_call_add_trailers_async(call, md, 1),
        grpc_call_send_data_async(call, "a", 1),
        grpc_call_finish_async(call, GRPC_STATUS_OK, "m"),
        grpc_call_finish_with_details_async(call, GRPC_STATUS_INTERNAL, "e", &det, 1),
    };
    bool injected = oom_disable();
    for (size_t i = 0; i < sizeof(rc) / sizeof(rc[0]); i++) {
      SRV_EXPECT(rc[i] == GRPC_OK || rc[i] == GRPC_ERR_NOMEM);
      SRV_EXPECT(injected || rc[i] == GRPC_OK);
    }
    if (!injected) {
      atomic_store(&g_call_points, n);
      break;
    }
  }
  /* queueing a message fails cleanly (and frees it) when nothing can be allocated */
  grpc_message *m1 = grpc_message_new("a", 1);
  grpc_message *m2 = grpc_message_new("a", 1);
  SRV_EXPECT(m1 != nullptr && m2 != nullptr);
  oom_enable_on(me, 0);
  grpc_err rc_send = grpc_call_send_async(call, m1);
  grpc_err rc_reply = grpc_call_reply_async(call, m2);
  (void)oom_disable();
  SRV_EXPECT(rc_send == GRPC_ERR_NOMEM);
  SRV_EXPECT(rc_reply == GRPC_ERR_NOMEM);
  /* the call is still usable: answer synchronously before the queued operations run */
  SRV_EXPECT(grpc_call_reply_data(call, req->data, req->len) == GRPC_OK);
}

/* Runs on the server thread: replies with gzip compression, failing allocation g_step of the
 * reply; a failed reply finishes the call with RESOURCE_EXHAUSTED. */
static void oom_gzip_handler(grpc_call *call, const grpc_message *req, [[maybe_unused]] void *ud) {
  SRV_EXPECT(grpc_call_set_compression(call, GRPC_COMPRESS_GZIP, GRPC_COMPRESSION_LEVEL_DEFAULT) ==
             GRPC_OK);
  oom_enable_on(oom_thread_id(), atomic_load(&g_step));
  grpc_err rc = grpc_call_reply_data(call, req->data, req->len);
  bool injected = oom_disable();
  SRV_EXPECT(rc == GRPC_OK || rc == GRPC_ERR_NOMEM);
  SRV_EXPECT(injected || rc == GRPC_OK);
  if (rc != GRPC_OK) /* a failed reply leaves the call open and finishable */
    SRV_EXPECT(grpc_call_finish(call, GRPC_STATUS_RESOURCE_EXHAUSTED, "oom") == GRPC_OK);
  atomic_store(&g_step_injected, injected ? 1 : 0);
}

void test_oom_live_call_and_client(void) {
  grpc_server *srv = fx_server_new();
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv, "/oom.Svc/Call", oom_call_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv, "/oom.Svc/Echo", oom_echo_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv, "/oom.Svc/Gzip", oom_gzip_handler, nullptr));
  fx_server_start();
  grpc_client *c = fx_client_new(grpc_server_port(srv));

  /* 1. grpc_call APIs under OOM on the server thread; the call still answers */
  atomic_store(&g_call_points, -1);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(c, "/oom.Svc/Call", "hi", 2, nullptr, 0, 0, &resp));
  bool call_ok = response_is(&resp, "hi", 2);
  grpc_client_response_free(&resp);
  assert_server_expectations();
  TEST_ASSERT_TRUE_MESSAGE(call_ok, "/oom.Svc/Call did not echo");
  TEST_ASSERT_TRUE_MESSAGE(atomic_load(&g_call_points) >= 0, "call handler did not converge");
  oom_report("grpc_call APIs (server thread)", atomic_load(&g_call_points));

  /* 2. gzip reply under OOM on the server thread, one call per allocation point */
  const char gz_req[] = "hello-compress-oom hello-compress-oom hello-compress-oom";
  for (int n = 0;; n++) {
    OOM_ASSERT_STEP(n);
    atomic_store(&g_step, n);
    atomic_store(&g_step_injected, -1);
    grpc_client_response r = {0};
    grpc_err rc = grpc_client_unary(c, "/oom.Svc/Gzip", gz_req, sizeof(gz_req), nullptr, 0, 0, &r);
    int injected = atomic_load(&g_step_injected);
    bool echoed = response_is(&r, gz_req, sizeof(gz_req));
    grpc_status st = r.status;
    grpc_client_response_free(&r);
    assert_server_expectations();
    TEST_ASSERT_EQUAL_INT(GRPC_OK, rc);
    TEST_ASSERT_NOT_EQUAL_INT_MESSAGE(-1, injected, "gzip handler did not run");
    if (!echoed)
      TEST_ASSERT_EQUAL_INT(GRPC_STATUS_RESOURCE_EXHAUSTED, st);
    if (!injected) {
      TEST_ASSERT_TRUE(echoed);
      oom_report("gzip reply (server thread)", n);
      break;
    }
  }

  /* 3. client unary: only the test thread's allocations fail (connection, headers, data,
   * message queue). A failed call reports an error; the client then still works. */
  char med_req[512];
  memset(med_req, 'R', sizeof(med_req));
  for (int n = 0;; n++) {
    OOM_ASSERT_STEP(n);
    oom_enable_self(n);
    grpc_client_response r = {0};
    grpc_err rc =
        grpc_client_unary(c, "/oom.Svc/Echo", med_req, sizeof(med_req), nullptr, 0, 2000, &r);
    bool injected = oom_disable();
    bool echoed = rc == GRPC_OK && response_is(&r, med_req, sizeof(med_req));
    grpc_status st = r.status;
    grpc_client_response_free(&r);
    if (!echoed) {
      TEST_ASSERT_TRUE_MESSAGE(injected, "unary failed without OOM");
      if (rc == GRPC_OK) /* transport OK but no response: must be reported as an RPC error */
        TEST_ASSERT_NOT_EQUAL_INT(GRPC_STATUS_OK, st);
      else
        TEST_ASSERT_NOT_EQUAL_INT(GRPC_ERR_TIMEOUT, rc);
      assert_echo_works(c);
    }
    if (!injected) {
      oom_report("grpc_client_unary (client thread)", n);
      break;
    }
  }

  /* 4. client stream_open / send */
  for (int n = 0;; n++) {
    OOM_ASSERT_STEP(n);
    oom_enable_self(n);
    grpc_err err = GRPC_OK;
    grpc_client_stream *st = grpc_client_stream_open(c, "/oom.Svc/Echo", nullptr, 0, 2000, &err);
    grpc_err rc_send = GRPC_OK;
    if (st != nullptr)
      rc_send = grpc_client_stream_send(st, "x", 1);
    bool injected = oom_disable();
    if (st == nullptr) {
      TEST_ASSERT_TRUE(injected);
      TEST_ASSERT_EQUAL_INT(GRPC_ERR_NOMEM, err);
    } else {
      TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
      ASSERT_OK_OR_NOMEM(rc_send);
      TEST_ASSERT_TRUE(injected || rc_send == GRPC_OK);
      grpc_client_stream_cancel(st);
    }
    assert_echo_works(c);
    if (!injected) {
      oom_report("grpc_client_stream_open/send", n);
      break;
    }
  }

  assert_server_expectations();
  grpc_client_free(c);
  g_fx.client = nullptr;
  fx_server_stop();
  TEST_ASSERT_EQUAL_INT(GRPC_OK, atomic_load(&g_srv_rc));
}

/* ---- request parsing on the server thread ------------------------------------------------ */

/* Streaming handler for /oom.Svc/Recv. The first message is answered with "armed" and then arms
 * a one-shot failure on the server thread, so one allocation made while receiving and parsing
 * the following messages fails; on_end disarms it and replies with the number of messages seen. */
static atomic_int g_recv_count = 0;

static void recv_on_message(grpc_call *call, [[maybe_unused]] const grpc_message *msg,
                            [[maybe_unused]] void *ud) {
  if (atomic_fetch_add(&g_recv_count, 1) == 0) {
    SRV_EXPECT(grpc_call_send_data(call, "armed", 5) == GRPC_OK);
    oom_enable_once_on(oom_thread_id(), atomic_load(&g_step));
  }
}

static void recv_on_end(grpc_call *call, [[maybe_unused]] void *ud) {
  bool injected = oom_disable();
  atomic_store(&g_step_injected, injected ? 1 : 0);
  char reply[16];
  int n = snprintf(reply, sizeof(reply), "%d", atomic_load(&g_recv_count));
  SRV_EXPECT(grpc_call_send_data(call, reply, (size_t)n) == GRPC_OK);
  SRV_EXPECT(grpc_call_finish(call, GRPC_STATUS_OK, nullptr) == GRPC_OK);
}

void test_oom_request_parsing(void) {
  grpc_server *srv = fx_server_new();
  const grpc_stream_handler recv = {.on_message = recv_on_message, .on_end = recv_on_end};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle_stream(srv, "/oom.Svc/Recv", &recv, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv, "/oom.Svc/Echo", oom_echo_handler, nullptr));
  fx_server_start();
  grpc_client *c = fx_client_new(grpc_server_port(srv));

  /* Step n fails the n-th allocation of the server thread (only that one) while it sends
   * "armed" and receives five more messages: the call either completes, ends with
   * RESOURCE_EXHAUSTED (message creation or the pending queue failed), or the stream or
   * connection is lost (nghttp2 itself ran out of memory). The server never crashes, leaks or
   * delivers a request with a message silently missing. */
  bool saw_exhausted = false;
  for (int n = 0;; n++) {
    OOM_ASSERT_STEP(n);
    atomic_store(&g_step, n);
    atomic_store(&g_step_injected, -1);
    atomic_store(&g_recv_count, 0);
    grpc_err err = GRPC_OK;
    grpc_client_stream *st = grpc_client_stream_open(c, "/oom.Svc/Recv", nullptr, 0, 2000, &err);
    TEST_ASSERT_NOT_NULL(st);
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(st, "m0", 2));
    /* sending "armed" itself allocates after arming, so an early step may lose the connection
     * before it arrives; later steps reach the allocations made while parsing */
    grpc_message *armed = nullptr;
    grpc_err rc_armed = grpc_client_stream_recv(st, &armed);
    bool got_armed = rc_armed == GRPC_OK && armed != nullptr;
    if (got_armed)
      TEST_ASSERT_EQUAL_MEMORY("armed", armed->data, 5);
    else
      ASSERT_ERR_ONE_OF2(rc_armed, GRPC_ERR_CLOSED, GRPC_ERR_IO);
    grpc_message_free(armed);
    for (int i = 1; got_armed && i <= 5; i++)
      (void)grpc_client_stream_send(st, "msg", 3); /* may fail once the connection is gone */
    grpc_client_response r = {0};
    grpc_err rc = grpc_client_stream_finish(st, &r);
    /* on_end does not run when the call was aborted: disarm from here then */
    int handler_injected = atomic_load(&g_step_injected);
    bool injected = oom_disable() || handler_injected == 1;
    bool complete = rc == GRPC_OK && r.status == GRPC_STATUS_OK && r.message != nullptr &&
                    r.message->len == 1 && r.message->data[0] == '6';
    bool exhausted = rc == GRPC_OK && r.status == GRPC_STATUS_RESOURCE_EXHAUSTED;
    grpc_status status = r.status;
    if (getenv("OOM_VERBOSE") != nullptr)
      printf("[oom] request parsing step %d: armed=%d rc=%d status=%d msg=%s count=%d inj=%d\n", n,
             got_armed, (int)rc, (int)status, r.status_message ? r.status_message : "-",
             atomic_load(&g_recv_count), handler_injected);
    grpc_client_response_free(&r);
    assert_server_expectations();
    if (!injected) {
      TEST_ASSERT_TRUE_MESSAGE(complete, "request without OOM did not complete");
      oom_report("request parsing (server thread)", n);
      break;
    }
    if (exhausted)
      saw_exhausted = true;
    if (!complete && !exhausted && rc == GRPC_OK) {
      /* an RPC error other than RESOURCE_EXHAUSTED: the server reset the stream or lost the
       * connection, never OK with a wrong message count */
      TEST_ASSERT_NOT_EQUAL_INT(GRPC_STATUS_OK, status);
    }
    assert_echo_works(c);
  }
  TEST_ASSERT_TRUE_MESSAGE(saw_exhausted,
                           "no step reached the RESOURCE_EXHAUSTED path of request parsing");

  grpc_client_free(c);
  g_fx.client = nullptr;
  fx_server_stop();
  TEST_ASSERT_EQUAL_INT(GRPC_OK, atomic_load(&g_srv_rc));
}

#ifndef _WIN32
/* Runs on the server thread: adds a trailer, then replies with allocation g_step failing. */
static void oom_web_handler(grpc_call *call, const grpc_message *req, [[maybe_unused]] void *ud) {
  grpc_metadata tr[] = {{.key = "x-t", .value = "val"}};
  SRV_EXPECT(grpc_call_add_trailers(call, tr, 1) == GRPC_OK);
  oom_enable_on(oom_thread_id(), atomic_load(&g_step));
  grpc_err rc = grpc_call_reply_data(call, req->data, req->len);
  bool injected = oom_disable();
  SRV_EXPECT(rc == GRPC_OK || rc == GRPC_ERR_NOMEM);
  SRV_EXPECT(injected || rc == GRPC_OK);
  if (rc != GRPC_OK) /* a failed reply leaves the call open and finishable */
    SRV_EXPECT(grpc_call_finish(call, GRPC_STATUS_RESOURCE_EXHAUSTED, "oom") == GRPC_OK);
  atomic_store(&g_step_injected, injected ? 1 : 0);
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

static void oom_h2_flush(nghttp2_session *sess, int fd) {
  for (;;) {
    const uint8_t *out;
    ssize_t wn = nghttp2_session_mem_send(sess, &out);
    if (wn <= 0)
      break;
    oom_send_all(fd, out, (size_t)wn);
  }
}

/* State of the raw nghttp2 client's current request. */
typedef struct oom_web_req {
  int32_t sid;
  bool closed;
  int http_status;
} oom_web_req;

static int oom_web_on_header([[maybe_unused]] nghttp2_session *s, const nghttp2_frame *f,
                             const uint8_t *name, size_t namelen, const uint8_t *value,
                             size_t valuelen, [[maybe_unused]] uint8_t flags, void *ud) {
  oom_web_req *r = ud;
  if (f->hd.stream_id == r->sid && namelen == 7 && memcmp(name, ":status", 7) == 0 && valuelen == 3)
    r->http_status = (value[0] - '0') * 100 + (value[1] - '0') * 10 + (value[2] - '0');
  return 0;
}

static int oom_web_on_close([[maybe_unused]] nghttp2_session *s, int32_t sid,
                            [[maybe_unused]] uint32_t code, void *ud) {
  oom_web_req *r = ud;
  if (sid == r->sid)
    r->closed = true;
  return 0;
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

/* Minimal HTTP/2 server built on nghttp2's own (libc) allocator, so that none of its
 * allocations go through the fault-injecting grpc allocator. Serves one connection at a time. */
static void *oom_raw_h2_server(void *arg) {
  int lfd = *(int *)arg;
  struct timeval tv = {.tv_sec = 0, .tv_usec = 100000};
  setsockopt(lfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  while (!atomic_load(&g_oom_raw_stop)) {
    int cfd = accept(lfd, nullptr, nullptr);
    if (cfd < 0)
      continue;
    setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    int one = 1; /* no Nagle / delayed-ACK stalls between frames */
    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    nghttp2_session_callbacks *cbs = nullptr;
    nghttp2_session_callbacks_new(&cbs);
    nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, oom_raw_srv_on_frame);
    nghttp2_session *sess = nullptr;
    nghttp2_session_server_new(&sess, cbs, nullptr);
    nghttp2_session_callbacks_del(cbs);
    nghttp2_submit_settings(sess, NGHTTP2_FLAG_NONE, nullptr, 0);
    oom_h2_flush(sess, cfd);
    uint8_t rbuf[4096];
    while (!atomic_load(&g_oom_raw_stop)) {
      ssize_t n = recv(cfd, rbuf, sizeof(rbuf), 0);
      if (n == 0)
        break;
      if (n < 0)
        continue;
      if (nghttp2_session_mem_recv(sess, rbuf, (size_t)n) < 0)
        break;
      oom_h2_flush(sess, cfd);
    }
    nghttp2_session_del(sess);
    close(cfd);
  }
  return nullptr;
}

/* Sends one gRPC-Web request on the fixture's raw nghttp2 connection and waits (up to ~5 s) for
 * its stream to close. */
static void oom_web_request(oom_web_req *req, const char *content_type, const uint8_t *body,
                            size_t body_len) {
  nghttp2_nv hdrs[] = {
      {(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)"/oom.Web/Call", 5, 13, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"content-type", (uint8_t *)content_type, 12, strlen(content_type),
       NGHTTP2_NV_FLAG_NONE},
  };
  oom_h2_buf src = {.data = body, .len = body_len, .off = 0};
  nghttp2_data_provider dp = {.source.ptr = &src, .read_callback = oom_h2_read_cb};
  *req = (oom_web_req){0};
  req->sid = nghttp2_submit_request(g_fx.web_sess, nullptr, hdrs, 5, &dp, nullptr);
  TEST_ASSERT_TRUE(req->sid > 0);
  oom_h2_flush(g_fx.web_sess, g_fx.web_fd);
  uint8_t rbuf[4096];
  for (int tries = 0; !req->closed && tries < 50; tries++) {
    ssize_t n = recv(g_fx.web_fd, rbuf, sizeof(rbuf), 0);
    if (n == 0)
      break;
    if (n < 0)
      continue; /* SO_RCVTIMEO tick */
    TEST_ASSERT_TRUE(nghttp2_session_mem_recv(g_fx.web_sess, rbuf, (size_t)n) >= 0);
    oom_h2_flush(g_fx.web_sess, g_fx.web_fd);
  }
  TEST_ASSERT_TRUE_MESSAGE(req->closed, "gRPC-Web response stream did not complete");
}

void test_oom_grpc_web_and_client_deterministic(void) {
  /* 1. Server route registration OOM (before the server thread exists) */
  grpc_server *srv = fx_server_new();
  for (int n = 0;; n++) {
    OOM_ASSERT_STEP(n);
    char path[32];
    snprintf(path, sizeof(path), "/oom/Route%d", n);
    oom_enable_self(n);
    grpc_err rc = grpc_server_handle(srv, path, oom_web_handler, nullptr);
    bool injected = oom_disable();
    ASSERT_OK_OR_NOMEM(rc);
    TEST_ASSERT_TRUE(injected || rc == GRPC_OK);
    TEST_ASSERT_EQUAL_INT(rc == GRPC_OK ? GRPC_ERR_EXISTS : GRPC_OK,
                          grpc_server_handle(srv, path, oom_web_handler, nullptr));
    if (!injected) {
      oom_report("grpc_server_handle", n);
      break;
    }
  }
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv, "/oom.Web/Call", oom_web_handler, nullptr));
  fx_server_start();

  /* 2. gRPC-Web-Text and gRPC-Web binary OOM inside grpc_call_reply_data / call finish, one
   * request per allocation point on the server thread */
  const uint8_t wframe[] = {0x00, 0x00, 0x00, 0x00, 0x02, 'o', 'k'};
  size_t wb64_len = 0;
  char *wb64 = grpc_metadata_encode_bin(wframe, sizeof(wframe), &wb64_len);
  TEST_ASSERT_NOT_NULL(wb64);
  uint8_t wb64_copy[32];
  TEST_ASSERT_TRUE(wb64_len <= sizeof(wb64_copy));
  memcpy(wb64_copy, wb64, wb64_len);
  grpc_free(wb64);
  const char *cts[] = {"application/grpc-web-text", "application/grpc-web"};
  for (int mode = 0; mode < 2; mode++) {
    g_fx.web_fd = socket(AF_INET, SOCK_STREAM, 0);
    TEST_ASSERT_TRUE(g_fx.web_fd >= 0);
    struct sockaddr_in sin = {0};
    sin.sin_family = AF_INET;
    sin.sin_port = htons(grpc_server_port(srv));
    inet_pton(AF_INET, "127.0.0.1", &sin.sin_addr);
    TEST_ASSERT_EQUAL_INT(0, connect(g_fx.web_fd, (struct sockaddr *)&sin, sizeof(sin)));
    struct timeval tv = {.tv_sec = 0, .tv_usec = 100000};
    setsockopt(g_fx.web_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    int one = 1;
    setsockopt(g_fx.web_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    oom_web_req req = {0};
    nghttp2_session_callbacks *cbs = nullptr;
    TEST_ASSERT_EQUAL_INT(0, nghttp2_session_callbacks_new(&cbs));
    nghttp2_session_callbacks_set_on_header_callback(cbs, oom_web_on_header);
    nghttp2_session_callbacks_set_on_stream_close_callback(cbs, oom_web_on_close);
    int rv = nghttp2_session_client_new(&g_fx.web_sess, cbs, &req);
    nghttp2_session_callbacks_del(cbs);
    TEST_ASSERT_EQUAL_INT(0, rv);
    nghttp2_submit_settings(g_fx.web_sess, NGHTTP2_FLAG_NONE, nullptr, 0);

    for (int n = 0;; n++) {
      OOM_ASSERT_STEP(n);
      atomic_store(&g_step, n);
      atomic_store(&g_step_injected, -1);
      if (mode == 0)
        oom_web_request(&req, cts[mode], wb64_copy, wb64_len);
      else
        oom_web_request(&req, cts[mode], wframe, sizeof(wframe));
      assert_server_expectations();
      TEST_ASSERT_EQUAL_INT(200, req.http_status);
      int injected = atomic_load(&g_step_injected);
      TEST_ASSERT_NOT_EQUAL_INT_MESSAGE(-1, injected, "gRPC-Web handler did not run");
      if (!injected) {
        oom_report(mode == 0 ? "gRPC-Web-Text reply (server thread)"
                             : "gRPC-Web reply (server thread)",
                   n);
        break;
      }
    }
    nghttp2_session_del(g_fx.web_sess);
    g_fx.web_sess = nullptr;
    close(g_fx.web_fd);
    g_fx.web_fd = -1;
  }
  assert_server_expectations();
  fx_server_stop();
  TEST_ASSERT_EQUAL_INT(GRPC_OK, atomic_load(&g_srv_rc));

  /* 3. Client-only OOM against the raw nghttp2 server (libc allocator, own thread): only the
   * test thread's allocations fail */
  g_oom_resp_frame[0] = 0;
  uint32_t rlen = htonl(sizeof(g_oom_resp_frame) - 5);
  memcpy(g_oom_resp_frame + 1, &rlen, 4);
  memset(g_oom_resp_frame + 5, 'Z', sizeof(g_oom_resp_frame) - 5);
  uint8_t expect[sizeof(g_oom_resp_frame) - 5];
  memset(expect, 'Z', sizeof(expect));

  g_fx.raw_lfd = socket(AF_INET, SOCK_STREAM, 0);
  TEST_ASSERT_TRUE(g_fx.raw_lfd >= 0);
  struct sockaddr_in lsin = {0};
  lsin.sin_family = AF_INET;
  lsin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  lsin.sin_port = 0;
  TEST_ASSERT_EQUAL_INT(0, bind(g_fx.raw_lfd, (struct sockaddr *)&lsin, sizeof(lsin)));
  TEST_ASSERT_EQUAL_INT(0, listen(g_fx.raw_lfd, 64));
  socklen_t slen = sizeof(lsin);
  TEST_ASSERT_EQUAL_INT(0, getsockname(g_fx.raw_lfd, (struct sockaddr *)&lsin, &slen));
  uint16_t lport = ntohs(lsin.sin_port);

  atomic_store(&g_oom_raw_stop, 0);
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&g_fx.raw_th, nullptr, oom_raw_h2_server, &g_fx.raw_lfd));
  g_fx.raw_running = true;

  for (int n = 0;; n++) {
    OOM_ASSERT_STEP(n);
    grpc_client *c = fx_client_new(lport);
    grpc_client_response r = {0};
    oom_enable_self(n);
    grpc_err rc = grpc_client_unary(c, "/raw.Svc/Echo", "hi", 2, nullptr, 0, 2000, &r);
    bool injected = oom_disable();
    bool ok = rc == GRPC_OK && response_is(&r, expect, sizeof(expect));
    grpc_status st = r.status;
    grpc_client_response_free(&r);
    if (!ok) {
      TEST_ASSERT_TRUE_MESSAGE(injected, "raw unary failed without OOM");
      if (rc == GRPC_OK)
        TEST_ASSERT_NOT_EQUAL_INT(GRPC_STATUS_OK, st);
      else
        TEST_ASSERT_NOT_EQUAL_INT(GRPC_ERR_TIMEOUT, rc);
      /* the same client recovers */
      TEST_ASSERT_EQUAL_INT(GRPC_OK,
                            grpc_client_unary(c, "/raw.Svc/Echo", "hi", 2, nullptr, 0, 2000, &r));
      ok = response_is(&r, expect, sizeof(expect));
      grpc_client_response_free(&r);
      TEST_ASSERT_TRUE_MESSAGE(ok, "client unusable after OOM");
    }
    grpc_client_free(c);
    g_fx.client = nullptr;
    if (!injected) {
      oom_report("grpc_client_unary (raw server)", n);
      break;
    }
  }
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
  RUN_TEST(test_oom_request_parsing);
#ifndef _WIN32
  RUN_TEST(test_oom_grpc_web_and_client_deterministic);
#endif
  return UNITY_END();
}
