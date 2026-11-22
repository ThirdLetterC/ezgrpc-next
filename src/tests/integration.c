#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <nghttp2/nghttp2.h>
#include <poll.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>
#endif
#include <uv.h>
#include "internal/grpc_threads.h"
#include "grpc.h"
#include "test.h"

/* ---- Synchronisation helpers ------------------------------------------------------------------ */

/* Upper bound for every "wait until something observable happens" loop. */
#define WAIT_DEADLINE_MS 3000

static uint64_t now_ms(void) {
  return uv_hrtime() / 1000000u;
}

/* Re-evaluates cond every 5 ms until it holds or WAIT_DEADLINE_MS passes. */
#define WAIT_FOR(cond)                                    \
  do {                                                    \
    const uint64_t wf_end_ = now_ms() + WAIT_DEADLINE_MS; \
    while (!(cond) && now_ms() < wf_end_)                 \
      uv_sleep(5);                                        \
  } while (0)

#define ASSERT_EVENTUALLY(cond)                                        \
  do {                                                                 \
    WAIT_FOR(cond);                                                    \
    TEST_ASSERT_TRUE_MESSAGE((cond), "timed out waiting for: " #cond); \
  } while (0)

/* Same for a poll-driven server: drives its loop on this thread while waiting. */
#define ASSERT_POLL_EVENTUALLY(srv, cond)                              \
  do {                                                                 \
    const uint64_t pf_end_ = now_ms() + WAIT_DEADLINE_MS;              \
    while (!(cond) && now_ms() < pf_end_)                              \
      (void)grpc_server_poll((srv), 5);                                \
    TEST_ASSERT_TRUE_MESSAGE((cond), "timed out waiting for: " #cond); \
  } while (0)

static grpc_server_stats srv_stats(const grpc_server *srv) {
  grpc_server_stats st = {0};
  (void)grpc_server_get_stats(srv, &st);
  return st;
}

static const char *md_find(const grpc_metadata *md, size_t n, const char *key) {
  for (size_t i = 0; i < n; i++) {
    if (md[i].key != nullptr && strcmp(md[i].key, key) == 0)
      return md[i].value;
  }
  return nullptr;
}

/* Checks made on the server thread. Test assertions abort the process there (see test.h): a
 * failing check records its source line, and the test thread asserts it with
 * ASSERT_HANDLER_CHECKS(). */
static atomic_int g_handler_fail_line;
#define HCHECK(cond)                                                                   \
  do {                                                                                 \
    if (!(cond)) {                                                                     \
      int hc_zero_ = 0;                                                                \
      (void)atomic_compare_exchange_strong(&g_handler_fail_line, &hc_zero_, __LINE__); \
    }                                                                                  \
  } while (0)
#define ASSERT_HANDLER_CHECKS()                                       \
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, atomic_load(&g_handler_fail_line), \
                                "a server-thread check failed at this line of integration.c")

/* ---- Per-test fixture -------------------------------------------------------------------------
 * Everything a test starts (server threads, clients, streams, sockets, raw HTTP/2 sessions, worker
 * threads, buffers, temporary files) is recorded here, so tearDown() can stop, join and free what a
 * failed assertion left behind before the next test runs. */

enum { FX_SLOTS = 16 };

typedef struct fx_server_slot {
  grpc_server *srv;
  pthread_t th;
  bool threaded;
} fx_server_slot;

typedef struct fx_log_slot {
  FILE *f;
  char path[320];
} fx_log_slot;

#ifndef _WIN32
typedef struct fx_raw_thread_slot {
  pthread_t th;
  int listen_fd;
  bool used;
} fx_raw_thread_slot;
#endif

static struct {
  fx_server_slot servers[FX_SLOTS];
  void *clients[FX_SLOTS];
  void *streams[FX_SLOTS];
  void *mem[FX_SLOTS];
  fx_log_slot logs[4];
#ifndef _WIN32
  int fds[FX_SLOTS]; /* fd + 1; 0 = empty */
  void *sessions[FX_SLOTS];
  fx_raw_thread_slot raw_threads[2];
  char tmpdir[256];
  char tmpfiles[4][320];
#endif
} g_fx;

static void fx_put(void **slots, void *p) {
  if (p == nullptr)
    return;
  for (size_t i = 0; i < FX_SLOTS; i++) {
    if (slots[i] == nullptr) {
      slots[i] = p;
      return;
    }
  }
  TEST_FAIL_MESSAGE("fixture slots exhausted");
}

static void fx_take(void **slots, void *p) {
  for (size_t i = 0; i < FX_SLOTS; i++) {
    if (slots[i] == p) {
      slots[i] = nullptr;
      return;
    }
  }
}

static void *run_server_fn(void *arg) {
  (void)grpc_server_run((grpc_server *)arg);
  return nullptr;
}

static fx_server_slot *fx_server_slot_of(const grpc_server *srv) {
  for (size_t i = 0; i < FX_SLOTS; i++) {
    if (g_fx.servers[i].srv == srv)
      return &g_fx.servers[i];
  }
  return nullptr;
}

/* Records a server so tearDown() frees it. Returns it (asserted non-null). */
static grpc_server *fx_server(grpc_server *srv) {
  TEST_ASSERT_NOT_NULL(srv);
  fx_server_slot *slot = fx_server_slot_of(nullptr);
  TEST_ASSERT_NOT_NULL_MESSAGE(slot, "fixture server slots exhausted");
  *slot = (fx_server_slot){.srv = srv};
  return srv;
}

/* Starts grpc_server_run() on a new thread. Configure the server before calling this. */
static void fx_start(grpc_server *srv) {
  fx_server_slot *slot = fx_server_slot_of(srv);
  TEST_ASSERT_NOT_NULL(slot);
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&slot->th, nullptr, run_server_fn, srv));
  slot->threaded = true;
}

/* Stops the server thread and joins it; the server itself stays allocated. */
static void fx_stop_join(grpc_server *srv) {
  fx_server_slot *slot = fx_server_slot_of(srv);
  if (slot == nullptr || !slot->threaded)
    return;
  grpc_server_stop(srv);
  pthread_join(slot->th, nullptr);
  slot->threaded = false;
}

static void fx_server_free(grpc_server *srv) {
  fx_stop_join(srv);
  fx_server_slot *slot = fx_server_slot_of(srv);
  if (slot != nullptr)
    *slot = (fx_server_slot){0};
  grpc_server_free(srv);
}

static grpc_client *fx_client(grpc_client *c) {
  fx_put(g_fx.clients, c);
  return c;
}

static void fx_client_free(grpc_client *c) {
  fx_take(g_fx.clients, c);
  grpc_client_free(c);
}

static grpc_client_stream *fx_stream(grpc_client_stream *st) {
  fx_put(g_fx.streams, st);
  return st;
}

static void fx_stream_cancel(grpc_client_stream *st) {
  fx_take(g_fx.streams, st);
  grpc_client_stream_cancel(st);
}

static grpc_err fx_stream_finish(grpc_client_stream *st, grpc_client_response *resp) {
  fx_take(g_fx.streams, st);
  return grpc_client_stream_finish(st, resp);
}

/* grpc_malloc()ed buffer released with grpc_free() */
static void *fx_mem(void *p) {
  TEST_ASSERT_NOT_NULL(p);
  fx_put(g_fx.mem, p);
  return p;
}

static void fx_free(void *p) {
  fx_take(g_fx.mem, p);
  grpc_free(p);
}

#ifndef _WIN32
static const char *fx_tmpdir(void) {
  if (g_fx.tmpdir[0] == '\0') {
    const char *base = getenv("TMPDIR");
    if (base == nullptr || base[0] == '\0' || strlen(base) > 64)
      base = "/tmp";
    snprintf(g_fx.tmpdir, sizeof(g_fx.tmpdir), "%s/ezgrpc-integ-XXXXXX", base);
    if (mkdtemp(g_fx.tmpdir) == nullptr) {
      g_fx.tmpdir[0] = '\0';
      TEST_FAIL_MESSAGE("mkdtemp failed");
    }
  }
  return g_fx.tmpdir;
}

/* Unique path inside this test's temporary directory, removed by tearDown(). */
static const char *fx_tmp_path(const char *name) {
  const char *dir = fx_tmpdir();
  for (size_t i = 0; i < sizeof(g_fx.tmpfiles) / sizeof(g_fx.tmpfiles[0]); i++) {
    if (g_fx.tmpfiles[i][0] == '\0') {
      snprintf(g_fx.tmpfiles[i], sizeof(g_fx.tmpfiles[i]), "%s/%s", dir, name);
      return g_fx.tmpfiles[i];
    }
  }
  TEST_FAIL_MESSAGE("fixture tmp file slots exhausted");
  return nullptr;
}

static int fx_fd(int fd) {
  TEST_ASSERT_TRUE_MESSAGE(fd >= 0, strerror(errno));
  for (size_t i = 0; i < FX_SLOTS; i++) {
    if (g_fx.fds[i] == 0) {
      g_fx.fds[i] = fd + 1;
      return fd;
    }
  }
  close(fd);
  TEST_FAIL_MESSAGE("fixture fd slots exhausted");
  return -1;
}

static void fx_close(int fd) {
  for (size_t i = 0; i < FX_SLOTS; i++) {
    if (g_fx.fds[i] == fd + 1)
      g_fx.fds[i] = 0;
  }
  close(fd);
}

static void fx_session_del(nghttp2_session *s) {
  fx_take(g_fx.sessions, s);
  nghttp2_session_del(s);
}

/* Thread of a raw (non-library) server blocked in accept()/recv() on listen_fd. */
static void fx_raw_thread(pthread_t th, int listen_fd) {
  for (size_t i = 0; i < 2; i++) {
    if (!g_fx.raw_threads[i].used) {
      g_fx.raw_threads[i] = (fx_raw_thread_slot){.th = th, .listen_fd = listen_fd, .used = true};
      return;
    }
  }
  TEST_FAIL_MESSAGE("fixture raw thread slots exhausted");
}

static void fx_raw_join(pthread_t th) {
  for (size_t i = 0; i < 2; i++) {
    if (g_fx.raw_threads[i].used && pthread_equal(g_fx.raw_threads[i].th, th))
      g_fx.raw_threads[i].used = false;
  }
  pthread_join(th, nullptr);
}
#endif

/* A log sink for a server, so a test can check what it logged instead of flooding stderr. */
static FILE *fx_log(void) {
  for (size_t i = 0; i < sizeof(g_fx.logs) / sizeof(g_fx.logs[0]); i++) {
    fx_log_slot *slot = &g_fx.logs[i];
    if (slot->f != nullptr)
      continue;
#ifndef _WIN32
    char name[32];
    snprintf(name, sizeof(name), "server-%zu.log", i);
    snprintf(slot->path, sizeof(slot->path), "%s", fx_tmp_path(name));
    slot->f = fopen(slot->path, "a");
#else
    slot->f = tmpfile();
#endif
    TEST_ASSERT_NOT_NULL(slot->f);
    setvbuf(slot->f, nullptr, _IONBF, 0);
    return slot->f;
  }
  TEST_FAIL_MESSAGE("fixture log slots exhausted");
  return nullptr;
}

/* True when the server writing to `log` has logged `needle`. Always true on Windows. */
static bool log_contains(FILE *log, const char *needle) {
#ifndef _WIN32
  for (size_t i = 0; i < sizeof(g_fx.logs) / sizeof(g_fx.logs[0]); i++) {
    if (g_fx.logs[i].f != log)
      continue;
    FILE *r = fopen(g_fx.logs[i].path, "rb");
    if (r == nullptr)
      return false;
    static char buf[256 * 1024];
    size_t n = fread(buf, 1, sizeof(buf) - 1, r);
    fclose(r);
    buf[n] = '\0';
    return strstr(buf, needle) != nullptr;
  }
  return false;
#else
  (void)log;
  (void)needle;
  return true;
#endif
}

/* Worker threads answering calls through the *_async API. Joined (not detached) so none can still
 * be inside grpc_call_unref() when its server is freed. */
static pthread_mutex_t g_workers_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_t g_workers[FX_SLOTS];
static size_t g_nb_workers;

/* Server thread: takes a call reference and hands it to fn on a new worker thread. */
static void spawn_call_worker(grpc_call *call, void *(*fn)(void *)) {
  grpc_call_ref(call);
  pthread_mutex_lock(&g_workers_mu);
  bool ok =
      g_nb_workers < FX_SLOTS && pthread_create(&g_workers[g_nb_workers], nullptr, fn, call) == 0;
  if (ok)
    g_nb_workers++;
  pthread_mutex_unlock(&g_workers_mu);
  if (!ok) {
    grpc_call_unref(call);
    HCHECK(false);
  }
}

static void join_call_workers(void) {
  pthread_t ths[FX_SLOTS];
  pthread_mutex_lock(&g_workers_mu);
  size_t n = g_nb_workers;
  memcpy(ths, g_workers, n * sizeof(ths[0]));
  g_nb_workers = 0;
  pthread_mutex_unlock(&g_workers_mu);
  for (size_t i = 0; i < n; i++)
    pthread_join(ths[i], nullptr);
}

static void reset_handler_state(void);

void setUp(void) {
  atomic_store(&g_handler_fail_line, 0);
  reset_handler_state();
}

void tearDown(void) {
  join_call_workers();
  for (size_t i = 0; i < FX_SLOTS; i++) {
    if (g_fx.streams[i] != nullptr)
      grpc_client_stream_cancel(g_fx.streams[i]);
  }
  for (size_t i = 0; i < FX_SLOTS; i++)
    grpc_client_free(g_fx.clients[i]);
#ifndef _WIN32
  for (size_t i = 0; i < FX_SLOTS; i++) {
    if (g_fx.sessions[i] != nullptr)
      nghttp2_session_del(g_fx.sessions[i]);
  }
  for (size_t i = 0; i < 2; i++) {
    if (g_fx.raw_threads[i].used) {
      (void)shutdown(g_fx.raw_threads[i].listen_fd, SHUT_RDWR);
      pthread_join(g_fx.raw_threads[i].th, nullptr);
    }
  }
  for (size_t i = 0; i < FX_SLOTS; i++) {
    if (g_fx.fds[i] != 0)
      close(g_fx.fds[i] - 1);
  }
#endif
  for (size_t i = 0; i < FX_SLOTS; i++) {
    if (g_fx.servers[i].threaded)
      grpc_server_stop(g_fx.servers[i].srv);
  }
  for (size_t i = 0; i < FX_SLOTS; i++) {
    if (g_fx.servers[i].threaded)
      pthread_join(g_fx.servers[i].th, nullptr);
    if (g_fx.servers[i].srv != nullptr)
      grpc_server_free(g_fx.servers[i].srv);
  }
  for (size_t i = 0; i < FX_SLOTS; i++)
    grpc_free(g_fx.mem[i]);
  for (size_t i = 0; i < sizeof(g_fx.logs) / sizeof(g_fx.logs[0]); i++) {
    if (g_fx.logs[i].f != nullptr)
      fclose(g_fx.logs[i].f);
  }
#ifndef _WIN32
  for (size_t i = 0; i < sizeof(g_fx.tmpfiles) / sizeof(g_fx.tmpfiles[0]); i++) {
    if (g_fx.tmpfiles[i][0] != '\0')
      (void)unlink(g_fx.tmpfiles[i]);
  }
  if (g_fx.tmpdir[0] != '\0')
    (void)rmdir(g_fx.tmpdir);
#endif
  memset(&g_fx, 0, sizeof(g_fx));
}

static bool bearer_validator(const char *token, [[maybe_unused]] void *userdata) {
  return strcmp(token, "valid-token") == 0;
}

/* ---- Handlers for comprehensive end-to-end integration tests ---------------- */

/* State written by handlers on the server thread and read by the test thread. */
static atomic_int g_bp_hit_limit;
static atomic_int g_bp_writable_fired;
static atomic_int g_cancel_started;
static atomic_int g_cancel_hits;
static atomic_int g_hold_started;
static atomic_int g_on_done_calls;
static atomic_int g_cancel_cb_fired;

static void reset_handler_state(void) {
  atomic_store(&g_bp_hit_limit, 0);
  atomic_store(&g_bp_writable_fired, 0);
  atomic_store(&g_cancel_started, 0);
  atomic_store(&g_cancel_hits, 0);
  atomic_store(&g_hold_started, 0);
  atomic_store(&g_on_done_calls, 0);
  atomic_store(&g_cancel_cb_fired, 0);
}

static void inspect_metadata_handler(grpc_call *call, const grpc_message *req,
                                     [[maybe_unused]] void *userdata) {
  HCHECK(grpc_call_request(call) != nullptr);
  HCHECK(grpc_call_peer(call) != nullptr);
  HCHECK(grpc_call_server(call) != nullptr);
  HCHECK(grpc_call_metadata_count(call) >= 4);

  grpc_metadata first = {0};
  HCHECK(grpc_call_metadata_at(call, 0, &first));
  HCHECK(!grpc_call_metadata_at(call, 9999, &first));

  /* Multi-value metadata iteration */
  size_t idx = 0, len = 0;
  int tag_count = 0;
  while (grpc_call_metadata_next(call, "x-tag", &idx, &len) != nullptr)
    tag_count++;

  /* Deadline check + traceparent hex a-f check */
  int64_t rem = grpc_call_deadline(call);
  HCHECK(rem > 0);
  grpc_trace_context tc = {0};
  HCHECK(grpc_call_trace_context(call, &tc));
  HCHECK(tc.trace_flags == 0xaf);

  /* Initial metadata + custom trailer */
  grpc_metadata init_md[] = {{.key = "x-init", .value = "yes"}};
  HCHECK(grpc_call_send_metadata(call, init_md, 1) == GRPC_OK);
  grpc_metadata tr[] = {{.key = "x-tags-seen", .value = tag_count == 2 ? "2" : "0"}};
  HCHECK(grpc_call_add_trailers(call, tr, 1) == GRPC_OK);

  /* Test per-call compression override (the client accepts deflate) */
  HCHECK(grpc_call_set_compression(call, GRPC_COMPRESS_DEFLATE, 6) == GRPC_OK);
  HCHECK(grpc_call_send_data(call, req->data, req->len) == GRPC_OK);
  /* Setting compression or initial metadata after send returns GRPC_ERR_STATE */
  HCHECK(grpc_call_set_compression(call, GRPC_COMPRESS_GZIP, -1) == GRPC_ERR_STATE);
  HCHECK(grpc_call_send_metadata(call, init_md, 1) == GRPC_ERR_STATE);
  HCHECK(grpc_call_finish(call, GRPC_STATUS_OK, nullptr) == GRPC_OK);
}

static void rich_error_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                               [[maybe_unused]] void *userdata) {
  const uint8_t d1[] = {0x01, 0x02, 0x03};
  const uint8_t d2[] = {0x04, 0x05};
  grpc_status_detail_entry details[] = {
      {.type_url = "type.googleapis.com/google.rpc.ErrorInfo",
       .value = d1,
       .value_len = sizeof(d1)},
      {.type_url = "type.googleapis.com/google.rpc.RetryInfo",
       .value = d2,
       .value_len = sizeof(d2)},
  };
  /* Includes '%' and non-ASCII to exercise percent-encoding of grpc-message */
  (void)grpc_call_finish_with_details(call, GRPC_STATUS_INVALID_ARGUMENT, "100% bad\x01input",
                                      details, 2);
}

static void *async_worker_ok(void *arg) {
  grpc_call *call = arg;
  grpc_metadata md[] = {{.key = "x-async-md", .value = "1"}};
  grpc_metadata tr[] = {{.key = "x-async-tr", .value = "2"}};
  (void)grpc_call_send_metadata_async(call, md, 1);
  (void)grpc_call_send_data_async(call, "part1-", 6);
  (void)grpc_call_add_trailers_async(call, tr, 1);
  (void)grpc_call_reply_async(call, grpc_message_new("part2", 5));
  grpc_call_unref(call);
  return nullptr;
}

static void async_ok_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                             [[maybe_unused]] void *userdata) {
  spawn_call_worker(call, async_worker_ok);
}

static void *async_worker_err(void *arg) {
  grpc_call *call = arg;
  const uint8_t raw[] = {0xAA, 0xBB};
  grpc_status_detail_entry det = {
      .type_url = "type.googleapis.com/google.rpc.DebugInfo",
      .value = raw,
      .value_len = sizeof(raw),
  };
  (void)grpc_call_finish_with_details_async(call, GRPC_STATUS_FAILED_PRECONDITION, "async fail",
                                            &det, 1);
  grpc_call_unref(call);
  return nullptr;
}

static void async_err_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                              [[maybe_unused]] void *userdata) {
  spawn_call_worker(call, async_worker_err);
}

static void on_cancel_cb([[maybe_unused]] grpc_call *c, void *ud) {
  atomic_fetch_add((atomic_int *)ud, 1);
}

static void slow_cancel_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                                void *userdata) {
  grpc_call_on_cancel(call, on_cancel_cb, userdata);
  atomic_store(&g_cancel_started, 1);
}

static void slow_deadline_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                                  [[maybe_unused]] void *userdata) {
  /* Intentionally never finishes so server deadline timer fires DEADLINE_EXCEEDED */
  (void)call;
}

static void writable_cb(grpc_call *call, [[maybe_unused]] void *userdata) {
  atomic_store(&g_bp_writable_fired, 1);
  (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

static void backpressure_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                                 [[maybe_unused]] void *userdata) {
  grpc_call_on_writable(call, writable_cb, nullptr);
  char chunk[4096];
  memset(chunk, 'A', sizeof(chunk));
  for (int i = 0; i < 4; i++) {
    grpc_err err = grpc_call_send_data(call, chunk, sizeof(chunk));
    if (err == GRPC_ERR_LIMIT) {
      atomic_store(&g_bp_hit_limit, 1);
      break;
    }
  }
  HCHECK(grpc_call_send_queue_bytes(call) > 0);
  if (!atomic_load(&g_bp_hit_limit))
    (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

static void large_echo_handler(grpc_call *call, const grpc_message *req,
                               [[maybe_unused]] void *userdata) {
  (void)grpc_call_reply_data(call, req->data, req->len);
}

static grpc_status reject_recv_bad([[maybe_unused]] grpc_call *call, const grpc_message *msg,
                                   const char **err_msg, [[maybe_unused]] void *userdata) {
  if (msg->len == 3 && memcmp(msg->data, "BAD", 3) == 0) {
    *err_msg = "blocked by recv interceptor";
    return GRPC_STATUS_PERMISSION_DENIED;
  }
  return GRPC_STATUS_OK;
}

static grpc_status reject_send_bad([[maybe_unused]] grpc_call *call, const grpc_message *msg,
                                   const char **err_msg, [[maybe_unused]] void *userdata) {
  if (msg->len == 7 && memcmp(msg->data, "BADSEND", 7) == 0) {
    *err_msg = "blocked by send interceptor";
    return GRPC_STATUS_INTERNAL;
  }
  return GRPC_STATUS_OK;
}

/* ---- Integration Test 1: Metadata, Async, Rich Errors, Interceptors, Compression ---- */

void test_integration_full_server_and_client(void) {
  grpc_server *server = fx_server(grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .max_message_size = 256 * 1024,
      .max_send_queue_bytes = 4096,
      .log_level = GRPC_LOG_OFF,
  }));

  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle(server, "/int.Svc/Meta", inspect_metadata_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle(server, "/int.Svc/RichErr", rich_error_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(server, "/int.Svc/AsyncOk", async_ok_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle(server, "/int.Svc/AsyncErr", async_err_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/int.Svc/Cancel", slow_cancel_handler,
                                                    (void *)&g_cancel_hits));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle(server, "/int.Svc/Deadline", slow_deadline_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle(server, "/int.Svc/Backpressure", backpressure_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle(server, "/int.Svc/LargeEcho", large_echo_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle(server, "/auth.Svc/Secret", large_echo_handler, nullptr));

  grpc_interceptor auth = grpc_interceptor_bearer_auth(bearer_validator, nullptr);
  auth.path_prefix = "/auth.Svc/";
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_add_interceptor(server, &auth));

  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_add_interceptor(server, &(grpc_interceptor){
                                                                .name = "guard",
                                                                .path_prefix = "/int.Svc/LargeEcho",
                                                                .on_recv_message = reject_recv_bad,
                                                                .on_send_message = reject_send_bad,
                                                            }));

  fx_start(server);

  grpc_err err = GRPC_OK;
  grpc_client *client = fx_client(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .timeout_ms = 5000,
          .compression = GRPC_COMPRESS_GZIP,
      },
      &err));
  TEST_ASSERT_NOT_NULL(client);

  /* 1. Metadata, repeated keys, deadline, initial md, trailers, deflate compression, traceparent a-f */
  grpc_metadata md[] = {
      {.key = "x-tag", .value = "alpha"},
      {.key = "x-tag", .value = "beta"},
      {.key = "traceparent", .value = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-af"},
  };
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/Meta", "payload", 7, md, 3, 3000, &resp));
  ASSERT_HANDLER_CHECKS();
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_MEMORY("payload", resp.message->data, 7);
  const char *x_init = md_find(resp.headers, resp.nb_headers, "x-init");
  TEST_ASSERT_NOT_NULL(x_init);
  TEST_ASSERT_EQUAL_STRING("yes", x_init);
  const char *enc = md_find(resp.headers, resp.nb_headers, "grpc-encoding");
  TEST_ASSERT_NOT_NULL(enc);
  TEST_ASSERT_EQUAL_STRING("deflate", enc);
  const char *tags_seen = md_find(resp.trailers, resp.nb_trailers, "x-tags-seen");
  TEST_ASSERT_NOT_NULL(tags_seen);
  TEST_ASSERT_EQUAL_STRING("2", tags_seen);
  grpc_client_response_free(&resp);

  /* 2. Rich error details + percent-encoded grpc-message */
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(client, "/int.Svc/RichErr", "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_INVALID_ARGUMENT, resp.status);
  const char *details_b64 = md_find(resp.trailers, resp.nb_trailers, GRPC_STATUS_DETAILS_TRAILER);
  TEST_ASSERT_NOT_NULL(details_b64);
  grpc_status_details decoded = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_status_details_decode_all(details_b64, &decoded));
  TEST_ASSERT_EQUAL_UINT64(2, decoded.nb_details);
  grpc_status_details_free(&decoded);
  grpc_client_response_free(&resp);

  /* 3. Worker-thread async reply & async finish_with_details */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/AsyncOk", "x", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/AsyncErr", "x", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_FAILED_PRECONDITION, resp.status);
  grpc_client_response_free(&resp);
  join_call_workers();

  /* 4. Bearer auth: missing, malformed, wrong token, valid token */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/auth.Svc/Secret", "s", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAUTHENTICATED, resp.status);
  grpc_client_response_free(&resp);

  grpc_metadata bad_fmt[] = {{.key = "authorization", .value = "Basic abc"}};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/auth.Svc/Secret", "s", 1, bad_fmt, 1, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAUTHENTICATED, resp.status);
  grpc_client_response_free(&resp);

  grpc_metadata bad_tok[] = {{.key = "authorization", .value = "Bearer wrong"}};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/auth.Svc/Secret", "s", 1, bad_tok, 1, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAUTHENTICATED, resp.status);
  grpc_client_response_free(&resp);

  grpc_metadata good_tok[] = {{.key = "authorization", .value = "Bearer valid-token"}};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/auth.Svc/Secret", "ok", 2, good_tok, 1, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* 5. Message-level recv & send interceptor rejections */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/LargeEcho", "BAD", 3, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_PERMISSION_DENIED, resp.status);
  grpc_client_response_free(&resp);

  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/LargeEcho", "BADSEND", 7, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_INTERNAL, resp.status);
  grpc_client_response_free(&resp);

  /* 6. Large multi-frame message (> 32 KiB) & unknown method (UNIMPLEMENTED) */
  char *big = fx_mem(grpc_malloc(48 * 1024));
  memset(big, 'Z', 48 * 1024);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(client, "/int.Svc/LargeEcho", big, 48 * 1024,
                                                   nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32(48 * 1024, resp.message->len);
  grpc_client_response_free(&resp);
  fx_free(big);

  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(client, "/no.Such/Method", "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNIMPLEMENTED, resp.status);
  grpc_client_response_free(&resp);

  /* 7. Backpressure limit trigger + on_writable callback firing */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/Backpressure", "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_EQUAL_INT(1, atomic_load(&g_bp_hit_limit));
  TEST_ASSERT_EQUAL_INT(1, atomic_load(&g_bp_writable_fired));
  grpc_client_response_free(&resp);

  /* 8. Server-side deadline expiration (grpc-timeout: 50m) & client stream cancel */
  grpc_metadata short_dl[] = {{.key = "grpc-timeout", .value = "50m"}};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/Deadline", "", 0, short_dl, 1, 2000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_DEADLINE_EXCEEDED, resp.status);
  grpc_client_response_free(&resp);

  grpc_client_stream *cs_cancel =
      fx_stream(grpc_client_stream_open(client, "/int.Svc/Cancel", nullptr, 0, 2000, &err));
  TEST_ASSERT_NOT_NULL(cs_cancel);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(cs_cancel, "wait", 4));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_close_send(cs_cancel));
  /* the handler registered its on_cancel callback; now cancel and wait for it to run */
  ASSERT_EVENTUALLY(atomic_load(&g_cancel_started) == 1);
  fx_stream_cancel(cs_cancel);
  ASSERT_EVENTUALLY(atomic_load(&g_cancel_hits) >= 1);
  TEST_ASSERT_EQUAL_INT(1, atomic_load(&g_cancel_hits));

  fx_client_free(client);
  fx_server_free(server);
  ASSERT_HANDLER_CHECKS();
}

/* ---- Integration Test 2: Health Check & Watch + Reflection v1/v1alpha ------- */

/* The first field after the echoed original_request (0x12 <len> <request>) of a reflection
 * response, or 0 when the message is too short. */
static uint8_t reflection_reply_field(const grpc_message *m, size_t req_len) {
  size_t off = 2 + req_len;
  if (m->len < 2 || m->data[0] != 0x12 || m->data[1] != req_len || m->len <= off)
    return 0;
  return m->data[off];
}

void test_integration_health_watch_and_reflection(void) {
  grpc_server *server = fx_server(grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF}));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_enable(server));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_reflection_enable(server));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(server, "/demo.Echo/Ping", large_echo_handler, nullptr));
  const uint8_t fake_fdp[] = {0x0A, 0x0A, 'd', 'e', 'm', 'o', '.', 'p', 'r', 'o', 't', 'o'};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_reflection_add_file_descriptor(server, "demo.proto", fake_fdp,
                                                                     sizeof(fake_fdp)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_reflection_add_file_descriptor(server, "demo.Echo", fake_fdp,
                                                                     sizeof(fake_fdp)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_set_status(server, "demo.Echo", GRPC_HEALTH_SERVING));

  fx_start(server);

  grpc_err err = GRPC_OK;
  grpc_client *client = fx_client(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1", .port = grpc_server_port(server), .timeout_ms = 3000},
      &err));
  TEST_ASSERT_NOT_NULL(client);

  /* 1. Health Check for "" and "demo.Echo" */
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, GRPC_HEALTH_PATH_CHECK, "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32(2, resp.message->len);
  TEST_ASSERT_EQUAL_UINT8(GRPC_HEALTH_SERVING, resp.message->data[1]);
  grpc_client_response_free(&resp);

  /* HealthCheckRequest { service = "demo.Echo" } */
  const uint8_t hreq[] = {0x0A, 9, 'd', 'e', 'm', 'o', '.', 'E', 'c', 'h', 'o'};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(client, GRPC_HEALTH_PATH_CHECK, hreq,
                                                   sizeof(hreq), nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT8(GRPC_HEALTH_SERVING, resp.message->data[1]);
  grpc_client_response_free(&resp);

  /* a service the server serves but whose status was never set is SERVING (here reflection) */
  const char refl_svc[] = "grpc.reflection.v1.ServerReflection";
  uint8_t hreq_refl[64] = {0x0A, (uint8_t)(sizeof(refl_svc) - 1)};
  memcpy(hreq_refl + 2, refl_svc, sizeof(refl_svc) - 1);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(client, GRPC_HEALTH_PATH_CHECK, hreq_refl,
                                                   2 + sizeof(refl_svc) - 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT8(GRPC_HEALTH_SERVING, resp.message->data[1]);
  grpc_client_response_free(&resp);

  /* a service the server does not serve and has no status: NOT_FOUND (health protocol) */
  const uint8_t hreq_none[] = {0x0A, 8, 'n', 'o', '.', 'S', 'u', 'c', 'h', 'x'};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(client, GRPC_HEALTH_PATH_CHECK, hreq_none,
                                                   sizeof(hreq_none), nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_NOT_FOUND, resp.status);
  TEST_ASSERT_NULL(resp.message);
  grpc_client_response_free(&resp);

  /* 2. Health Watch: receive initial SERVING, update to NOT_SERVING, receive update, cancel */
  grpc_client_stream *watch =
      fx_stream(grpc_client_stream_open(client, GRPC_HEALTH_PATH_WATCH, nullptr, 0, 3000, &err));
  TEST_ASSERT_NOT_NULL(watch);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(watch, hreq, sizeof(hreq)));
  grpc_message *wmsg = nullptr;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(watch, &wmsg));
  TEST_ASSERT_NOT_NULL(wmsg);
  TEST_ASSERT_EQUAL_UINT8(GRPC_HEALTH_SERVING, wmsg->data[1]);
  grpc_message_free(wmsg);

  /* grpc_health_set_status() is safe from any thread */
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_health_set_status(server, "demo.Echo", GRPC_HEALTH_NOT_SERVING));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(watch, &wmsg));
  TEST_ASSERT_NOT_NULL(wmsg);
  TEST_ASSERT_EQUAL_UINT8(GRPC_HEALTH_NOT_SERVING, wmsg->data[1]);
  grpc_message_free(wmsg);
  fx_stream_cancel(watch);

  /* 3. Reflection v1: list_services (field 7), file_by_filename (field 3), file_containing_symbol (field 4) */
  grpc_client_stream *ref =
      fx_stream(grpc_client_stream_open(client, GRPC_REFLECTION_V1_PATH, nullptr, 0, 3000, &err));
  TEST_ASSERT_NOT_NULL(ref);

  /* list_services = "" -> tag (7<<3)|2 = 0x3A, len 0 */
  const uint8_t req_list[] = {0x3A, 0x00};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(ref, req_list, sizeof(req_list)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(ref, &wmsg));
  TEST_ASSERT_NOT_NULL(wmsg);
  TEST_ASSERT_TRUE(wmsg->len > 0);
  grpc_message_free(wmsg);

  /* file_by_filename = "demo.proto" -> tag (3<<3)|2 = 0x1A */
  const uint8_t req_file[] = {0x1A, 10, 'd', 'e', 'm', 'o', '.', 'p', 'r', 'o', 't', 'o'};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(ref, req_file, sizeof(req_file)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(ref, &wmsg));
  TEST_ASSERT_NOT_NULL(wmsg);
  TEST_ASSERT_TRUE(wmsg->len > 0);
  grpc_message_free(wmsg);

  /* file_containing_symbol = "demo.Echo" -> tag (4<<3)|2 = 0x22 */
  const uint8_t req_sym[] = {0x22, 9, 'd', 'e', 'm', 'o', '.', 'E', 'c', 'h', 'o'};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(ref, req_sym, sizeof(req_sym)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(ref, &wmsg));
  TEST_ASSERT_NOT_NULL(wmsg);
  TEST_ASSERT_TRUE(wmsg->len > 0);
  grpc_message_free(wmsg);

  /* lookups: a key answers itself and the names inside it ("demo.Echo" answers
   * "demo.Echo.Say"), but not a prefix that is not a name boundary or an empty query; misses get
   * an error_response. reflection_reply_field() is the field after the echoed request:
   * 0x22 = file_descriptor_response, 0x3A = error_response. */
  const struct {
    const char *symbol;
    uint8_t expect;
  } lookups[] = {
      {"nope", 0x3A}, {"demo.Echo.Say", 0x22}, {"demo", 0x3A}, {"demo.Ech", 0x3A},
      {"", 0x3A},     {"demo.EchoX", 0x3A},
  };
  for (size_t i = 0; i < sizeof(lookups) / sizeof(lookups[0]); i++) {
    uint8_t req[64];
    size_t slen = strlen(lookups[i].symbol);
    req[0] = 0x22; /* file_containing_symbol */
    req[1] = (uint8_t)slen;
    memcpy(req + 2, lookups[i].symbol, slen);
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(ref, req, slen + 2));
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(ref, &wmsg));
    TEST_ASSERT_NOT_NULL(wmsg);
    TEST_ASSERT_EQUAL_HEX8_MESSAGE(lookups[i].expect, reflection_reply_field(wmsg, slen + 2),
                                   lookups[i].symbol);
    grpc_message_free(wmsg);
  }

  TEST_ASSERT_EQUAL_INT(GRPC_OK, fx_stream_finish(ref, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  fx_client_free(client);
  fx_server_free(server);
}

/* ---- Integration Test 3: Poll-driven events, Keepalive/Idle, Max Conns ------ */

typedef struct poll_client_arg {
  uint16_t port;
  atomic_int ok;
} poll_client_arg;

static poll_client_arg g_poll_carg;

static void *run_poll_client(void *arg) {
  poll_client_arg *a = arg;
  grpc_err err = GRPC_OK;
  grpc_client *c = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = a->port, .timeout_ms = 2000}, &err);
  if (c == nullptr)
    return nullptr;
  grpc_client_response resp = {0};
  if (grpc_client_unary(c, "/poll.Svc/Echo", "ev", 2, nullptr, 0, 0, &resp) == GRPC_OK &&
      resp.status == GRPC_STATUS_OK && resp.message != nullptr && resp.message->len == 2) {
    atomic_store(&a->ok, 1);
  }
  grpc_client_response_free(&resp);
  grpc_client_free(c);
  return nullptr;
}

#ifndef _WIN32
/* Plain TCP connection to 127.0.0.1:port (asserted). */
static int raw_connect4(uint16_t port) {
  int fd = fx_fd(socket(AF_INET, SOCK_STREAM, 0));
  struct sockaddr_in sin = {0};
  sin.sin_family = AF_INET;
  sin.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &sin.sin_addr);
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, connect(fd, (struct sockaddr *)&sin, sizeof(sin)),
                                strerror(errno));
  return fd;
}

/* True once the peer closed the connection (EOF or reset), without blocking. */
static bool fd_peer_closed(int fd) {
  char b;
  ssize_t n = recv(fd, &b, 1, MSG_DONTWAIT);
  return n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR);
}
#endif

void test_integration_poll_events_and_maintenance(void) {
  grpc_server *server = fx_server(grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .max_connections = 2,
      .keepalive_time_ms = 50,
      .keepalive_timeout_ms = 500,
      .max_connection_idle_ms = 120,
      .log_level = GRPC_LOG_OFF,
  }));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_register_path(server, "/poll.Svc/Echo", nullptr));

  g_poll_carg.port = grpc_server_port(server);
  atomic_store(&g_poll_carg.ok, 0);
  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_poll_client, &g_poll_carg));

  int replied = 0;
  for (int iter = 0; iter < 50 && !replied; iter++) {
    int n = grpc_server_poll(server, 50);
    if (n > 0) {
      size_t count = 0;
      const grpc_event *evs = grpc_server_events(server, &count);
      for (size_t i = 0; i < count; i++) {
        if (evs[i].type == GRPC_EVENT_MESSAGE) {
          (void)grpc_call_reply_data(evs[i].call, evs[i].message->data, evs[i].message->len);
          replied = 1;
        }
      }
      grpc_server_flush(server);
    }
  }
  pthread_join(th, nullptr);
  TEST_ASSERT_EQUAL_INT(1, replied);
  TEST_ASSERT_EQUAL_INT(1, atomic_load(&g_poll_carg.ok));

  /* Let the server see the poll client's close, so both slots are free again */
  ASSERT_POLL_EVENTUALLY(server, srv_stats(server).active_connections == 0);

  /* Fill max_connections=2 with two idle clients */
  grpc_err cerr = GRPC_OK;
  grpc_client *idle_c = fx_client(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(server)}, &cerr));
  TEST_ASSERT_NOT_NULL(idle_c);
  grpc_client *c2 = fx_client(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(server)}, &cerr));
  TEST_ASSERT_NOT_NULL(c2);
  ASSERT_POLL_EVENTUALLY(server, srv_stats(server).active_connections == 2);
  uint64_t total_before = srv_stats(server).total_connections;

#ifndef _WIN32
  /* A 3rd connection is accepted and closed at once (reject_connection), never counted */
  int c3 = raw_connect4(grpc_server_port(server));
  ASSERT_POLL_EVENTUALLY(server, fd_peer_closed(c3));
  TEST_ASSERT_EQUAL_UINT64(total_before, srv_stats(server).total_connections);
  fx_close(c3);
#else
  grpc_client *c3 = fx_client(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(server)}, &cerr));
  for (int i = 0; i < 6; i++)
    (void)grpc_server_poll(server, 60);
  TEST_ASSERT_EQUAL_UINT64(total_before, srv_stats(server).total_connections);
  fx_client_free(c3);
#endif

  /* Maintenance timer (250 ms tick): idle clients never answer the keepalive PING and have no
   * streams, so the server sends GOAWAY and then closes both connections */
  ASSERT_POLL_EVENTUALLY(server, srv_stats(server).active_connections == 0);
  fx_client_free(c2);
  fx_client_free(idle_c);

  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_drain(server, 100));
  fx_server_free(server);
}

/* ---- Integration Test 4: gRPC-Web, gRPC-Web-Text, CORS OPTIONS, 415, IPv6 --- */

#ifndef _WIN32
static void raw_h2_send_all(int fd, const uint8_t *data, size_t len) {
  size_t off = 0;
  while (off < len) {
    ssize_t n = send(fd, (const char *)(data + off), len - off, 0);
    if (n <= 0)
      break;
    off += (size_t)n;
  }
}

typedef struct raw_h2_payload {
  const uint8_t *data;
  size_t len;
  size_t off;
} raw_h2_payload;

static ssize_t raw_h2_read_cb([[maybe_unused]] nghttp2_session *s, [[maybe_unused]] int32_t sid,
                              uint8_t *buf, size_t len, uint32_t *flags, nghttp2_data_source *src,
                              [[maybe_unused]] void *ud) {
  raw_h2_payload *p = src->ptr;
  size_t rem = p->len - p->off;
  size_t n = rem < len ? rem : len;
  if (n > 0) {
    memcpy(buf, p->data + p->off, n);
    p->off += n;
  }
  if (p->off == p->len)
    *flags = NGHTTP2_DATA_FLAG_EOF;
  return (ssize_t)n;
}

static ssize_t raw_h2_chunked_read_cb([[maybe_unused]] nghttp2_session *s,
                                      [[maybe_unused]] int32_t stream_id, uint8_t *buf, size_t len,
                                      uint32_t *flags, nghttp2_data_source *src,
                                      [[maybe_unused]] void *ud) {
  raw_h2_payload *p = src->ptr;
  size_t rem = p->len - p->off;
  /* Cap first chunk at 200 bytes so server parses msg1 (155B) and leaves 45B at recv_start=155 */
  size_t cap = p->off == 0 ? 200 : rem;
  size_t n = rem < cap ? rem : cap;
  if (n > len)
    n = len;
  if (n > 0) {
    memcpy(buf, p->data + p->off, n);
    p->off += n;
  }
  if (p->off == p->len)
    *flags = NGHTTP2_DATA_FLAG_EOF;
  return (ssize_t)n;
}

/* What a raw HTTP/2 client session received, per stream and per connection. */
typedef struct h2_stream_rec {
  int32_t sid;
  int http_status; /* :status, 0 when none */
  int grpc_status; /* grpc-status in a HEADERS frame, -1 when none */
  char grpc_encoding[16];
  bool closed;
  bool rst;
  uint32_t rst_code;
  size_t body_total;
  uint8_t body[1024]; /* first bytes of the response body */
  size_t body_len;
} h2_stream_rec;

typedef struct h2_rec {
  h2_stream_rec streams[20];
  size_t nb_streams;
  size_t nb_closed;
  int goaways;
  uint32_t goaway_code;
  int32_t goaway_last_ids[4]; /* last_stream_id of the first GOAWAYs */
  int ping_acks;
} h2_rec;

static h2_stream_rec *h2_rec_find(h2_rec *r, int32_t sid) {
  for (size_t i = 0; i < r->nb_streams; i++) {
    if (r->streams[i].sid == sid)
      return &r->streams[i];
  }
  return nullptr;
}

/* Registers a submitted request so its response is recorded. */
static int32_t h2_track(h2_rec *r, int32_t sid) {
  TEST_ASSERT_TRUE_MESSAGE(sid > 0, "nghttp2_submit_request failed");
  TEST_ASSERT_TRUE(r->nb_streams < sizeof(r->streams) / sizeof(r->streams[0]));
  r->streams[r->nb_streams++] = (h2_stream_rec){.sid = sid, .grpc_status = -1};
  return sid;
}

static int h2_rec_on_header([[maybe_unused]] nghttp2_session *s, const nghttp2_frame *f,
                            const uint8_t *name, size_t namelen, const uint8_t *value,
                            size_t valuelen, [[maybe_unused]] uint8_t flags, void *ud) {
  h2_stream_rec *st = h2_rec_find(ud, f->hd.stream_id);
  if (f->hd.type != NGHTTP2_HEADERS || st == nullptr)
    return 0;
  char v[32];
  size_t vl = valuelen < sizeof(v) - 1 ? valuelen : sizeof(v) - 1;
  memcpy(v, value, vl);
  v[vl] = '\0';
  if (namelen == 7 && memcmp(name, ":status", 7) == 0)
    st->http_status = atoi(v);
  else if (namelen == 11 && memcmp(name, "grpc-status", 11) == 0)
    st->grpc_status = atoi(v);
  else if (namelen == 13 && memcmp(name, "grpc-encoding", 13) == 0) {
    size_t el = vl < sizeof(st->grpc_encoding) - 1 ? vl : sizeof(st->grpc_encoding) - 1;
    memcpy(st->grpc_encoding, v, el);
    st->grpc_encoding[el] = '\0';
  }
  return 0;
}

static int h2_rec_on_data([[maybe_unused]] nghttp2_session *s, [[maybe_unused]] uint8_t flags,
                          int32_t sid, const uint8_t *data, size_t len, void *ud) {
  h2_stream_rec *st = h2_rec_find(ud, sid);
  if (st == nullptr)
    return 0;
  st->body_total += len;
  size_t room = sizeof(st->body) - st->body_len;
  size_t n = len < room ? len : room;
  memcpy(st->body + st->body_len, data, n);
  st->body_len += n;
  return 0;
}

static int h2_rec_on_frame([[maybe_unused]] nghttp2_session *s, const nghttp2_frame *f, void *ud) {
  h2_rec *r = ud;
  if (f->hd.type == NGHTTP2_RST_STREAM) {
    h2_stream_rec *st = h2_rec_find(r, f->hd.stream_id);
    if (st != nullptr) {
      st->rst = true;
      st->rst_code = f->rst_stream.error_code;
    }
  } else if (f->hd.type == NGHTTP2_GOAWAY) {
    if (r->goaways < 4)
      r->goaway_last_ids[r->goaways] = f->goaway.last_stream_id;
    r->goaways++;
    r->goaway_code = f->goaway.error_code;
  } else if (f->hd.type == NGHTTP2_PING && (f->hd.flags & NGHTTP2_FLAG_ACK)) {
    r->ping_acks++;
  }
  return 0;
}

static int h2_rec_on_close([[maybe_unused]] nghttp2_session *s, int32_t sid,
                           [[maybe_unused]] uint32_t error_code, void *ud) {
  h2_rec *r = ud;
  h2_stream_rec *st = h2_rec_find(r, sid);
  if (st != nullptr && !st->closed) {
    st->closed = true;
    r->nb_closed++;
  }
  return 0;
}

/* Client session recording into r, tracked by the fixture. */
static nghttp2_session *h2_client_session(h2_rec *r) {
  nghttp2_session_callbacks *cbs = nullptr;
  TEST_ASSERT_EQUAL_INT(0, nghttp2_session_callbacks_new(&cbs));
  nghttp2_session_callbacks_set_on_header_callback(cbs, h2_rec_on_header);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, h2_rec_on_data);
  nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, h2_rec_on_frame);
  nghttp2_session_callbacks_set_on_stream_close_callback(cbs, h2_rec_on_close);
  nghttp2_session *sess = nullptr;
  int rc = nghttp2_session_client_new(&sess, cbs, r);
  nghttp2_session_callbacks_del(cbs);
  TEST_ASSERT_EQUAL_INT(0, rc);
  fx_put(g_fx.sessions, sess);
  return sess;
}

static void h2_flush(nghttp2_session *sess, int fd) {
  for (;;) {
    const uint8_t *out;
    ssize_t n = nghttp2_session_mem_send(sess, &out);
    if (n <= 0)
      break;
    raw_h2_send_all(fd, out, (size_t)n);
  }
}

/* Feeds what arrives within timeout_ms to the session and flushes its replies. Returns false
 * once the connection is closed. */
static bool h2_pump(nghttp2_session *sess, int fd, int timeout_ms) {
  struct pollfd pfd = {.fd = fd, .events = POLLIN};
  int pr = poll(&pfd, 1, timeout_ms);
  if (pr <= 0)
    return pr == 0 || errno == EINTR;
  uint8_t buf[32768];
  ssize_t n = recv(fd, buf, sizeof(buf), 0);
  if (n < 0)
    return errno == EINTR || errno == EAGAIN;
  if (n == 0 || nghttp2_session_mem_recv(sess, buf, (size_t)n) < 0)
    return false;
  h2_flush(sess, fd);
  return true;
}

#define H2_PUMP_UNTIL(sess, fd, cond, ms)                                \
  do {                                                                   \
    const uint64_t hp_end_ = now_ms() + (ms);                            \
    while (!(cond) && now_ms() < hp_end_ && h2_pump((sess), (fd), 10)) { \
    }                                                                    \
  } while (0)

/* The gRPC-Web response body as frames: base64 chunks (each padded) decoded for -text. */
static size_t web_body_decode(const h2_stream_rec *st, bool text, uint8_t *out, size_t cap) {
  if (!text) {
    size_t n = st->body_len < cap ? st->body_len : cap;
    memcpy(out, st->body, n);
    return n;
  }
  size_t len = 0, seg = 0;
  for (size_t i = 0; i + 4 <= st->body_len || seg < st->body_len; i += 4) {
    bool end = i + 4 >= st->body_len || memchr(st->body + i, '=', 4) != nullptr;
    if (!end)
      continue;
    size_t seg_end = i + 4 < st->body_len ? i + 4 : st->body_len;
    size_t dl = 0;
    uint8_t *dec = grpc_metadata_decode_bin((const char *)st->body + seg, seg_end - seg, &dl);
    if (dec != nullptr) {
      size_t n = dl < cap - len ? dl : cap - len;
      memcpy(out + len, dec, n);
      len += n;
      grpc_free(dec);
    }
    seg = seg_end;
  }
  return len;
}

/* grpc-status from a gRPC-Web trailer frame (flag 0x80) in the body, or -1. When msg is given,
 * the first data frame must equal it. */
static int web_grpc_status(const h2_stream_rec *st, bool text, const char *msg) {
  uint8_t buf[1024];
  size_t len = web_body_decode(st, text, buf, sizeof(buf));
  bool msg_ok = msg == nullptr;
  size_t off = 0;
  while (off + 5 <= len) {
    uint8_t flag = buf[off];
    uint32_t mlen = ((uint32_t)buf[off + 1] << 24) | ((uint32_t)buf[off + 2] << 16) |
                    ((uint32_t)buf[off + 3] << 8) | buf[off + 4];
    if (mlen > len - off - 5)
      break;
    const char *payload = (const char *)buf + off + 5;
    if (flag & 0x80) {
      static const char key[] = "grpc-status:";
      for (size_t i = 0; i + sizeof(key) - 1 <= mlen; i++) {
        if (memcmp(payload + i, key, sizeof(key) - 1) == 0) {
          size_t j = i + sizeof(key) - 1;
          while (j < mlen && payload[j] == ' ')
            j++;
          int v = 0;
          bool any = false;
          while (j < mlen && payload[j] >= '0' && payload[j] <= '9') {
            v = v * 10 + (payload[j++] - '0');
            any = true;
          }
          return any && msg_ok ? v : -1;
        }
      }
      return -1;
    }
    if (!msg_ok && mlen == strlen(msg) && memcmp(payload, msg, mlen) == 0)
      msg_ok = true;
    off += 5 + mlen;
  }
  return -1;
}

static const h2_stream_rec *h2_get(h2_rec *r, int32_t sid) {
  const h2_stream_rec *st = h2_rec_find(r, sid);
  TEST_ASSERT_NOT_NULL(st);
  TEST_ASSERT_TRUE_MESSAGE(st->closed, "stream not closed");
  return st;
}

/* Answered with a complete gRPC response (HTTP 200, no reset) whose grpc-status is `status`. */
static void assert_grpc_stream(h2_rec *r, int32_t sid, int status) {
  const h2_stream_rec *st = h2_get(r, sid);
  TEST_ASSERT_FALSE(st->rst);
  TEST_ASSERT_EQUAL_INT(200, st->http_status);
  TEST_ASSERT_EQUAL_INT(status, st->grpc_status);
}

/* Same for gRPC-Web, whose status travels in the body. */
static void assert_web_stream(h2_rec *r, int32_t sid, bool text, int status, const char *msg) {
  const h2_stream_rec *st = h2_get(r, sid);
  TEST_ASSERT_FALSE(st->rst);
  TEST_ASSERT_EQUAL_INT(200, st->http_status);
  TEST_ASSERT_EQUAL_INT(status, web_grpc_status(st, text, msg));
}

#define NV(n, v) \
  {(uint8_t *)(n), (uint8_t *)(v), sizeof(n) - 1, sizeof(v) - 1, NGHTTP2_NV_FLAG_NONE}

static h2_rec g_raw_rec;
#endif

static void run_raw_h2_requests(uint16_t port) {
#ifndef _WIN32
  int fd = fx_fd(socket(AF_INET6, SOCK_STREAM, 0));
  struct sockaddr_in6 sin6 = {0};
  sin6.sin6_family = AF_INET6;
  sin6.sin6_port = htons(port);
  inet_pton(AF_INET6, "::1", &sin6.sin6_addr);
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, connect(fd, (struct sockaddr *)&sin6, sizeof(sin6)),
                                strerror(errno));

  h2_rec *rec = &g_raw_rec;
  memset(rec, 0, sizeof(*rec));
  nghttp2_session *sess = h2_client_session(rec);
  nghttp2_submit_settings(sess, NGHTTP2_FLAG_NONE, nullptr, 0);

  /* 1. CORS OPTIONS preflight */
  nghttp2_nv opt_hdrs[] = {
      NV(":method", "OPTIONS"),
      NV(":scheme", "http"),
      NV(":path", "/web.Svc/Echo"),
      NV(":authority", "localhost"),
  };
  int32_t s_opt =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, opt_hdrs, 4, nullptr, nullptr));

  /* 2. Unsupported content-type (415) */
  nghttp2_nv bad_ct[] = {
      NV(":method", "POST"),
      NV(":scheme", "http"),
      NV(":path", "/web.Svc/Echo"),
      NV(":authority", "localhost"),
      NV("content-type", "text/plain"),
  };
  int32_t s_415 = h2_track(rec, nghttp2_submit_request(sess, nullptr, bad_ct, 5, nullptr, nullptr));

  /* 3. gRPC-Web binary request */
  const uint8_t web_frame[] = {0x00, 0x00, 0x00, 0x00, 0x02, 'h', 'i'};
  raw_h2_payload p_web = {.data = web_frame, .len = sizeof(web_frame), .off = 0};
  nghttp2_data_provider dp_web = {.source.ptr = &p_web, .read_callback = raw_h2_read_cb};
  nghttp2_nv web_hdrs[] = {
      NV(":method", "POST"),
      NV(":scheme", "http"),
      NV(":path", "/web.Svc/Echo"),
      NV(":authority", "localhost"),
      NV("content-type", "application/grpc-web+proto"),
  };
  int32_t s_web =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, web_hdrs, 5, &dp_web, nullptr));

  /* 4. gRPC-Web-Text (Base64) request */
  size_t b64_len = 0;
  char *b64 = fx_mem(grpc_metadata_encode_bin(web_frame, sizeof(web_frame), &b64_len));
  raw_h2_payload p_txt = {.data = (const uint8_t *)b64, .len = b64_len, .off = 0};
  nghttp2_data_provider dp_txt = {.source.ptr = &p_txt, .read_callback = raw_h2_read_cb};
  nghttp2_nv txt_hdrs[] = {
      NV(":method", "POST"),
      NV(":scheme", "http"),
      NV(":path", "/web.Svc/Echo"),
      NV(":authority", "localhost"),
      NV("content-type", "application/grpc-web-text"),
  };
  int32_t s_txt =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, txt_hdrs, 5, &dp_txt, nullptr));

  /* 5. gRPC-Web trailers-only (handler calls finish without send) */
  raw_h2_payload p_nobody = {.data = web_frame, .len = sizeof(web_frame), .off = 0};
  nghttp2_data_provider dp_nobody = {.source.ptr = &p_nobody, .read_callback = raw_h2_read_cb};
  nghttp2_nv nobody_hdrs[] = {
      NV(":method", "POST"),
      NV(":scheme", "http"),
      NV(":path", "/web.Svc/NoBody"),
      NV(":authority", "localhost"),
      NV("content-type", "application/grpc-web"),
  };
  int32_t s_nobody =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, nobody_hdrs, 5, &dp_nobody, nullptr));

  /* 6. gRPC-Web-Text invalid base64 -> INVALID_ARGUMENT */
  const uint8_t bad_b64[] = "!!!not-base64!!!";
  raw_h2_payload p_badb64 = {.data = bad_b64, .len = sizeof(bad_b64) - 1, .off = 0};
  nghttp2_data_provider dp_badb64 = {.source.ptr = &p_badb64, .read_callback = raw_h2_read_cb};
  int32_t s_badb64 =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, txt_hdrs, 5, &dp_badb64, nullptr));

  /* 7. gRPC-Web-Text truncated message -> DATA_LOSS */
  const uint8_t trunc_frame[] = {0x00, 0x00, 0x00, 0x00, 0x10, 'a'};
  size_t tb64_len = 0;
  char *tb64 = fx_mem(grpc_metadata_encode_bin(trunc_frame, sizeof(trunc_frame), &tb64_len));
  raw_h2_payload p_tb64 = {.data = (const uint8_t *)tb64, .len = tb64_len, .off = 0};
  nghttp2_data_provider dp_tb64 = {.source.ptr = &p_tb64, .read_callback = raw_h2_read_cb};
  int32_t s_tb64 =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, txt_hdrs, 5, &dp_tb64, nullptr));

  /* 8. Raw gRPC truncated message + grpc-encoding: identity -> DATA_LOSS */
  raw_h2_payload p_trunc = {.data = trunc_frame, .len = sizeof(trunc_frame), .off = 0};
  nghttp2_data_provider dp_trunc = {.source.ptr = &p_trunc, .read_callback = raw_h2_read_cb};
  nghttp2_nv id_hdrs[] = {
      NV(":method", "POST"),
      NV(":scheme", "http"),
      NV(":path", "/web.Svc/Echo"),
      NV(":authority", "localhost"),
      NV("content-type", "application/grpc"),
      NV("te", "trailers"),
      NV("grpc-encoding", "identity"),
  };
  int32_t s_trunc =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, id_hdrs, 7, &dp_trunc, nullptr));

  /* 9 & 10. Oversized gzip-compressed message (both gRPC and gRPC-Web-Text) -> RESOURCE_EXHAUSTED */
  char big_plain[2048];
  memset(big_plain, 'Z', sizeof(big_plain));
  uint8_t *zbuf = nullptr;
  size_t zlen = 0;
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_compress(GRPC_COMPRESS_GZIP, GRPC_COMPRESSION_LEVEL_DEFAULT,
                                      (const uint8_t *)big_plain, sizeof(big_plain), &zbuf, &zlen));
  fx_mem(zbuf);
  uint8_t *zframe = fx_mem(grpc_malloc(5 + zlen));
  zframe[0] = 0x01;
  uint32_t zlen_be = htonl((uint32_t)zlen);
  memcpy(zframe + 1, &zlen_be, 4);
  memcpy(zframe + 5, zbuf, zlen);
  fx_free(zbuf);

  raw_h2_payload p_zraw = {.data = zframe, .len = 5 + zlen, .off = 0};
  nghttp2_data_provider dp_zraw = {.source.ptr = &p_zraw, .read_callback = raw_h2_read_cb};
  nghttp2_nv gz_hdrs[] = {
      NV(":method", "POST"),
      NV(":scheme", "http"),
      NV(":path", "/web.Svc/Echo"),
      NV(":authority", "localhost"),
      NV("content-type", "application/grpc"),
      NV("te", "trailers"),
      NV("grpc-encoding", "gzip"),
  };
  int32_t s_zraw =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, gz_hdrs, 7, &dp_zraw, nullptr));

  size_t zb64_len = 0;
  char *zb64 = fx_mem(grpc_metadata_encode_bin(zframe, 5 + zlen, &zb64_len));
  raw_h2_payload p_zb64 = {.data = (const uint8_t *)zb64, .len = zb64_len, .off = 0};
  nghttp2_data_provider dp_zb64 = {.source.ptr = &p_zb64, .read_callback = raw_h2_read_cb};
  nghttp2_nv gz_txt_hdrs[] = {
      NV(":method", "POST"),
      NV(":scheme", "http"),
      NV(":path", "/web.Svc/Echo"),
      NV(":authority", "localhost"),
      NV("content-type", "application/grpc-web-text"),
      NV("grpc-encoding", "gzip"),
  };
  int32_t s_zb64 =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, gz_txt_hdrs, 6, &dp_zb64, nullptr));

  /* 10b. grpc-accept-encoding with a space-padded token list not naming the server's gzip (a
   * trailing space at the end of the value would make nghttp2 reset the stream as malformed) */
  const uint8_t gz_req_frame[] = {0x00, 0x00, 0x00, 0x00, 0x04, 'g', 'z', 'i', 'p'};
  raw_h2_payload p_ae = {.data = gz_req_frame, .len = sizeof(gz_req_frame), .off = 0};
  nghttp2_data_provider dp_ae = {.source.ptr = &p_ae, .read_callback = raw_h2_read_cb};
  nghttp2_nv ae_hdrs[] = {
      NV(":method", "POST"),
      NV(":scheme", "http"),
      NV(":path", "/web.Svc/Codec"),
      NV(":authority", "localhost"),
      NV("content-type", "application/grpc"),
      NV("te", "trailers"),
      NV("grpc-accept-encoding", "deflate , identity"),
  };
  int32_t s_ae = h2_track(rec, nghttp2_submit_request(sess, nullptr, ae_hdrs, 7, &dp_ae, nullptr));

  /* 10c. Headers-only END_STREAM (no DATA) on valid gRPC path -> half_closed + call_queue */
  nghttp2_nv empty_hdrs[] = {
      NV(":method", "POST"),
      NV(":scheme", "http"),
      NV(":path", "/web.Svc/NoBody"),
      NV(":authority", "localhost"),
      NV("content-type", "application/grpc"),
      NV("te", "trailers"),
  };
  int32_t s_empty =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, empty_hdrs, 6, nullptr, nullptr));

  /* 10d. Invalid compressed payload (cflag=1, bad gzip) + trailing 2 bytes: the corrupt message
   * is rejected with INTERNAL before the truncation matters */
  const uint8_t bad_gz_frame[] = {0x01, 0x00, 0x00, 0x00, 0x03, 'b', 'a', 'd', 0x00, 0x00};
  raw_h2_payload p_bgz = {.data = bad_gz_frame, .len = sizeof(bad_gz_frame), .off = 0};
  nghttp2_data_provider dp_bgz = {.source.ptr = &p_bgz, .read_callback = raw_h2_read_cb};
  int32_t s_bgz =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, gz_hdrs, 7, &dp_bgz, nullptr));

  /* 10e. Two-chunk DATA stream to trigger recv_data memmove compaction (recv_start > 0) */
  uint8_t compact_buf[310];
  compact_buf[0] = 0;
  uint32_t m150 = htonl(150);
  memcpy(compact_buf + 1, &m150, 4);
  memset(compact_buf + 5, 'A', 150);
  compact_buf[155] = 0;
  memcpy(compact_buf + 156, &m150, 4);
  memset(compact_buf + 160, 'B', 150);
  raw_h2_payload p_cmp = {.data = compact_buf, .len = sizeof(compact_buf), .off = 0};
  nghttp2_data_provider dp_cmp = {.source.ptr = &p_cmp, .read_callback = raw_h2_chunked_read_cb};
  nghttp2_nv cmp_hdrs[] = {
      NV(":method", "POST"),
      NV(":scheme", "http"),
      NV(":path", "/web.Svc/Echo"),
      NV(":authority", "localhost"),
      NV("content-type", "application/grpc"),
      NV("te", "trailers"),
  };
  int32_t s_cmp =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, cmp_hdrs, 6, &dp_cmp, nullptr));

  h2_flush(sess, fd);
  H2_PUMP_UNTIL(sess, fd, rec->nb_closed == rec->nb_streams, WAIT_DEADLINE_MS);
  TEST_ASSERT_EQUAL_UINT64(rec->nb_streams, rec->nb_closed);

  /* Every stream got the answer the server is specified to give */
  const h2_stream_rec *st_opt = h2_get(rec, s_opt);
  TEST_ASSERT_FALSE(st_opt->rst);
  TEST_ASSERT_EQUAL_INT(204, st_opt->http_status);
  const h2_stream_rec *st_415 = h2_get(rec, s_415);
  TEST_ASSERT_FALSE(st_415->rst);
  TEST_ASSERT_EQUAL_INT(415, st_415->http_status);
  assert_web_stream(rec, s_web, false, GRPC_STATUS_OK, "hi");
  assert_web_stream(rec, s_txt, true, GRPC_STATUS_OK, "hi");
  assert_web_stream(rec, s_nobody, false, GRPC_STATUS_OK, nullptr);
  /* characters outside the base64 alphabet are rejected before decoding */
  const h2_stream_rec *st_badb64 = h2_get(rec, s_badb64);
  TEST_ASSERT_FALSE(st_badb64->rst);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_INVALID_ARGUMENT, web_grpc_status(st_badb64, true, nullptr));
  assert_web_stream(rec, s_tb64, true, GRPC_STATUS_DATA_LOSS, nullptr);
  assert_grpc_stream(rec, s_trunc, GRPC_STATUS_DATA_LOSS);
  assert_grpc_stream(rec, s_zraw, GRPC_STATUS_RESOURCE_EXHAUSTED);
  assert_web_stream(rec, s_zb64, true, GRPC_STATUS_RESOURCE_EXHAUSTED, nullptr);
  /* gzip is not in the client's grpc-accept-encoding, so the reply is not gzip-encoded */
  assert_grpc_stream(rec, s_ae, GRPC_STATUS_OK);
  TEST_ASSERT_TRUE(strcmp(h2_get(rec, s_ae)->grpc_encoding, "gzip") != 0);
  /* a unary method needs exactly one request message: none or two end with INTERNAL */
  assert_grpc_stream(rec, s_empty, GRPC_STATUS_INTERNAL);
  assert_grpc_stream(rec, s_bgz, GRPC_STATUS_INTERNAL);
  assert_grpc_stream(rec, s_cmp, GRPC_STATUS_INTERNAL);

  /* 11. Too many headers (> CONFIG_STREAM_MAX_HEADERS): only that stream is reset */
  nghttp2_nv many_hdrs[265];
  many_hdrs[0] = (nghttp2_nv)NV(":method", "POST");
  many_hdrs[1] = (nghttp2_nv)NV(":scheme", "http");
  many_hdrs[2] = (nghttp2_nv)NV(":path", "/web.Svc/NoBody");
  many_hdrs[3] = (nghttp2_nv)NV(":authority", "localhost");
  many_hdrs[4] = (nghttp2_nv)NV("content-type", "application/grpc");
  for (int h = 5; h < 265; h++)
    many_hdrs[h] = (nghttp2_nv)NV("x-h", "1");
  int32_t s_many =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, many_hdrs, 265, nullptr, nullptr));
  h2_flush(sess, fd);
  H2_PUMP_UNTIL(sess, fd, h2_rec_find(rec, s_many)->closed, WAIT_DEADLINE_MS);
  const h2_stream_rec *st_many = h2_get(rec, s_many);
  TEST_ASSERT_TRUE(st_many->rst);
  TEST_ASSERT_EQUAL_UINT32(NGHTTP2_INTERNAL_ERROR, st_many->rst_code);

  /* ... while the connection survives: the next request on it is served */
  raw_h2_payload p_after = {.data = web_frame, .len = sizeof(web_frame), .off = 0};
  nghttp2_data_provider dp_after = {.source.ptr = &p_after, .read_callback = raw_h2_read_cb};
  int32_t s_after =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, nobody_hdrs, 5, &dp_after, nullptr));
  h2_flush(sess, fd);
  H2_PUMP_UNTIL(sess, fd, h2_rec_find(rec, s_after)->closed, WAIT_DEADLINE_MS);
  assert_web_stream(rec, s_after, false, GRPC_STATUS_OK, nullptr);
  TEST_ASSERT_EQUAL_INT(0, rec->goaways);

  /* 12. Rapid PINGs: more than max_ping_strikes=1 inside min_ping_interval_ms earn a GOAWAY with
   * ENHANCE_YOUR_CALM */
  static const uint8_t ping_bytes[8] = {1, 2, 3, 4, 5, 6, 7, 8};
  for (int i = 0; i < 4; i++)
    nghttp2_submit_ping(sess, NGHTTP2_FLAG_NONE, ping_bytes);
  h2_flush(sess, fd);
  H2_PUMP_UNTIL(sess, fd, rec->goaways > 0, WAIT_DEADLINE_MS);
  TEST_ASSERT_TRUE_MESSAGE(rec->goaways > 0, "no GOAWAY after excess PINGs");
  TEST_ASSERT_EQUAL_UINT32(NGHTTP2_ENHANCE_YOUR_CALM, rec->goaway_code);

  fx_free(b64);
  fx_free(tb64);
  fx_free(zframe);
  fx_free(zb64);
  fx_session_del(sess);
  fx_close(fd);
#else
  (void)port;
#endif
}

static void test_on_done_cb([[maybe_unused]] grpc_call *call, [[maybe_unused]] grpc_status status,
                            [[maybe_unused]] uint64_t duration_ms, [[maybe_unused]] void *ud) {
  atomic_fetch_add(&g_on_done_calls, 1);
}

static void test_cancel_cb([[maybe_unused]] grpc_call *call, [[maybe_unused]] void *ud) {
  atomic_fetch_add(&g_cancel_cb_fired, 1);
}

static void cancel_watch_on_start(grpc_call *call, [[maybe_unused]] void *ud) {
  grpc_call_on_cancel(call, test_cancel_cb, nullptr);
  atomic_store(&g_hold_started, 1);
}

static void no_body_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                            [[maybe_unused]] void *userdata) {
  (void)grpc_call_finish(call, GRPC_STATUS_OK, "empty");
  (void)grpc_call_finish(call, GRPC_STATUS_OK, "again");
}

static void codec_resp_handler(grpc_call *call, const grpc_message *req,
                               [[maybe_unused]] void *userdata) {
  if (req->len == 2 && memcmp(req->data, "id", 2) == 0) {
    (void)grpc_call_set_compression(call, GRPC_COMPRESS_IDENTITY, GRPC_COMPRESSION_LEVEL_DEFAULT);
  } else if (req->len == 4 && memcmp(req->data, "gzip", 4) == 0) {
    (void)grpc_call_set_compression(call, GRPC_COMPRESS_GZIP, GRPC_COMPRESSION_LEVEL_DEFAULT);
  }
#if defined(GRPC_ENABLE_ZSTD)
  else if (req->len == 4 && memcmp(req->data, "zstd", 4) == 0) {
    (void)grpc_call_set_compression(call, GRPC_COMPRESS_ZSTD, GRPC_COMPRESSION_LEVEL_DEFAULT);
  }
#endif
  (void)grpc_call_reply_data(call, req->data, req->len);
}

void test_integration_grpc_web_cors_ipv6_and_client_edge_cases(void) {
  grpc_server *server = fx_server(grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv6_addr = "::1",
      .max_message_size = 1024,
      .max_ping_strikes = 1,
      .min_ping_interval_ms = 5000,
      .max_connection_age_ms = 5000,
      .log_level = GRPC_LOG_OFF,
  }));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(server, "/web.Svc/Echo", large_echo_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle(server, "/web.Svc/Slow", slow_deadline_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(server, "/web.Svc/NoBody", no_body_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(server, "/web.Svc/Codec", codec_resp_handler, nullptr));

  /* Server edge-case validation & fallback set/disable (before the server thread starts: only
   * the server thread may configure a running server) */
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_register_path(nullptr, "/a/b", nullptr));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_set_fallback(
                   server, &(grpc_stream_handler){.on_message = no_body_handler}, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_set_fallback(server, nullptr, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_set_fallback(nullptr, nullptr, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_set_fallback(server, nullptr, nullptr));

  fx_start(server);
  uint16_t port = grpc_server_port(server);

  run_raw_h2_requests(port);

  /* Client over IPv6 target, codecs (deflate, zstd, snappy), max_message_size, timeout & errors */
  grpc_err err = GRPC_OK;
  TEST_ASSERT_NULL(grpc_client_new(nullptr, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  const char *bad_targets[] = {"no-port", "", "unix:", "[::1", "[::1]:0", "127.0.0.1:99999"};
  for (size_t i = 0; i < sizeof(bad_targets) / sizeof(bad_targets[0]); i++) {
    TEST_ASSERT_NULL(
        grpc_client_new(&(grpc_client_config){.targets = &bad_targets[i], .nb_targets = 1}, &err));
    TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  }

  /* Client TLS error paths: invalid CA/cert/key paths & TLS to plaintext server */
#if defined(GRPC_ENABLE_TLS)
  TEST_ASSERT_NULL(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1",
                            .port = port,
                            .tls = {.enable = true, .ca_file = "/nonexistent/ca.pem"}},
      &err));
  TEST_ASSERT_NULL(grpc_client_new(&(grpc_client_config){.host = "127.0.0.1",
                                                         .port = port,
                                                         .tls = {.enable = true,
                                                                 .cert_file = "/nonexistent/c.pem",
                                                                 .key_file = "/nonexistent/k.pem"}},
                                   &err));
  TEST_ASSERT_NULL(
      grpc_client_new(&(grpc_client_config){.host = "127.0.0.1",
                                            .port = port,
                                            .timeout_ms = 500,
                                            .tls = {.enable = true, .insecure_skip_verify = true}},
                      &err));
#endif

  char ep6[64];
  snprintf(ep6, sizeof(ep6), "[::1]:%u", port);
  const char *t6[] = {ep6};

  const grpc_compression_algorithm algs[] = {
      GRPC_COMPRESS_DEFLATE,
#if defined(GRPC_ENABLE_ZSTD)
      GRPC_COMPRESS_ZSTD,
#endif
#if defined(GRPC_ENABLE_SNAPPY)
      GRPC_COMPRESS_SNAPPY,
#endif
  };
  for (size_t i = 0; i < sizeof(algs) / sizeof(algs[0]); i++) {
    grpc_client *c = fx_client(grpc_client_new(
        &(grpc_client_config){
            .targets = t6,
            .nb_targets = 1,
            .compression = algs[i],
            .timeout_ms = 2000,
        },
        &err));
    TEST_ASSERT_NOT_NULL(c);
    grpc_client_response resp = {0};
    TEST_ASSERT_EQUAL_INT(
        GRPC_OK, grpc_client_unary(c, "/web.Svc/Echo", "codec-test", 10, nullptr, 0, 0, &resp));
    TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
    TEST_ASSERT_NOT_NULL(resp.message);
    TEST_ASSERT_EQUAL_MEMORY("codec-test", resp.message->data, 10);
    grpc_client_response_free(&resp);
    fx_client_free(c);
  }

  /* Client timeout (GRPC_ERR_TIMEOUT) & oversized request (RESOURCE_EXHAUSTED) */
  grpc_client *c = fx_client(grpc_client_new(
      &(grpc_client_config){.targets = t6, .nb_targets = 1, .timeout_ms = 2000}, &err));
  TEST_ASSERT_NOT_NULL(c);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_TIMEOUT,
                        grpc_client_unary(c, "/web.Svc/Slow", "", 0, nullptr, 0, 40, &resp));
  grpc_client_response_free(&resp);

  char huge[2048];
  memset(huge, 'X', sizeof(huge));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(c, "/web.Svc/Echo", huge, sizeof(huge), nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_RESOURCE_EXHAUSTED, resp.status);
  grpc_client_response_free(&resp);

  /* Identity, Gzip, and Zstd responses */
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(c, "/web.Svc/Codec", "gzip", 4, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(c, "/web.Svc/Codec", "id", 2, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* Client max_message_size exceeded by an (uncompressed) 64-byte response -> GRPC_ERR_LIMIT */
  grpc_client *c_tiny = fx_client(grpc_client_new(
      &(grpc_client_config){
          .targets = t6, .nb_targets = 1, .max_message_size = 8, .timeout_ms = 2000},
      &err));
  TEST_ASSERT_NOT_NULL(c_tiny);
  char msg64[64];
  memset(msg64, 'M', sizeof(msg64));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_LIMIT, grpc_client_unary(c_tiny, "/web.Svc/Echo", msg64,
                                                          sizeof(msg64), nullptr, 0, 0, &resp));
  grpc_client_response_free(&resp);
  fx_client_free(c_tiny);
#if defined(GRPC_ENABLE_ZSTD)
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(c, "/web.Svc/Codec", "zstd", 4, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
#endif
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID,
                        grpc_client_unary(c, "/web.Svc/Codec", "id", 2, nullptr, 0, 0, nullptr));

#if defined(GRPC_ENABLE_TLS)
  TEST_ASSERT_NULL(grpc_server_new_ex(
      &(grpc_config){
          .port = 0, .tls = {.cert_file = "a.pem", .cert_pem = "pem"}, .log_level = GRPC_LOG_OFF},
      &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
#endif

  /* Stream EOF recv + post-close_send GRPC_ERR_STATE + null safety */
  TEST_ASSERT_NULL(grpc_client_stream_open(nullptr, "/a/b", nullptr, 0, 0, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_client_stream_send(nullptr, "x", 1));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_client_stream_close_send(nullptr));
  grpc_message *m = nullptr;
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_client_stream_recv(nullptr, &m));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_client_stream_finish(nullptr, &resp));
  grpc_client_stream_cancel(nullptr);

  grpc_trace_context bad_tc = {0};
  char tp_buf[64];
  TEST_ASSERT_FALSE(grpc_call_trace_context(nullptr, &bad_tc));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_traceparent_format(&bad_tc, tp_buf, sizeof(tp_buf)));

  grpc_client_stream *st =
      fx_stream(grpc_client_stream_open(c, "/web.Svc/Echo", nullptr, 0, 2000, &err));
  TEST_ASSERT_NOT_NULL(st);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(st, "hi", 2));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_close_send(st));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_close_send(st));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_STATE, grpc_client_stream_send(st, "no", 2));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(st, &m));
  TEST_ASSERT_NOT_NULL(m);
  grpc_message_free(m);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(st, &m));
  TEST_ASSERT_NULL(m);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, fx_stream_finish(st, &resp));
  grpc_client_response_free(&resp);

  fx_client_free(c);
  fx_server_free(server);
}

static void *async_worker_finish_only(void *arg) {
  grpc_call *call = arg;
  (void)grpc_call_finish_async(call, GRPC_STATUS_ALREADY_EXISTS, "exists");
  grpc_call_unref(call);
  return nullptr;
}

static void async_finish_only_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                                      [[maybe_unused]] void *userdata) {
  spawn_call_worker(call, async_worker_finish_only);
}

static void comp_codecs_handler(grpc_call *call, const grpc_message *req,
                                [[maybe_unused]] void *userdata) {
  (void)grpc_call_set_compression(call, GRPC_COMPRESS_IDENTITY, -1);
#if defined(GRPC_ENABLE_ZSTD)
  HCHECK(grpc_call_set_compression(call, GRPC_COMPRESS_ZSTD, 3) == GRPC_OK);
#endif
#if defined(GRPC_ENABLE_SNAPPY)
  HCHECK(grpc_call_set_compression(call, GRPC_COMPRESS_SNAPPY, -1) == GRPC_OK);
#endif
  HCHECK(grpc_call_set_compression(call, (grpc_compression_algorithm)99, -1) ==
         GRPC_ERR_UNSUPPORTED);
  grpc_metadata bad_md[] = {{.key = ":bad", .value = "v"}};
  HCHECK(grpc_call_send_metadata(call, bad_md, 1) == GRPC_ERR_INVALID);
  HCHECK(grpc_call_send_metadata_async(call, bad_md, 1) == GRPC_ERR_INVALID);
  (void)grpc_call_reply(call, grpc_message_new(req->data, req->len));
}

/* A client for a server whose connection slots are all taken: the server accepts the connection
 * and closes it at once, so either grpc_client_new() fails or the client's first call does. */
static bool extra_connection_rejected(const grpc_client_config *cfg, const char *method) {
  grpc_err err = GRPC_OK;
  grpc_client *c = fx_client(grpc_client_new(cfg, &err));
  if (c == nullptr)
    return err == GRPC_ERR_IO || err == GRPC_ERR_CLOSED;
  grpc_client_response resp = {0};
  grpc_err rc = grpc_client_unary(c, method, "x", 1, nullptr, 0, 0, &resp);
  bool failed = rc == GRPC_ERR_CLOSED || rc == GRPC_ERR_IO;
  grpc_client_response_free(&resp);
  fx_client_free(c);
  return failed;
}

#ifndef _WIN32
/* Raw HTTP/2 frames read off a socket that never answers anything (no PING ACK). */
typedef struct raw_frames {
  uint8_t buf[4096];
  size_t len;
  int pings;
  bool closed;
} raw_frames;

static void raw_frames_pump(raw_frames *r, int fd, int timeout_ms) {
  struct pollfd pfd = {.fd = fd, .events = POLLIN};
  if (r->closed || poll(&pfd, 1, timeout_ms) <= 0)
    return;
  ssize_t n = recv(fd, r->buf + r->len, sizeof(r->buf) - r->len, 0);
  if (n == 0 || (n < 0 && errno != EINTR && errno != EAGAIN)) {
    r->closed = true;
    return;
  }
  if (n < 0)
    return;
  r->len += (size_t)n;
  while (r->len >= 9) {
    size_t flen = ((size_t)r->buf[0] << 16) | ((size_t)r->buf[1] << 8) | r->buf[2];
    if (9 + flen > sizeof(r->buf)) {
      r->len = 0; /* not expected from this server: drop it */
      break;
    }
    if (r->len < 9 + flen)
      break;
    if (r->buf[3] == NGHTTP2_PING && !(r->buf[4] & NGHTTP2_FLAG_ACK))
      r->pings++;
    memmove(r->buf, r->buf + 9 + flen, r->len - 9 - flen);
    r->len -= 9 + flen;
  }
}

#define RAW_FRAMES_UNTIL(r, fd, cond)                     \
  do {                                                    \
    const uint64_t rf_end_ = now_ms() + WAIT_DEADLINE_MS; \
    while (!(cond) && !(r)->closed && now_ms() < rf_end_) \
      raw_frames_pump((r), (fd), 10);                     \
  } while (0)

static raw_frames g_ka_frames;
#endif

void test_integration_maintenance_drain_and_edge_paths(void) {
  /* 1. Maintenance timer: keepalive PING to a client that stopped reading, then the
   * keepalive-timeout close (logged at DEBUG; the TRACE level is captured, not printed) */
  FILE *srv_log = fx_log();
  grpc_server *srv = fx_server(grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .keepalive_time_ms = 30,
      .keepalive_timeout_ms = 200,
      .max_message_size = 5 * 1024 * 1024,
      .log_level = GRPC_LOG_TRACE,
      .log_file = srv_log,
  }));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle(srv, "/m.Svc/AsyncFin", async_finish_only_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv, "/m.Svc/Codecs", comp_codecs_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv, "/m.Svc/Big", large_echo_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv, "/m.Svc/Hold", slow_deadline_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_enable(srv));
  fx_start(srv);

  /* Connect via DNS "localhost" + test async_finish_only, codecs, and 512 KiB big write */
  grpc_err err = GRPC_OK;
  grpc_client *c = fx_client(grpc_client_new(
      &(grpc_client_config){.host = "localhost", .port = grpc_server_port(srv), .timeout_ms = 3000},
      &err));
  TEST_ASSERT_NOT_NULL(c);

  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(c, "/m.Svc/AsyncFin", "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_ALREADY_EXISTS, resp.status);
  grpc_client_response_free(&resp);
  join_call_workers();

  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(c, "/m.Svc/Codecs", "hello", 5, nullptr, 0, 0, &resp));
  ASSERT_HANDLER_CHECKS();
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* Health check with unknown varint + length-delimited fields before service name */
  const uint8_t hreq_unk[] = {0x10, 0x01, 0x1A, 0x02, 'x', 'y', 0x0A, 0x00};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(c, GRPC_HEALTH_PATH_CHECK, hreq_unk,
                                                   sizeof(hreq_unk), nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* 4 MiB payload to overflow loopback socket buffers and exercise async uv_write / on_sub_write / on_session_write */
  size_t big_sz = 4 * 1024 * 1024;
  char *big = fx_mem(grpc_malloc(big_sz));
  memset(big, 'M', big_sz);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(c, "/m.Svc/Big", big, big_sz, nullptr, 0, 5000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32((uint32_t)big_sz, resp.message->len);
  grpc_client_response_free(&resp);
  fx_free(big);

  /* Client max_message_size limit exceeded + invalid request metadata */
  grpc_client *c_small = fx_client(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(srv),
          .max_message_size = 64,
          .timeout_ms = 2000,
      },
      &err));
  TEST_ASSERT_NOT_NULL(c_small);
  char med[256];
  memset(med, 'R', sizeof(med));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_LIMIT, grpc_client_unary(c_small, "/m.Svc/Big", med, sizeof(med),
                                                          nullptr, 0, 0, &resp));
  grpc_client_response_free(&resp);
  grpc_metadata null_key[] = {{.key = nullptr, .value = "v"}};
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID,
                        grpc_client_unary(c_small, "/m.Svc/Big", "a", 1, null_key, 1, 0, &resp));
  fx_client_free(c_small);

  /* c is idle now and reads nothing: a maintenance tick (every 250 ms) sends it a keepalive PING
   * and a later one closes the connection, as no ACK came within keepalive_timeout_ms */
  ASSERT_EVENTUALLY(srv_stats(srv).active_connections == 0);
  TEST_ASSERT_TRUE(log_contains(srv_log, "keepalive timeout"));
  fx_client_free(c);
  fx_server_free(srv);

  /* 2. An active Health/Watch kept open across server shutdown: drain times out on it, and
   * grpc_server_free() covers health_free's watcher cleanup */
  grpc_server *dsrv = fx_server(grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF}));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_enable(dsrv));
  fx_start(dsrv);
  grpc_client *c2 = fx_client(grpc_client_new(&(grpc_client_config){.host = "127.0.0.1",
                                                                    .port = grpc_server_port(dsrv),
                                                                    .timeout_ms = 2000},
                                              &err));
  TEST_ASSERT_NOT_NULL(c2);
  grpc_client_stream *w =
      fx_stream(grpc_client_stream_open(c2, GRPC_HEALTH_PATH_WATCH, nullptr, 0, 2000, &err));
  TEST_ASSERT_NOT_NULL(w);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(w, "", 0));
  grpc_message *wm = nullptr;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(w, &wm));
  TEST_ASSERT_NOT_NULL(wm);
  TEST_ASSERT_EQUAL_UINT8(GRPC_HEALTH_SERVING, wm->data[1]);
  grpc_message_free(wm);
  fx_stop_join(dsrv);
  /* Drain with active stream times out (GRPC_ERR_TIMEOUT) */
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_TIMEOUT, grpc_server_drain(dsrv, 30));
  fx_stream_cancel(w);
  fx_client_free(c2);
  fx_server_free(dsrv);

#ifndef _WIN32
  /* 3. Unix target via targets[] ("unix:///path") + UDS max_connections=1 rejection */
  const char *upath = fx_tmp_path("uds.sock");
  grpc_server *usrv = fx_server(grpc_server_new(
      &(grpc_config){.unix_path = upath, .max_connections = 1, .log_level = GRPC_LOG_OFF}));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(usrv, "/u.Svc/Echo", large_echo_handler, nullptr));
  fx_start(usrv);

  char utarget[400];
  snprintf(utarget, sizeof(utarget), "unix://%s", upath);
  const char *utargets[] = {utarget};
  grpc_client *uc1 = fx_client(grpc_client_new(
      &(grpc_client_config){.targets = utargets, .nb_targets = 1, .timeout_ms = 2000}, &err));
  TEST_ASSERT_NOT_NULL(uc1);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(uc1, "/u.Svc/Echo", "u", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  /* uc1 holds the only slot: the server accepts uc2 and closes it at once, so uc2 fails to
   * connect or, if the close lands after the connect, its first call fails */
  TEST_ASSERT_TRUE(extra_connection_rejected(
      &(grpc_client_config){.targets = utargets, .nb_targets = 1, .timeout_ms = 2000},
      "/u.Svc/Echo"));
  TEST_ASSERT_EQUAL_UINT64(1, srv_stats(usrv).total_connections);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(uc1, "/u.Svc/Echo", "u", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  fx_client_free(uc1);
  fx_server_free(usrv);
#endif

  /* 4. Server invalid IPv6 & invalid log level & keepalive-timeout + idle-GOAWAY */
  TEST_ASSERT_NULL(grpc_server_new_ex(
      &(grpc_config){.port = 0, .ipv6_addr = "not-ipv6", .log_level = GRPC_LOG_OFF}, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  TEST_ASSERT_NULL(
      grpc_server_new_ex(&(grpc_config){.port = 0, .log_level = (grpc_log_level)99}, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);

  /* Keepalive timeout + PING ACK: raw socket ignores PING (timeout), grpc_client ACKs PING */
  FILE *ka_log = fx_log();
  grpc_server *ka_srv = fx_server(grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .keepalive_time_ms = 20,
      .keepalive_timeout_ms = 50,
      .log_level = GRPC_LOG_DEBUG,
      .log_file = ka_log,
  }));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(ka_srv, "/ka.Svc/Ping", no_body_handler, nullptr));
  fx_start(ka_srv);
#ifndef _WIN32
  grpc_client *ka_c = fx_client(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1", .port = grpc_server_port(ka_srv), .timeout_ms = 2000},
      &err));
  TEST_ASSERT_NOT_NULL(ka_c);
  int raw_fd = raw_connect4(grpc_server_port(ka_srv));
  /* client connection preface + empty SETTINGS; the raw peer never ACKs anything */
  static const uint8_t preface[] = {
      'P',  'R',  'I',  ' ',  '*', ' ', 'H',  'T',  'T',  'P',  '/', '2', '.', '0',
      '\r', '\n', '\r', '\n', 'S', 'M', '\r', '\n', '\r', '\n', 0,   0,   0,   NGHTTP2_SETTINGS,
      0,    0,    0,    0,    0};
  raw_h2_send_all(raw_fd, preface, sizeof(preface));
  raw_frames *kaf = &g_ka_frames;
  memset(kaf, 0, sizeof(*kaf));
  /* the first maintenance tick (250 ms) PINGs both idle connections */
  RAW_FRAMES_UNTIL(kaf, raw_fd, kaf->pings > 0);
  TEST_ASSERT_TRUE_MESSAGE(kaf->pings > 0, "no keepalive PING");
  /* ka_c reads (and ACKs) its PING during this call */
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(ka_c, "/ka.Svc/Ping", "", 0, nullptr, 0, 1000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  /* the raw connection never ACKs, so it is closed after keepalive_timeout_ms */
  RAW_FRAMES_UNTIL(kaf, raw_fd, false);
  TEST_ASSERT_TRUE_MESSAGE(kaf->closed, "keepalive timeout did not close the connection");
  TEST_ASSERT_TRUE(log_contains(ka_log, "keepalive timeout"));
  fx_close(raw_fd);
  fx_client_free(ka_c);
#endif
  fx_server_free(ka_srv);

  /* Idle timeout: client connects and stays idle -> server sends GOAWAY and closes */
  grpc_server *idle_srv = fx_server(grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .max_connection_idle_ms = 30,
      .max_connection_age_grace_ms = 30,
      .log_level = GRPC_LOG_OFF,
  }));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_status_details_decode_all(nullptr, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID,
                        grpc_server_handle(nullptr, "/a/b", no_body_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_handle_stream(
                                              nullptr, "/a/b", &(grpc_stream_handler){0}, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_unregister(nullptr, "/a/b"));
  fx_start(idle_srv);
  grpc_client *ic = fx_client(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(idle_srv)}, &err));
  TEST_ASSERT_NOT_NULL(ic);
  ASSERT_EVENTUALLY(srv_stats(idle_srv).total_connections == 1 &&
                    srv_stats(idle_srv).active_connections == 0);
  fx_client_free(ic);
  fx_server_free(idle_srv);

  /* Max connection age + grace timeout: GOAWAY, then close although a stream is still open */
  grpc_server *age_srv = fx_server(grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .max_connection_age_ms = 30,
      .max_connection_age_grace_ms = 30,
      .log_level = GRPC_LOG_OFF,
  }));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle(age_srv, "/age.Svc/Hold", slow_deadline_handler, nullptr));
  fx_start(age_srv);
  grpc_client *ac = fx_client(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(age_srv)}, &err));
  TEST_ASSERT_NOT_NULL(ac);
  grpc_client_stream *as =
      fx_stream(grpc_client_stream_open(ac, "/age.Svc/Hold", nullptr, 0, 0, &err));
  TEST_ASSERT_NOT_NULL(as);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(as, "x", 1));
  ASSERT_EVENTUALLY(srv_stats(age_srv).active_streams == 1);
  ASSERT_EVENTUALLY(srv_stats(age_srv).active_connections == 0);
  TEST_ASSERT_EQUAL_UINT64(1, srv_stats(age_srv).total_connections);
  fx_stream_cancel(as);
  fx_client_free(ac);
  fx_server_free(age_srv);

  /* Unreachable port with retries -> GRPC_ERR_IO */
  TEST_ASSERT_NULL(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = 1,
          .max_retries = 1,
          .retry_backoff_ms = 5,
      },
      &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_IO, err);

  /* Poll-driven GRPC_EVENT_CANCEL when client cancels stream */
  grpc_server *pc_srv = fx_server(grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF}));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_register_path(pc_srv, "/pc.Svc/Hold", nullptr));
  grpc_client *pc_c = fx_client(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1", .port = grpc_server_port(pc_srv), .timeout_ms = 2000},
      &err));
  TEST_ASSERT_NOT_NULL(pc_c);
  grpc_client_stream *pc_st =
      fx_stream(grpc_client_stream_open(pc_c, "/pc.Svc/Hold", nullptr, 0, 2000, &err));
  TEST_ASSERT_NOT_NULL(pc_st);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(pc_st, "x", 1));
  int saw_msg = 0;
  for (int i = 0; i < 20 && !saw_msg; i++) {
    if (grpc_server_poll(pc_srv, 30) > 0) {
      size_t cnt = 0;
      const grpc_event *evs = grpc_server_events(pc_srv, &cnt);
      for (size_t j = 0; j < cnt; j++) {
        if (evs[j].type == GRPC_EVENT_MESSAGE) {
          saw_msg = 1;
        }
      }
    }
  }
  TEST_ASSERT_EQUAL_INT(1, saw_msg);
  fx_stream_cancel(pc_st);
  int saw_cancel = 0;
  for (int i = 0; i < 20 && !saw_cancel; i++) {
    int n = grpc_server_poll(pc_srv, 30);
    if (n > 0) {
      size_t cnt = 0;
      const grpc_event *evs = grpc_server_events(pc_srv, &cnt);
      for (size_t j = 0; j < cnt; j++) {
        if (evs[j].type == GRPC_EVENT_CANCEL) {
          TEST_ASSERT_EQUAL_INT(GRPC_STATUS_CANCELLED, grpc_call_cancel_reason(evs[j].call));
          saw_cancel = 1;
        }
      }
    }
  }
  TEST_ASSERT_EQUAL_INT(1, saw_cancel);
  fx_client_free(pc_c);
  fx_server_free(pc_srv);

  /* Large client payload (4 MiB) to exercise async uv_write / on_sub_write + TCP max_connections */
  FILE *big_log = fx_log();
  grpc_server *big_srv = fx_server(grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .max_connections = 1,
      .max_message_size = 8 * 1024 * 1024,
      .log_level = GRPC_LOG_INFO,
      .log_file = big_log,
  }));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(big_srv, "/big.Svc/Echo", large_echo_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle_stream(big_srv, "/big.Svc/Hold",
                                         &(grpc_stream_handler){.on_start = cancel_watch_on_start},
                                         nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_enable(big_srv));
  /* a service name longer than the 127 bytes kept: found again by the same long name */
  char long_name[131];
  memset(long_name, 'a', 130);
  long_name[130] = '\0';
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_health_set_status(big_srv, long_name, GRPC_HEALTH_NOT_SERVING));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_channelz_enable(big_srv));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_reflection_enable(big_srv));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_reflection_enable(nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_add_interceptor(
                                     big_srv, &(grpc_interceptor){.on_done = test_on_done_cb}));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_add_interceptor(
                                     big_srv, &(grpc_interceptor){.path_prefix = "/other.Svc/"}));
  grpc_interceptor ba_inc = grpc_interceptor_bearer_auth(bearer_validator, nullptr);
  ba_inc.path_prefix = "/big.Svc/Auth";
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_add_interceptor(big_srv, &ba_inc));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(big_srv, "/big.Svc/Auth", no_body_handler, nullptr));
  /* 10 tokens/s, burst 2 */
  grpc_interceptor rl_inc = grpc_interceptor_rate_limit(10.0, 2);
  rl_inc.path_prefix = "/big.Svc/RL";
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_add_interceptor(big_srv, &rl_inc));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(big_srv, "/big.Svc/RL", no_body_handler, nullptr));
  fx_start(big_srv);
  grpc_client *bc1 =
      fx_client(grpc_client_new(&(grpc_client_config){.host = "127.0.0.1",
                                                      .port = grpc_server_port(big_srv),
                                                      .max_message_size = 8 * 1024 * 1024,
                                                      .timeout_ms = 5000},
                                &err));
  TEST_ASSERT_NOT_NULL(bc1);
  ASSERT_EVENTUALLY(srv_stats(big_srv).active_connections == 1);
  /* 2nd TCP connection while max_connections=1 -> reject_connection() */
  TEST_ASSERT_TRUE(extra_connection_rejected(
      &(grpc_client_config){
          .host = "127.0.0.1", .port = grpc_server_port(big_srv), .timeout_ms = 500},
      "/big.Svc/Echo"));
  TEST_ASSERT_TRUE(log_contains(big_log, "max_connections (1) reached"));
  TEST_ASSERT_EQUAL_UINT64(1, srv_stats(big_srv).total_connections);

  size_t c_big_sz = 4 * 1024 * 1024;
  char *c_big_buf = fx_mem(grpc_malloc(c_big_sz));
  memset(c_big_buf, 'A', c_big_sz);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(bc1, "/big.Svc/Echo", c_big_buf, c_big_sz,
                                                   nullptr, 0, 5000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32((uint32_t)c_big_sz, resp.message->len);
  grpc_client_response_free(&resp);
  fx_free(c_big_buf);

  /* Channelz GetServers after >127 bytes transferred (multi-byte varints) */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(bc1, GRPC_CHANNELZ_PATH_GET_SERVERS, "", 0,
                                                   nullptr, 0, 2000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* Reflection with unknown protobuf fields (varint, len-delimited, fixed64) */
  grpc_client_stream *rf =
      fx_stream(grpc_client_stream_open(bc1, GRPC_REFLECTION_V1_PATH, nullptr, 0, 2000, &err));
  TEST_ASSERT_NOT_NULL(rf);
  const uint8_t unk_rf[] = {0xF8, 0x06, 0x01, 0xFA, 0x06, 0x01, 'A', 0xF9, 0x06,
                            0,    0,    0,    0,    0,    0,    0,   0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(rf, unk_rf, sizeof(unk_rf)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_close_send(rf));
  /* 0xF8 0x06 / 0xFA 0x06 / 0xF9 0x06 are two-byte tags of field 111 (varint, bytes, fixed64);
   * all three are skipped, and a request with no supported field is answered with an
   * error_response (field 7) carrying UNIMPLEMENTED (12), after the echoed original_request */
  grpc_message *rfm = nullptr;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(rf, &rfm));
  TEST_ASSERT_NOT_NULL(rfm);
  TEST_ASSERT_TRUE(rfm->len > 2 + sizeof(unk_rf) + 4);
  TEST_ASSERT_EQUAL_HEX8(0x12, rfm->data[0]); /* original_request */
  TEST_ASSERT_EQUAL_UINT8(sizeof(unk_rf), rfm->data[1]);
  TEST_ASSERT_EQUAL_MEMORY(unk_rf, rfm->data + 2, sizeof(unk_rf));
  const uint8_t *er = rfm->data + 2 + sizeof(unk_rf);
  TEST_ASSERT_EQUAL_HEX8(0x3A, er[0]); /* error_response */
  TEST_ASSERT_EQUAL_HEX8(0x08, er[2]); /* error_code */
  TEST_ASSERT_EQUAL_UINT8(12, er[3]);  /* UNIMPLEMENTED */
  grpc_message_free(rfm);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, fx_stream_finish(rf, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* Bearer auth with extra spaces */
  grpc_metadata sp_auth[] = {{.key = "authorization", .value = "Bearer   valid-token"}};
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(bc1, "/big.Svc/Auth", "", 0, sp_auth, 1, 2000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* Rate limit: the burst of 2 passes, the bucket then runs dry (RESOURCE_EXHAUSTED) and refills */
  for (int i = 0; i < 2; i++) {
    TEST_ASSERT_EQUAL_INT(GRPC_OK,
                          grpc_client_unary(bc1, "/big.Svc/RL", "", 0, nullptr, 0, 2000, &resp));
    TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
    grpc_client_response_free(&resp);
  }
  bool limited = false;
  for (int i = 0; i < 20 && !limited; i++) {
    TEST_ASSERT_EQUAL_INT(GRPC_OK,
                          grpc_client_unary(bc1, "/big.Svc/RL", "", 0, nullptr, 0, 2000, &resp));
    limited = resp.status == GRPC_STATUS_RESOURCE_EXHAUSTED;
    if (!limited)
      TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
    grpc_client_response_free(&resp);
  }
  TEST_ASSERT_TRUE_MESSAGE(limited, "rate limiter never rejected a call");
  grpc_status rl_status = GRPC_STATUS_RESOURCE_EXHAUSTED;
  const uint64_t rl_end = now_ms() + WAIT_DEADLINE_MS;
  while (rl_status == GRPC_STATUS_RESOURCE_EXHAUSTED && now_ms() < rl_end) {
    uv_sleep(20);
    TEST_ASSERT_EQUAL_INT(GRPC_OK,
                          grpc_client_unary(bc1, "/big.Svc/RL", "", 0, nullptr, 0, 2000, &resp));
    rl_status = resp.status;
    grpc_client_response_free(&resp);
  }
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, rl_status);

  /* on_cancel callback + on_done interceptor for a cancelled stream */
  int done_before = atomic_load(&g_on_done_calls);
  grpc_client_stream *hs =
      fx_stream(grpc_client_stream_open(bc1, "/big.Svc/Hold", nullptr, 0, 2000, &err));
  TEST_ASSERT_NOT_NULL(hs);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(hs, "x", 1));
  ASSERT_EVENTUALLY(atomic_load(&g_hold_started) == 1);
  fx_stream_cancel(hs);
  ASSERT_EVENTUALLY(atomic_load(&g_cancel_cb_fired) >= 1);
  TEST_ASSERT_EQUAL_INT(1, atomic_load(&g_cancel_cb_fired));
  ASSERT_EVENTUALLY(atomic_load(&g_on_done_calls) > done_before);

  /* Health check with >127-byte service name (multi-byte varint) + 2 concurrent watchers */
  uint8_t long_hc[140];
  long_hc[0] = 0x0A;
  long_hc[1] = 0x82; /* varint 130: (0x02 | 0x80), 0x01 */
  long_hc[2] = 0x01;
  memcpy(long_hc + 3, long_name, 130);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(bc1, GRPC_HEALTH_PATH_CHECK, long_hc, 133,
                                                   nullptr, 0, 2000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32(2, resp.message->len);
  TEST_ASSERT_EQUAL_UINT8(GRPC_HEALTH_NOT_SERVING, resp.message->data[1]);
  grpc_client_response_free(&resp);

  grpc_client_stream *w1 =
      fx_stream(grpc_client_stream_open(bc1, GRPC_HEALTH_PATH_WATCH, nullptr, 0, 2000, &err));
  TEST_ASSERT_NOT_NULL(w1);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(w1, "", 0));
  grpc_message *wm1 = nullptr;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(w1, &wm1));
  TEST_ASSERT_NOT_NULL(wm1);
  grpc_message_free(wm1);
  fx_stream_cancel(w1);

  /* Client retry when first subchannel's server shuts down mid-connection */
  grpc_server *srv_ok = fx_server(grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF}));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv_ok, "/big.Svc/Echo", large_echo_handler, nullptr));
  fx_start(srv_ok);
  char ep_a[64], ep_b[64];
  snprintf(ep_a, sizeof(ep_a), "127.0.0.1:%u", grpc_server_port(big_srv));
  snprintf(ep_b, sizeof(ep_b), "127.0.0.1:%u", grpc_server_port(srv_ok));
  const char *rr_targets[] = {ep_a, ep_b};
  fx_client_free(bc1);
  grpc_client *rr = fx_client(grpc_client_new(&(grpc_client_config){.targets = rr_targets,
                                                                    .nb_targets = 2,
                                                                    .max_retries = 2,
                                                                    .retry_backoff_ms = 5,
                                                                    .timeout_ms = 2000},
                                              &err));
  TEST_ASSERT_NOT_NULL(rr);
  /* Stop big_srv so first subchannel fails on send/recv and retries on srv_ok */
  fx_server_free(big_srv);
  /* Control: without retries, the first call (round robin starts at targets[0]) fails */
  grpc_client *rr0 = fx_client(grpc_client_new(
      &(grpc_client_config){.targets = rr_targets, .nb_targets = 2, .timeout_ms = 2000}, &err));
  TEST_ASSERT_NOT_NULL(rr0);
  grpc_err rc0 = grpc_client_unary(rr0, "/big.Svc/Echo", "retry", 5, nullptr, 0, 2000, &resp);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_IO, rc0);
  grpc_client_response_free(&resp);
  fx_client_free(rr0);
  TEST_ASSERT_EQUAL_UINT64(0, srv_stats(srv_ok).total_calls);
  /* With retries the call fails on targets[0] and succeeds on targets[1] */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(rr, "/big.Svc/Echo", "retry", 5, nullptr, 0, 2000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_MEMORY("retry", resp.message->data, 5);
  grpc_client_response_free(&resp);
  TEST_ASSERT_EQUAL_UINT64(1, srv_stats(srv_ok).total_calls);
  fx_client_free(rr);
  fx_server_free(srv_ok);
}

#ifndef _WIN32
static void burst_8mb_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                              [[maybe_unused]] void *ud) {
  size_t sz = 8 * 1024 * 1024;
  char *buf = grpc_malloc(sz);
  if (buf != nullptr) {
    memset(buf, 'B', sz);
    (void)grpc_call_reply_data(call, buf, sz);
    grpc_free(buf);
  } else {
    (void)grpc_call_finish(call, GRPC_STATUS_RESOURCE_EXHAUSTED, "oom");
  }
}

/* Best effort, not asserted: waits (bounded) until a peer's large write has filled this socket's
 * small receive buffer (FIONREAD stops growing), so the writer must fall back to an async
 * uv_write. */
static void wait_socket_backlog(int fd) {
  int prev = -1;
  const uint64_t end = now_ms() + WAIT_DEADLINE_MS;
  while (now_ms() < end) {
    int n = 0;
    if (ioctl(fd, FIONREAD, &n) != 0)
      return;
    if (n >= 2048 && n == prev)
      return;
    prev = n;
    uv_sleep(5);
  }
}

typedef struct slow_h2_srv_ctx {
  int listen_fd;
  bool abort_mid_write;
} slow_h2_srv_ctx;

static int slow_h2_on_frame(nghttp2_session *s, const nghttp2_frame *f, void *ud) {
  int *got_eos = ud;
  if (f->hd.stream_id > 0 && (f->hd.flags & NGHTTP2_FLAG_END_STREAM)) {
    *got_eos = 1;
    nghttp2_nv resp_hdrs[] = {
        NV(":status", "200"),
        NV("content-type", "application/grpc"),
        NV("grpc-status", "0"),
    };
    nghttp2_submit_response(s, f->hd.stream_id, resp_hdrs, 3, nullptr);
  }
  return 0;
}

static void *slow_h2_server_thread(void *arg) {
  slow_h2_srv_ctx *ctx = arg;
  int cfd = accept(ctx->listen_fd, nullptr, nullptr);
  if (cfd < 0)
    return nullptr;

  int got_eos = 0;
  nghttp2_session_callbacks *cbs = nullptr;
  nghttp2_session_callbacks_new(&cbs);
  nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, slow_h2_on_frame);
  nghttp2_session *sess = nullptr;
  nghttp2_session_server_new(&sess, cbs, &got_eos);
  nghttp2_session_callbacks_del(cbs);

  nghttp2_settings_entry iv[] = {
      {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, 16 * 1024 * 1024},
  };
  nghttp2_submit_settings(sess, NGHTTP2_FLAG_NONE, iv, 1);
  nghttp2_session_set_local_window_size(sess, NGHTTP2_FLAG_NONE, 0, 16 * 1024 * 1024);
  h2_flush(sess, cfd);

  /* Let the client's 8 MiB write fill the socket buffer and go async */
  wait_socket_backlog(cfd);

  if (ctx->abort_mid_write) {
    struct linger lg = {.l_onoff = 1, .l_linger = 0};
    setsockopt(cfd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    nghttp2_session_del(sess);
    close(cfd);
    return nullptr;
  }

  struct timeval tv = {.tv_sec = 3, .tv_usec = 0};
  setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  uint8_t rbuf[32768];
  while (!got_eos) {
    ssize_t n = recv(cfd, rbuf, sizeof(rbuf), 0);
    if (n <= 0)
      break;
    nghttp2_session_mem_recv(sess, rbuf, (size_t)n);
    h2_flush(sess, cfd);
  }
  h2_flush(sess, cfd);
  nghttp2_session_del(sess);
  close(cfd);
  return nullptr;
}

static h2_rec g_burst_rec;
static h2_rec g_burst_rec2;
static slow_h2_srv_ctx g_slow_ctx;
static slow_h2_srv_ctx g_slow_ctx2;

void test_integration_async_write_backpressure(void) {
  /* 1. Server-side async uv_write / on_session_write via 8 MiB burst + small SO_RCVBUF */
  grpc_server *srv = fx_server(grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .max_message_size = 16 * 1024 * 1024,
      .log_level = GRPC_LOG_OFF,
  }));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv, "/bp.Svc/Burst", burst_8mb_handler, nullptr));
  fx_start(srv);

  int fd = fx_fd(socket(AF_INET, SOCK_STREAM, 0));
  int rcvbuf = 4096;
  setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
  struct sockaddr_in sin = {0};
  sin.sin_family = AF_INET;
  sin.sin_port = htons(grpc_server_port(srv));
  inet_pton(AF_INET, "127.0.0.1", &sin.sin_addr);
  TEST_ASSERT_EQUAL_INT(0, connect(fd, (struct sockaddr *)&sin, sizeof(sin)));

  h2_rec *rec = &g_burst_rec;
  memset(rec, 0, sizeof(*rec));
  nghttp2_session *sess = h2_client_session(rec);
  nghttp2_settings_entry iv[] = {{NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, 16 * 1024 * 1024}};
  nghttp2_submit_settings(sess, NGHTTP2_FLAG_NONE, iv, 1);
  nghttp2_session_set_local_window_size(sess, NGHTTP2_FLAG_NONE, 0, 16 * 1024 * 1024);
  const uint8_t req_frame[] = {0, 0, 0, 0, 1, 'x'};
  raw_h2_payload p = {.data = req_frame, .len = sizeof(req_frame), .off = 0};
  nghttp2_data_provider dp = {.source.ptr = &p, .read_callback = raw_h2_read_cb};
  nghttp2_nv hdrs[] = {
      NV(":method", "POST"),
      NV(":scheme", "http"),
      NV(":path", "/bp.Svc/Burst"),
      NV(":authority", "localhost"),
      NV("content-type", "application/grpc"),
      NV("te", "trailers"),
  };
  int32_t sid = h2_track(rec, nghttp2_submit_request(sess, nullptr, hdrs, 6, &dp, nullptr));
  h2_flush(sess, fd);
  /* The handler queued its 8 MiB reply; let it fill the tiny socket buffer so the server's write
   * goes through uv_write + on_session_write */
  ASSERT_EVENTUALLY(srv_stats(srv).messages_sent >= 1);
  wait_socket_backlog(fd);
  H2_PUMP_UNTIL(sess, fd, h2_rec_find(rec, sid)->closed, 4 * WAIT_DEADLINE_MS);
  assert_grpc_stream(rec, sid, GRPC_STATUS_OK);
  TEST_ASSERT_EQUAL_UINT64(5 + 8 * 1024 * 1024, h2_rec_find(rec, sid)->body_total);
  fx_session_del(sess);
  fx_close(fd);

  /* 1b. Server-side abort mid async uv_write (on_session_write status < 0) */
  int fd2 = fx_fd(socket(AF_INET, SOCK_STREAM, 0));
  setsockopt(fd2, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
  TEST_ASSERT_EQUAL_INT(0, connect(fd2, (struct sockaddr *)&sin, sizeof(sin)));
  h2_rec *rec2 = &g_burst_rec2;
  memset(rec2, 0, sizeof(*rec2));
  nghttp2_session *sess2 = h2_client_session(rec2);
  nghttp2_submit_settings(sess2, NGHTTP2_FLAG_NONE, iv, 1);
  nghttp2_session_set_local_window_size(sess2, NGHTTP2_FLAG_NONE, 0, 16 * 1024 * 1024);
  raw_h2_payload p2 = {.data = req_frame, .len = sizeof(req_frame), .off = 0};
  nghttp2_data_provider dp2 = {.source.ptr = &p2, .read_callback = raw_h2_read_cb};
  h2_track(rec2, nghttp2_submit_request(sess2, nullptr, hdrs, 6, &dp2, nullptr));
  h2_flush(sess2, fd2);
  ASSERT_EVENTUALLY(srv_stats(srv).messages_sent >= 2);
  wait_socket_backlog(fd2);
  struct linger lg = {.l_onoff = 1, .l_linger = 0};
  setsockopt(fd2, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
  fx_session_del(sess2);
  fx_close(fd2);
  /* the server sees the reset under its pending write and closes the connection */
  ASSERT_EVENTUALLY(srv_stats(srv).active_connections == 0);

  fx_server_free(srv);

  /* 2. Client-side async uv_write / on_sub_write via 8 MiB upload to slow raw HTTP/2 server */
  int lfd = fx_fd(socket(AF_INET, SOCK_STREAM, 0));
  int opt = 1;
  setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  int lrcv = 4096;
  setsockopt(lfd, SOL_SOCKET, SO_RCVBUF, &lrcv, sizeof(lrcv));
  struct sockaddr_in lsin = {0};
  lsin.sin_family = AF_INET;
  lsin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  lsin.sin_port = 0;
  TEST_ASSERT_EQUAL_INT(0, bind(lfd, (struct sockaddr *)&lsin, sizeof(lsin)));
  TEST_ASSERT_EQUAL_INT(0, listen(lfd, 4));
  socklen_t slen = sizeof(lsin);
  TEST_ASSERT_EQUAL_INT(0, getsockname(lfd, (struct sockaddr *)&lsin, &slen));
  uint16_t lport = ntohs(lsin.sin_port);

  g_slow_ctx = (slow_h2_srv_ctx){.listen_fd = lfd};
  pthread_t sth;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&sth, nullptr, slow_h2_server_thread, &g_slow_ctx));
  fx_raw_thread(sth, lfd);

  grpc_err err = GRPC_OK;
  grpc_client *c = fx_client(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = lport, .timeout_ms = 5000}, &err));
  TEST_ASSERT_NOT_NULL(c);
  size_t big = 8 * 1024 * 1024;
  char *payload = fx_mem(grpc_malloc(big));
  memset(payload, 'C', big);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(c, "/bp.Svc/Upload", payload, big, nullptr, 0, 5000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  fx_free(payload);
  fx_client_free(c);
  fx_raw_join(sth);

  /* 3. Abort mid async uv_write on client (on_sub_write status < 0): the call fails */
  g_slow_ctx2 = (slow_h2_srv_ctx){.listen_fd = lfd, .abort_mid_write = true};
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&sth, nullptr, slow_h2_server_thread, &g_slow_ctx2));
  fx_raw_thread(sth, lfd);
  grpc_client *c2 = fx_client(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = lport, .timeout_ms = 2000}, &err));
  TEST_ASSERT_NOT_NULL(c2);
  payload = fx_mem(grpc_malloc(big));
  memset(payload, 'D', big);
  grpc_err arc = grpc_client_unary(c2, "/bp.Svc/Upload", payload, big, nullptr, 0, 2000, &resp);
  TEST_ASSERT_TRUE_MESSAGE(arc == GRPC_ERR_IO || arc == GRPC_ERR_CLOSED,
                           "upload to a peer that reset the connection did not fail");
  grpc_client_response_free(&resp);
  fx_free(payload);
  fx_client_free(c2);
  fx_raw_join(sth);
  fx_close(lfd);
}
#endif

#ifndef _WIN32
/* ---- request framing: trailers, message limits and compression flags (raw HTTP/2) ---- */

static atomic_int g_frame_count = 0;

static void frame_echo_handler(grpc_call *call, const grpc_message *req,
                               [[maybe_unused]] void *ud) {
  HCHECK(grpc_call_reply_data(call, req->data, req->len) == GRPC_OK);
}

static void frame_count_on_message([[maybe_unused]] grpc_call *call,
                                   [[maybe_unused]] const grpc_message *msg,
                                   [[maybe_unused]] void *ud) {
  atomic_fetch_add(&g_frame_count, 1);
}

static void frame_count_on_end(grpc_call *call, [[maybe_unused]] void *ud) {
  char reply[16];
  int n = snprintf(reply, sizeof(reply), "%d", atomic_load(&g_frame_count));
  HCHECK(grpc_call_send_data(call, reply, (size_t)n) == GRPC_OK);
  HCHECK(grpc_call_finish(call, GRPC_STATUS_OK, nullptr) == GRPC_OK);
}

/* Body then request trailers: EOF without END_STREAM, and the trailers end the stream. */
static ssize_t trailer_read_cb(nghttp2_session *s, int32_t sid, uint8_t *buf, size_t len,
                               uint32_t *flags, nghttp2_data_source *src, void *ud) {
  ssize_t n = raw_h2_read_cb(s, sid, buf, len, flags, src, ud);
  if (*flags & NGHTTP2_DATA_FLAG_EOF) {
    *flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
    nghttp2_nv tr[] = {NV("x-trailer", "1")};
    if (nghttp2_submit_trailer(s, sid, tr, 1) != 0)
      return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  return n;
}

/* Body that never ends: the request stays open after the payload. */
static ssize_t open_read_cb(nghttp2_session *s, int32_t sid, uint8_t *buf, size_t len,
                            uint32_t *flags, nghttp2_data_source *src, void *ud) {
  raw_h2_payload *p = src->ptr;
  if (p->off == p->len)
    return NGHTTP2_ERR_DEFERRED;
  ssize_t n = raw_h2_read_cb(s, sid, buf, len, flags, src, ud);
  *flags = NGHTTP2_DATA_FLAG_NONE;
  return n;
}

/* Writes a gRPC frame (flag, big-endian length, payload) at out; returns its size. */
static size_t grpc_frame(uint8_t *out, uint8_t flag, uint32_t declared, const void *payload,
                         size_t len) {
  out[0] = flag;
  out[1] = (uint8_t)(declared >> 24);
  out[2] = (uint8_t)(declared >> 16);
  out[3] = (uint8_t)(declared >> 8);
  out[4] = (uint8_t)declared;
  if (len > 0)
    memcpy(out + 5, payload, len);
  return 5 + len;
}

void test_integration_request_framing(void) {
  grpc_server *server = fx_server(grpc_server_new(&(grpc_config){
      .port = 0, .ipv4_addr = "127.0.0.1", .max_message_size = 100, .log_level = GRPC_LOG_OFF}));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(server, "/f.Svc/Echo", frame_echo_handler, nullptr));
  const grpc_stream_handler count = {.on_message = frame_count_on_message,
                                     .on_end = frame_count_on_end};
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle_stream(server, "/f.Svc/Count", &count, nullptr));
  fx_start(server);
  uint16_t port = grpc_server_port(server);

  int fd = fx_fd(socket(AF_INET, SOCK_STREAM, 0));
  struct sockaddr_in sin = {.sin_family = AF_INET, .sin_port = htons(port)};
  inet_pton(AF_INET, "127.0.0.1", &sin.sin_addr);
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, connect(fd, (struct sockaddr *)&sin, sizeof(sin)),
                                strerror(errno));
  static h2_rec rec_storage;
  h2_rec *rec = &rec_storage;
  memset(rec, 0, sizeof(*rec));
  nghttp2_session *sess = h2_client_session(rec);
  nghttp2_submit_settings(sess, NGHTTP2_FLAG_NONE, nullptr, 0);

#define FRAME_HDRS(path, ...)              \
  {NV(":method", "POST"),                  \
   NV(":scheme", "http"),                  \
   NV(":path", path),                      \
   NV(":authority", "localhost"),          \
   NV("content-type", "application/grpc"), \
   NV("te", "trailers"),                   \
   __VA_ARGS__}
  nghttp2_nv echo_hdrs[] = FRAME_HDRS("/f.Svc/Echo", NV("x-pad", "0"));
  nghttp2_nv count_hdrs[] = FRAME_HDRS("/f.Svc/Count", NV("x-pad", "0"));
  nghttp2_nv br_hdrs[] = FRAME_HDRS("/f.Svc/Echo", NV("grpc-encoding", "br"));
#undef FRAME_HDRS
  const size_t nhdrs = 7;

  /* 1. one message followed by request trailers: an ordinary unary call */
  uint8_t f_tr[16];
  raw_h2_payload p_tr = {.data = f_tr, .len = grpc_frame(f_tr, 0, 2, "hi", 2)};
  nghttp2_data_provider dp_tr = {.source.ptr = &p_tr, .read_callback = trailer_read_cb};
  int32_t s_tr =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, echo_hdrs, nhdrs, &dp_tr, nullptr));
  /* 2. a second unary message, with the request left open: rejected at once */
  uint8_t f_two[32];
  size_t two_len = grpc_frame(f_two, 0, 1, "a", 1);
  two_len += grpc_frame(f_two + two_len, 0, 1, "b", 1);
  raw_h2_payload p_two = {.data = f_two, .len = two_len};
  nghttp2_data_provider dp_two = {.source.ptr = &p_two, .read_callback = open_read_cb};
  int32_t s_two =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, echo_hdrs, nhdrs, &dp_two, nullptr));
  /* 3. an invalid compressed flag */
  uint8_t f_flag[16];
  raw_h2_payload p_flag = {.data = f_flag, .len = grpc_frame(f_flag, 7, 2, "hi", 2)};
  nghttp2_data_provider dp_flag = {.source.ptr = &p_flag, .read_callback = raw_h2_read_cb};
  int32_t s_flag =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, echo_hdrs, nhdrs, &dp_flag, nullptr));
  /* 4. a compressed message with a codec the server does not have */
  uint8_t f_br[16];
  raw_h2_payload p_br = {.data = f_br, .len = grpc_frame(f_br, 1, 2, "hi", 2)};
  nghttp2_data_provider dp_br = {.source.ptr = &p_br, .read_callback = raw_h2_read_cb};
  int32_t s_br =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, br_hdrs, nhdrs, &dp_br, nullptr));
  /* 5. a compressed message without grpc-encoding */
  uint8_t f_noenc[16];
  raw_h2_payload p_noenc = {.data = f_noenc, .len = grpc_frame(f_noenc, 1, 2, "hi", 2)};
  nghttp2_data_provider dp_noenc = {.source.ptr = &p_noenc, .read_callback = raw_h2_read_cb};
  int32_t s_noenc =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, echo_hdrs, nhdrs, &dp_noenc, nullptr));
  /* 6. exactly max_message_size bytes pass; one more is rejected */
  static uint8_t f_max[128], f_over[128], body[101];
  memset(body, 'M', sizeof(body));
  raw_h2_payload p_max = {.data = f_max, .len = grpc_frame(f_max, 0, 100, body, 100)};
  nghttp2_data_provider dp_max = {.source.ptr = &p_max, .read_callback = raw_h2_read_cb};
  int32_t s_max =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, echo_hdrs, nhdrs, &dp_max, nullptr));
  raw_h2_payload p_over = {.data = f_over, .len = grpc_frame(f_over, 0, 101, body, 101)};
  nghttp2_data_provider dp_over = {.source.ptr = &p_over, .read_callback = raw_h2_read_cb};
  int32_t s_over =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, echo_hdrs, nhdrs, &dp_over, nullptr));
  /* 7. thirty small messages in one DATA frame, 450 bytes in all, to a streaming method */
  static uint8_t f_many[512];
  size_t many_len = 0;
  for (int i = 0; i < 30; i++)
    many_len += grpc_frame(f_many + many_len, 0, 10, "0123456789", 10);
  raw_h2_payload p_many = {.data = f_many, .len = many_len};
  nghttp2_data_provider dp_many = {.source.ptr = &p_many, .read_callback = raw_h2_read_cb};
  int32_t s_many =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, count_hdrs, nhdrs, &dp_many, nullptr));
  /* 9. content-type must be application/grpc itself (or with +codec / parameters), not merely
   * start with it */
  nghttp2_nv xyz_hdrs[] = {NV(":method", "POST"),
                           NV(":scheme", "http"),
                           NV(":path", "/f.Svc/Echo"),
                           NV(":authority", "localhost"),
                           NV("content-type", "application/grpcXYZ"),
                           NV("te", "trailers")};
  uint8_t f_ct[16];
  raw_h2_payload p_xyz = {.data = f_ct, .len = grpc_frame(f_ct, 0, 2, "hi", 2)};
  nghttp2_data_provider dp_xyz = {.source.ptr = &p_xyz, .read_callback = raw_h2_read_cb};
  int32_t s_xyz =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, xyz_hdrs, 6, &dp_xyz, nullptr));
  nghttp2_nv proto_hdrs[] = {NV(":method", "POST"),
                             NV(":scheme", "http"),
                             NV(":path", "/f.Svc/Echo"),
                             NV(":authority", "localhost"),
                             NV("content-type", "application/grpc+proto"),
                             NV("te", "trailers")};
  raw_h2_payload p_proto = {.data = f_ct, .len = grpc_frame(f_ct, 0, 2, "hi", 2)};
  nghttp2_data_provider dp_proto = {.source.ptr = &p_proto, .read_callback = raw_h2_read_cb};
  int32_t s_proto =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, proto_hdrs, 6, &dp_proto, nullptr));

  /* 8. a declared length far above the limit, with the request left open: rejected on the
   * prefix, before any of it is buffered */
  uint8_t f_huge[16];
  raw_h2_payload p_huge = {.data = f_huge, .len = grpc_frame(f_huge, 0, 0x7FFFFFFF, "x", 1)};
  nghttp2_data_provider dp_huge = {.source.ptr = &p_huge, .read_callback = open_read_cb};
  int32_t s_huge =
      h2_track(rec, nghttp2_submit_request(sess, nullptr, echo_hdrs, nhdrs, &dp_huge, nullptr));

  h2_flush(sess, fd);
  H2_PUMP_UNTIL(sess, fd, rec->nb_closed == rec->nb_streams, 5000);
  ASSERT_HANDLER_CHECKS();

  const h2_stream_rec *st = h2_get(rec, s_tr);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, st->grpc_status);
  TEST_ASSERT_EQUAL_INT(200, st->http_status);
  TEST_ASSERT_EQUAL_UINT64(7, st->body_len);
  TEST_ASSERT_EQUAL_MEMORY("hi", st->body + 5, 2);
  /* answered early: trailers, then RST_STREAM NO_ERROR so the client stops sending */
  st = h2_get(rec, s_two);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_INTERNAL, st->grpc_status);
  TEST_ASSERT_TRUE(st->rst);
  TEST_ASSERT_EQUAL_UINT32(NGHTTP2_NO_ERROR, st->rst_code);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_INTERNAL, h2_get(rec, s_flag)->grpc_status);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNIMPLEMENTED, h2_get(rec, s_br)->grpc_status);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_INTERNAL, h2_get(rec, s_noenc)->grpc_status);
  st = h2_get(rec, s_max);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, st->grpc_status);
  TEST_ASSERT_EQUAL_UINT64(105, st->body_total);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_RESOURCE_EXHAUSTED, h2_get(rec, s_over)->grpc_status);
  st = h2_get(rec, s_many);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, st->grpc_status);
  TEST_ASSERT_EQUAL_UINT64(7, st->body_len);
  TEST_ASSERT_EQUAL_MEMORY("30", st->body + 5, 2);
  st = h2_get(rec, s_huge);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_RESOURCE_EXHAUSTED, st->grpc_status);
  TEST_ASSERT_TRUE(st->rst);
  TEST_ASSERT_EQUAL_INT(415, h2_get(rec, s_xyz)->http_status);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, h2_get(rec, s_proto)->grpc_status);

  /* nothing is left behind on the still-open connection (request trailers used to orphan a
   * second stream per call) */
  grpc_server_stats stats;
  uint64_t until = now_ms() + 2000;
  do {
    grpc_server_get_stats(server, &stats);
  } while (stats.active_streams != 0 && now_ms() < until && h2_pump(sess, fd, 10));
  TEST_ASSERT_EQUAL_UINT64(0, stats.active_streams);

  fx_close(fd);
  fx_server_free(server);
}
#endif

#ifndef _WIN32
/* ---- server lifecycle: drain with event routes, ping strikes, unix sockets, idle ---- */

static void slow_unary_handler(grpc_call *call, const grpc_message *req,
                               [[maybe_unused]] void *ud) {
  struct timespec ts = {.tv_sec = 1, .tv_nsec = 500 * 1000 * 1000};
  nanosleep(&ts, nullptr); /* holds the server thread longer than max_connection_idle_ms */
  HCHECK(grpc_call_reply_data(call, req->data, req->len) == GRPC_OK);
}

/* Keeps a reference to the first two calls and where their x-shared value lives. */
static grpc_call *g_kept_calls[2];
static const char *g_kept_values[2];
static atomic_int g_kept = 0;

static void keep_call_handler(grpc_call *call, const grpc_message *req, [[maybe_unused]] void *ud) {
  int i = atomic_fetch_add(&g_kept, 1);
  if (i < 2) {
    g_kept_calls[i] = grpc_call_ref(call);
    g_kept_values[i] = grpc_call_metadata(call, "x-shared", nullptr);
  }
  HCHECK(grpc_call_reply_data(call, req->data, req->len) == GRPC_OK);
}

/* An on_done interceptor that flushes the server from inside the deadline scan. */
static atomic_int g_done_flushes = 0;
static void flush_on_done(grpc_call *call, [[maybe_unused]] grpc_status status,
                          [[maybe_unused]] uint64_t duration_ms, void *userdata) {
  (void)call;
  grpc_server_flush(userdata);
  atomic_fetch_add(&g_done_flushes, 1);
}

static void never_finish_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                                 [[maybe_unused]] void *ud) {
  (void)call;
}

typedef struct drain_client_ctx {
  grpc_client_stream *st;
  grpc_err rc;
  grpc_client_response resp;
} drain_client_ctx;

/* Sends one message on an open event-route stream while the main thread drains. */
static void *drain_client_thread(void *arg) {
  drain_client_ctx *ctx = arg;
  struct timespec ts = {.tv_sec = 0, .tv_nsec = 200 * 1000 * 1000};
  nanosleep(&ts, nullptr);
  (void)grpc_client_stream_send(ctx->st, "ev", 2);
  (void)grpc_client_stream_close_send(ctx->st);
  ctx->rc = grpc_client_stream_finish(ctx->st, &ctx->resp);
  return nullptr;
}

/* Connects a raw HTTP/2 session to port, sends `pings` PINGs and waits for their ACKs. */
static void raw_pings(uint16_t port, h2_rec *rec, int pings, bool keep_open, int *out_fd) {
  int fd = fx_fd(socket(AF_INET, SOCK_STREAM, 0));
  struct sockaddr_in sin = {.sin_family = AF_INET, .sin_port = htons(port)};
  inet_pton(AF_INET, "127.0.0.1", &sin.sin_addr);
  TEST_ASSERT_EQUAL_INT(0, connect(fd, (struct sockaddr *)&sin, sizeof(sin)));
  memset(rec, 0, sizeof(*rec));
  nghttp2_session *sess = h2_client_session(rec);
  nghttp2_submit_settings(sess, NGHTTP2_FLAG_NONE, nullptr, 0);
  for (int i = 0; i < pings; i++)
    nghttp2_submit_ping(sess, NGHTTP2_FLAG_NONE, nullptr);
  h2_flush(sess, fd);
  H2_PUMP_UNTIL(sess, fd, rec->ping_acks + rec->goaways >= pings, 2000);
  H2_PUMP_UNTIL(sess, fd, rec->goaways > 0, 200); /* a GOAWAY would follow the ACKs */
  if (keep_open)
    *out_fd = fd;
  else
    fx_close(fd);
}

void test_integration_server_lifecycle_fixes(void) {
  /* 1. ping strikes do not carry over to the next connection in the same session slot */
  grpc_server *ps = fx_server(grpc_server_new(&(grpc_config){.port = 0,
                                                             .ipv4_addr = "127.0.0.1",
                                                             .max_connections = 1,
                                                             .max_ping_strikes = 1,
                                                             .min_ping_interval_ms = 10000,
                                                             .log_level = GRPC_LOG_OFF}));
  fx_start(ps);
  static h2_rec prec;
  raw_pings(grpc_server_port(ps), &prec, 1, false, nullptr);
  TEST_ASSERT_EQUAL_INT(1, prec.ping_acks);
  grpc_server_stats st;
  uint64_t until = now_ms() + 2000;
  do { /* the slot comes back once the server has seen the close */
    grpc_server_get_stats(ps, &st);
  } while (st.active_connections != 0 && now_ms() < until);
  TEST_ASSERT_EQUAL_UINT64(0, st.active_connections);
  /* two quick PINGs on a fresh connection: one strike at most, allowed by max_ping_strikes */
  raw_pings(grpc_server_port(ps), &prec, 2, false, nullptr);
  TEST_ASSERT_EQUAL_INT(2, prec.ping_acks);
  TEST_ASSERT_EQUAL_INT(0, prec.goaways);
  fx_server_free(ps);

  /* 2. a server that fails to start never removes another server's Unix socket; a stale socket
   * is replaced, anything else is left alone */
  char sock_path[256];
  snprintf(sock_path, sizeof(sock_path), "%s", fx_tmp_path("life.sock"));
  grpc_server *ua = fx_server(grpc_server_new(
      &(grpc_config){.port = 0, .unix_path = sock_path, .log_level = GRPC_LOG_OFF}));
  fx_start(ua);
  grpc_err err = GRPC_OK;
  grpc_server *ub = grpc_server_new_ex(
      &(grpc_config){
          .port = 0, .ipv4_addr = "999.0.0.1", .unix_path = sock_path, .log_level = GRPC_LOG_OFF},
      &err);
  TEST_ASSERT_NULL(ub);
  TEST_ASSERT_EQUAL_INT(0, access(sock_path, F_OK));
  /* binding the live socket again fails instead of stealing it */
  ub = grpc_server_new_ex(
      &(grpc_config){.port = 0, .unix_path = sock_path, .log_level = GRPC_LOG_OFF}, &err);
  TEST_ASSERT_NULL(ub);
  TEST_ASSERT_EQUAL_INT(0, access(sock_path, F_OK));
  grpc_client *uc = fx_client(
      grpc_client_new(&(grpc_client_config){.unix_path = sock_path, .timeout_ms = 2000}, &err));
  TEST_ASSERT_NOT_NULL_MESSAGE(uc, "the first server's socket is gone");
  fx_client_free(uc);
  fx_server_free(ua);
  TEST_ASSERT_NOT_EQUAL_INT(0, access(sock_path, F_OK)); /* removed by its owner */
  /* a stale socket file (no listener) is replaced */
  int stale = socket(AF_UNIX, SOCK_STREAM, 0);
  struct sockaddr_un sun = {.sun_family = AF_UNIX};
  TEST_ASSERT_TRUE(strlen(sock_path) < sizeof(sun.sun_path));
  memcpy(sun.sun_path, sock_path, strlen(sock_path) + 1);
  TEST_ASSERT_EQUAL_INT(0, bind(stale, (struct sockaddr *)&sun, sizeof(sun)));
  close(stale);
  grpc_server *uc2 = fx_server(grpc_server_new(
      &(grpc_config){.port = 0, .unix_path = sock_path, .log_level = GRPC_LOG_OFF}));
  fx_server_free(uc2);
  /* a regular file is never deleted */
  FILE *f = fopen(sock_path, "w");
  TEST_ASSERT_NOT_NULL(f);
  fclose(f);
  ub = grpc_server_new_ex(
      &(grpc_config){.port = 0, .unix_path = sock_path, .log_level = GRPC_LOG_OFF}, &err);
  TEST_ASSERT_NULL(ub);
  TEST_ASSERT_EQUAL_INT(0, access(sock_path, F_OK));
  unlink(sock_path);

  /* 3. max_connection_idle_ms counts from when the last stream ended: a call slower than the
   * idle limit does not get its connection closed right after it */
  grpc_server *is = fx_server(grpc_server_new(&(grpc_config){.port = 0,
                                                             .ipv4_addr = "127.0.0.1",
                                                             .max_connection_idle_ms = 1000,
                                                             .log_level = GRPC_LOG_OFF}));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(is, "/life.Svc/Slow", slow_unary_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(is, "/life.Svc/Echo", frame_echo_handler, nullptr));
  fx_start(is);
  grpc_client *ic = fx_client(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(is), .timeout_ms = 3000},
      &err));
  TEST_ASSERT_NOT_NULL(ic);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(ic, "/life.Svc/Slow", "s", 1, nullptr, 0, 3000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  struct timespec half = {.tv_sec = 0, .tv_nsec = 500 * 1000 * 1000};
  nanosleep(&half, nullptr);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(ic, "/life.Svc/Echo", "e", 1, nullptr, 0, 3000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  grpc_server_get_stats(is, &st);
  TEST_ASSERT_EQUAL_UINT64(1, st.total_connections);
  fx_client_free(ic);
  fx_server_free(is);

  /* 4. request headers belong to the call: two calls on one connection with the same header
   * (decoded from one shared HPACK entry) get separate copies, so the last reference may be
   * dropped on any thread while the loop thread decodes further requests */
  grpc_server *hs = fx_server(grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF}));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(hs, "/life.Svc/Keep", keep_call_handler, nullptr));
  fx_start(hs);
  atomic_store(&g_kept, 0);
  grpc_client *hc = fx_client(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(hs), .timeout_ms = 3000},
      &err));
  TEST_ASSERT_NOT_NULL(hc);
  grpc_metadata shared_md[] = {{.key = "x-shared", .value = "same-value-on-both-calls"}};
  for (int i = 0; i < 2; i++) {
    TEST_ASSERT_EQUAL_INT(
        GRPC_OK, grpc_client_unary(hc, "/life.Svc/Keep", "k", 1, shared_md, 1, 3000, &resp));
    TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
    grpc_client_response_free(&resp);
  }
  ASSERT_HANDLER_CHECKS();
  TEST_ASSERT_EQUAL_INT(2, atomic_load(&g_kept));
  TEST_ASSERT_TRUE(g_kept_values[0] != g_kept_values[1]);
  TEST_ASSERT_EQUAL_STRING("same-value-on-both-calls", g_kept_values[0]);
  TEST_ASSERT_EQUAL_STRING("same-value-on-both-calls", g_kept_values[1]);
  /* the final unrefs happen here, on the test thread, while the server keeps serving */
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(hc, "/life.Svc/Keep", "k", 1, shared_md, 1, 3000, &resp));
  grpc_client_response_free(&resp);
  grpc_call_unref(g_kept_calls[0]);
  grpc_call_unref(g_kept_calls[1]);
  fx_client_free(hc);
  fx_server_free(hs);

  /* 5. a port already in use is not silently shared; reuse_port opts into sharing (Unix) */
  grpc_server *p1 = fx_server(grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF}));
  uint16_t taken = grpc_server_port(p1);
  TEST_ASSERT_NULL(grpc_server_new_ex(
      &(grpc_config){.port = taken, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF}, &err));
  fx_server_free(p1);
  grpc_server *r1 = fx_server(grpc_server_new(&(grpc_config){
      .port = 0, .ipv4_addr = "127.0.0.1", .reuse_port = true, .log_level = GRPC_LOG_OFF}));
  grpc_server *r2 = fx_server(grpc_server_new(&(grpc_config){.port = grpc_server_port(r1),
                                                             .ipv4_addr = "127.0.0.1",
                                                             .reuse_port = true,
                                                             .log_level = GRPC_LOG_OFF}));
  TEST_ASSERT_EQUAL_UINT16(grpc_server_port(r1), grpc_server_port(r2));
  fx_server_free(r2);
  fx_server_free(r1);

  /* 6. drain shuts connections down in gRPC's two steps: a GOAWAY notice naming the largest
   * stream id, then the final GOAWAY naming the last stream */
  grpc_server *gs = fx_server(grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF}));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(gs, "/life.Svc/Echo", frame_echo_handler, nullptr));
  fx_start(gs);
  int gfd = fx_fd(socket(AF_INET, SOCK_STREAM, 0));
  struct sockaddr_in gsin = {.sin_family = AF_INET, .sin_port = htons(grpc_server_port(gs))};
  inet_pton(AF_INET, "127.0.0.1", &gsin.sin_addr);
  TEST_ASSERT_EQUAL_INT(0, connect(gfd, (struct sockaddr *)&gsin, sizeof(gsin)));
  static h2_rec grec;
  memset(&grec, 0, sizeof(grec));
  nghttp2_session *gsess = h2_client_session(&grec);
  nghttp2_submit_settings(gsess, NGHTTP2_FLAG_NONE, nullptr, 0);
  nghttp2_nv ghdrs[] = {NV(":method", "POST"),
                        NV(":scheme", "http"),
                        NV(":path", "/life.Svc/Echo"),
                        NV(":authority", "localhost"),
                        NV("content-type", "application/grpc"),
                        NV("te", "trailers")};
  static const uint8_t gbody[] = {0, 0, 0, 0, 1, 'g'};
  raw_h2_payload gp = {.data = gbody, .len = sizeof(gbody)};
  nghttp2_data_provider gdp = {.source.ptr = &gp, .read_callback = raw_h2_read_cb};
  int32_t gsid = h2_track(&grec, nghttp2_submit_request(gsess, nullptr, ghdrs, 6, &gdp, nullptr));
  h2_flush(gsess, gfd);
  H2_PUMP_UNTIL(gsess, gfd, grec.nb_closed == 1, 3000);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, h2_get(&grec, gsid)->grpc_status);
  fx_stop_join(gs); /* this thread owns the server now */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_drain(gs, 0));
  H2_PUMP_UNTIL(gsess, gfd, grec.goaways >= 2, 2000);
  TEST_ASSERT_EQUAL_INT(2, grec.goaways);
  TEST_ASSERT_EQUAL_INT32(INT32_MAX, grec.goaway_last_ids[0]);
  TEST_ASSERT_EQUAL_INT32(gsid, grec.goaway_last_ids[1]);
  fx_close(gfd);
  fx_server_free(gs);

  /* 7. an on_done callback of an expired call may flush the server: the deadline scan defers it
   * instead of letting it close a session the scan is walking, and the status still goes out */
  grpc_server *ds = fx_server(grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF}));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(ds, "/life.Svc/Never", never_finish_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_add_interceptor(ds, &(grpc_interceptor){
                                                                     .name = "flush-on-done",
                                                                     .on_done = flush_on_done,
                                                                     .userdata = ds,
                                                                 }));
  fx_start(ds);
  atomic_store(&g_done_flushes, 0);
  grpc_client *dc = fx_client(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(ds), .timeout_ms = 3000},
      &err));
  TEST_ASSERT_NOT_NULL(dc);
  for (int i = 0; i < 3; i++) {
    /* the grpc-timeout header makes the server's deadline fire */
    grpc_client_response dr = {0};
    grpc_err drc = grpc_client_unary(dc, "/life.Svc/Never", "n", 1, nullptr, 0, 150, &dr);
    TEST_ASSERT_TRUE(drc == GRPC_OK || drc == GRPC_ERR_TIMEOUT);
    if (drc == GRPC_OK)
      TEST_ASSERT_EQUAL_INT(GRPC_STATUS_DEADLINE_EXCEEDED, dr.status);
    grpc_client_response_free(&dr);
  }
  until = now_ms() + 2000;
  while (atomic_load(&g_done_flushes) < 3 && now_ms() < until) {
    struct timespec t = {.tv_sec = 0, .tv_nsec = 5 * 1000 * 1000};
    nanosleep(&t, nullptr);
  }
  TEST_ASSERT_EQUAL_INT(3, atomic_load(&g_done_flushes));
  fx_client_free(dc);
  fx_server_free(ds);

  /* 8. drain hands event-route events to the application instead of discarding them */
  grpc_server *es = fx_server(grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF}));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_register_path(es, "/life.Svc/Event", nullptr));
  fx_start(es);
  grpc_client *ec = fx_client(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(es), .timeout_ms = 5000},
      &err));
  TEST_ASSERT_NOT_NULL(ec);
  static drain_client_ctx dctx;
  memset(&dctx, 0, sizeof(dctx));
  dctx.st = grpc_client_stream_open(ec, "/life.Svc/Event", nullptr, 0, 5000, &err);
  TEST_ASSERT_NOT_NULL(dctx.st);
  /* the server sees the stream before the application takes the loop over */
  until = now_ms() + 2000;
  do {
    grpc_server_get_stats(es, &st);
  } while (st.active_streams == 0 && now_ms() < until);
  TEST_ASSERT_EQUAL_UINT64(1, st.active_streams);
  fx_stop_join(es); /* this thread now owns the server */
  pthread_t cth;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&cth, nullptr, drain_client_thread, &dctx));
  bool answered = false;
  grpc_err drc;
  int rounds = 0;
  while ((drc = grpc_server_drain(es, 3000)) == GRPC_ERR_STATE && rounds++ < 10) {
    size_t n = 0;
    const grpc_event *ev = grpc_server_events(es, &n);
    for (size_t i = 0; i < n; i++) {
      if (ev[i].type == GRPC_EVENT_MESSAGE && !answered) {
        TEST_ASSERT_EQUAL_UINT32(2, ev[i].message->len);
        TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_call_send_data(ev[i].call, "ok", 2));
        TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_call_finish(ev[i].call, GRPC_STATUS_OK, nullptr));
        answered = true;
      }
    }
  }
  TEST_ASSERT_EQUAL_INT(GRPC_OK, drc);
  TEST_ASSERT_TRUE(answered);
  pthread_join(cth, nullptr);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, dctx.rc);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, dctx.resp.status);
  TEST_ASSERT_NOT_NULL(dctx.resp.message);
  TEST_ASSERT_EQUAL_MEMORY("ok", dctx.resp.message->data, 2);
  grpc_client_response_free(&dctx.resp);
  fx_client_free(ec);
  fx_server_free(es);
}
#endif

int main(void) {
#ifndef _WIN32
  signal(SIGPIPE, SIG_IGN);
#endif
  UNITY_BEGIN();
  RUN_TEST(test_integration_full_server_and_client);
  RUN_TEST(test_integration_health_watch_and_reflection);
  RUN_TEST(test_integration_poll_events_and_maintenance);
  RUN_TEST(test_integration_grpc_web_cors_ipv6_and_client_edge_cases);
  RUN_TEST(test_integration_maintenance_drain_and_edge_paths);
#ifndef _WIN32
  RUN_TEST(test_integration_async_write_backpressure);
  RUN_TEST(test_integration_request_framing);
  RUN_TEST(test_integration_server_lifecycle_fixes);
#endif
  return UNITY_END();
}
