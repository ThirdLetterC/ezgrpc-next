/* HTTP/3 client allocation failures: one allocation of the calling thread fails at a time (the
 * server thread is never affected) during a unary call over HTTP/3. Every failure ends that call
 * with an error, the client keeps working, and no request state is kept for a stream nghttp3
 * never accepted (a failed nghttp3_conn_submit_request() used to park it until disconnect).
 * Run from the directory holding test_cert.pem / test_key.pem. */
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "grpc.h"
#include "internal_client.h"
#include "test.h"

/* ---- one-shot countdown allocator for the test thread --------------------------------- */

static grpc_allocator g_base;
static _Thread_local bool t_is_target;
static atomic_int g_fail_after = -1; /* -1 off; n: the n-th allocation (0-based) fails, once */
static atomic_int g_injected = 0;

static bool should_fail(void) {
  if (!t_is_target)
    return false;
  int cur = atomic_load(&g_fail_after);
  while (cur >= 0) {
    if (atomic_compare_exchange_weak(&g_fail_after, &cur, cur - 1)) {
      if (cur == 0) {
        atomic_fetch_add(&g_injected, 1);
        return true;
      }
      return false;
    }
  }
  return false;
}

static void *f_malloc(size_t n) {
  return should_fail() ? nullptr : g_base.malloc(n);
}
static void *f_calloc(size_t a, size_t b) {
  return should_fail() ? nullptr : g_base.calloc(a, b);
}
static void *f_realloc(void *p, size_t n) {
  return should_fail() ? nullptr : g_base.realloc(p, n);
}
static void *f_aligned(size_t al, size_t n) {
  return should_fail() ? nullptr : g_base.aligned_alloc(al, n);
}

/* ---- fixture ----------------------------------------------------------------------------- */

static struct {
  grpc_server *server;
  pthread_t thread;
  bool running;
  grpc_client *client;
} fx;

static void *run_server(void *arg) {
  (void)grpc_server_run(arg);
  return nullptr;
}

static void echo(grpc_call *call, const grpc_message *req, [[maybe_unused]] void *ud) {
  (void)grpc_call_reply_data(call, req->data, req->len);
}

void setUp(void) {
  memset(&fx, 0, sizeof(fx));
}

void tearDown(void) {
  atomic_store(&g_fail_after, -1);
  grpc_client_free(fx.client);
  if (fx.running) {
    grpc_server_stop(fx.server);
    pthread_join(fx.thread, nullptr);
  }
  grpc_server_free(fx.server);
  memset(&fx, 0, sizeof(fx));
}

static bool unary_ok(void) {
  grpc_client_response r = {0};
  grpc_err rc = grpc_client_unary(fx.client, "/o.Svc/Echo", "ping", 4, nullptr, 0, 3000, &r);
  bool ok = rc == GRPC_OK && r.status == GRPC_STATUS_OK && r.message != nullptr &&
            r.message->len == 4 && memcmp(r.message->data, "ping", 4) == 0;
  grpc_client_response_free(&r);
  return ok;
}

void test_http3_client_allocation_failures(void) {
  fx.server = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .tls = {.cert_file = "test_cert.pem", .key_file = "test_key.pem"},
      .http3 = {.enable = true},
      .log_level = GRPC_LOG_OFF,
  });
  TEST_ASSERT_NOT_NULL(fx.server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(fx.server, "/o.Svc/Echo", echo, nullptr));
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&fx.thread, nullptr, run_server, fx.server));
  fx.running = true;
  grpc_err err = GRPC_OK;
  fx.client =
      grpc_client_new(&(grpc_client_config){.host = "127.0.0.1",
                                            .port = grpc_server_http3_port(fx.server),
                                            .timeout_ms = 3000,
                                            .transport = GRPC_TRANSPORT_HTTP3,
                                            .tls = {.enable = true, .insecure_skip_verify = true}},
                      &err);
  TEST_ASSERT_NOT_NULL(fx.client);
  TEST_ASSERT_TRUE(unary_ok());

  t_is_target = true;
  int points = 0;
  for (int n = 0; n < 2000; n++) {
    atomic_store(&g_injected, 0);
    atomic_store(&g_fail_after, n);
    bool ok = unary_ok();
    atomic_store(&g_fail_after, -1);
    bool injected = atomic_load(&g_injected) > 0;
    /* the client keeps working after a failure */
    TEST_ASSERT_TRUE_MESSAGE(unary_ok(), "the client did not recover from an allocation failure");
    /* streams whose RPC ended close as their data is acknowledged; a state kept for a stream
     * nghttp3 never accepted would never go away */
    for (int i = 0; i < 50 && h3c_orphan_count(&fx.client->subs[0]) > 0; i++)
      (void)unary_ok();
    TEST_ASSERT_EQUAL_size_t_MESSAGE(0, h3c_orphan_count(&fx.client->subs[0]),
                                     "request state kept for a stream nghttp3 never knew");
    if (!injected) {
      TEST_ASSERT_TRUE(ok);
      points = n;
      break;
    }
  }
  t_is_target = false;
  TEST_ASSERT_TRUE_MESSAGE(points > 0, "the injection loop did not converge");
  printf("[http3_oom] %d client allocation points\n", points);
}

int main(void) {
  /* installed before anything is allocated, and kept */
  grpc_set_allocator(nullptr);
  g_base = *grpc_get_allocator();
  grpc_set_allocator(&(grpc_allocator){
      .malloc = f_malloc,
      .calloc = f_calloc,
      .realloc = f_realloc,
      .free = g_base.free,
      .aligned_alloc = f_aligned,
  });
  UNITY_BEGIN();
  RUN_TEST(test_http3_client_allocation_failures);
  return UNITY_END();
}
