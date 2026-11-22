/* Client regression tests against a scripted raw HTTP/2 server (nghttp2), for server behaviour
 * the library's own server never produces: malformed statuses, unusable compression, early
 * GOAWAYs, responses cut off after the reply, and so on. The server records what reaches it
 * (requests, connections, RST_STREAMs, grpc-timeout and :authority). POSIX only. */
#include <arpa/inet.h>
#include <netinet/in.h>
#include <nghttp2/nghttp2.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "grpc.h"
#include "test.h"

/* ---- the scripted server ------------------------------------------------------------- */

typedef enum scenario {
  SC_OK,            /* one message, grpc-status 0 */
  SC_STATUS_ABC,    /* grpc-status: abc */
  SC_BATCH,         /* ten 8-byte messages in one DATA frame */
  SC_CUT_OFF,       /* headers + a message, then the connection closes without trailers */
  SC_UNKNOWN_CODEC, /* grpc-encoding: br with a compressed message */
  SC_NO_ENCODING,   /* a compressed message without grpc-encoding */
  SC_BAD_FLAG,      /* compressed flag 7 */
  SC_HUGE_MESSAGE,  /* a 100-byte message, never finished */
  SC_GOAWAY,        /* answers, then GOAWAY on the connection, which stays open */
  SC_HTTP_503,      /* :status 503 only */
  SC_PERCENT,       /* grpc-message: "50%25 caf%C3%A9" with status 2 */
  SC_EXACT_64,      /* one 64-byte message */
} scenario;

static struct {
  scenario scen;
  int lfd;
  pthread_t acceptor;
  bool running;
  atomic_bool stop;
  atomic_int live_threads;
  atomic_int requests;
  atomic_int connections;
  atomic_int resets;
  pthread_mutex_t lock;
  char last_timeout[64];
  char last_authority[128];
  grpc_client *client;
} fx = {.lock = PTHREAD_MUTEX_INITIALIZER};

static uint64_t now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

typedef struct conn {
  int fd;
  nghttp2_session *s;
  bool close_after_flush;
  bool goaway_sent;
} conn;

typedef struct body {
  uint8_t buf[4096];
  size_t len, off;
  const char *trailer_status; /* nullptr: no trailers */
  const char *trailer_message;
} body;

static void conn_flush(conn *c) {
  const uint8_t *d;
  ssize_t n;
  while ((n = nghttp2_session_mem_send(c->s, &d)) > 0) {
    for (ssize_t o = 0; o < n;) {
      ssize_t w = send(c->fd, d + o, (size_t)(n - o), MSG_NOSIGNAL);
      if (w <= 0)
        return;
      o += w;
    }
  }
}

static ssize_t body_read(nghttp2_session *s, int32_t sid, uint8_t *buf, size_t len, uint32_t *flags,
                         nghttp2_data_source *src, void *ud) {
  body *b = src->ptr;
  size_t n = b->len - b->off;
  if (n > len)
    n = len;
  memcpy(buf, b->buf + b->off, n);
  b->off += n;
  if (b->off == b->len) {
    *flags |= NGHTTP2_DATA_FLAG_EOF;
    if (b->trailer_status != nullptr) {
      *flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
      nghttp2_nv t[2] = {{(uint8_t *)"grpc-status", (uint8_t *)b->trailer_status, 11,
                          strlen(b->trailer_status), 0}};
      size_t nt = 1;
      if (b->trailer_message != nullptr)
        t[nt++] = (nghttp2_nv){(uint8_t *)"grpc-message", (uint8_t *)b->trailer_message, 12,
                               strlen(b->trailer_message), 0};
      nghttp2_submit_trailer(s, sid, t, nt);
    } else if (fx.scen == SC_CUT_OFF) {
      *flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
      ((conn *)ud)->close_after_flush = true;
    }
  }
  return (ssize_t)n;
}

static void add_msg(body *b, uint8_t flag, const void *p, size_t n) {
  b->buf[b->len++] = flag;
  uint32_t l = htonl((uint32_t)n);
  memcpy(b->buf + b->len, &l, 4);
  b->len += 4;
  memcpy(b->buf + b->len, p, n);
  b->len += n;
}

static body g_body;

static void respond(conn *c, int32_t sid) {
  atomic_fetch_add(&fx.requests, 1);
  memset(&g_body, 0, sizeof(g_body));
  nghttp2_nv h[3] = {{(uint8_t *)":status", (uint8_t *)"200", 7, 3, 0},
                     {(uint8_t *)"content-type", (uint8_t *)"application/grpc", 12, 16, 0}};
  size_t nh = 2;
  g_body.trailer_status = "0";
  switch (fx.scen) {
  case SC_OK:
  case SC_GOAWAY:
    add_msg(&g_body, 0, "ok", 2);
    break;
  case SC_STATUS_ABC:
    add_msg(&g_body, 0, "hi", 2);
    g_body.trailer_status = "abc";
    break;
  case SC_BATCH:
    for (int i = 0; i < 10; i++)
      add_msg(&g_body, 0, "12345678", 8);
    break;
  case SC_CUT_OFF:
    add_msg(&g_body, 0, "reply", 5);
    g_body.trailer_status = nullptr;
    break;
  case SC_UNKNOWN_CODEC:
    h[nh++] = (nghttp2_nv){(uint8_t *)"grpc-encoding", (uint8_t *)"br", 13, 2, 0};
    add_msg(&g_body, 1, "\x1b\x03\x00\xf8", 4);
    break;
  case SC_NO_ENCODING:
    add_msg(&g_body, 1, "\x1f\x8b\x08\x00", 4);
    break;
  case SC_BAD_FLAG:
    add_msg(&g_body, 7, "hi", 2);
    break;
  case SC_HUGE_MESSAGE: {
    char big[100];
    memset(big, 'A', sizeof(big));
    add_msg(&g_body, 0, big, sizeof(big));
    g_body.trailer_status = nullptr;
    /* headers and the message without END_STREAM, never finished */
    nghttp2_submit_headers(c->s, 0, sid, nullptr, h, nh, nullptr);
    nghttp2_submit_data(
        c->s, NGHTTP2_FLAG_NONE, sid,
        &(nghttp2_data_provider){.source.ptr = &g_body, .read_callback = body_read});
    return;
  }
  case SC_HTTP_503: {
    nghttp2_nv x[] = {{(uint8_t *)":status", (uint8_t *)"503", 7, 3, 0}};
    nghttp2_submit_response(c->s, sid, x, 1, nullptr);
    return;
  }
  case SC_PERCENT:
    g_body.trailer_status = "2";
    g_body.trailer_message = "50%25 caf%C3%A9";
    break;
  case SC_EXACT_64: {
    char m[64];
    memset(m, 'E', sizeof(m));
    add_msg(&g_body, 0, m, sizeof(m));
    break;
  }
  }
  nghttp2_data_provider dp = {.source.ptr = &g_body, .read_callback = body_read};
  nghttp2_submit_response(c->s, sid, h, nh, &dp);
  if (fx.scen == SC_GOAWAY && !c->goaway_sent) {
    c->goaway_sent = true;
    nghttp2_submit_goaway(c->s, 0, sid, NGHTTP2_NO_ERROR, nullptr, 0);
  }
}

static int on_frame(nghttp2_session *s, const nghttp2_frame *f, void *ud) {
  (void)s;
  if (f->hd.type == NGHTTP2_RST_STREAM)
    atomic_fetch_add(&fx.resets, 1);
  if ((f->hd.type == NGHTTP2_DATA || f->hd.type == NGHTTP2_HEADERS) &&
      (f->hd.flags & NGHTTP2_FLAG_END_STREAM))
    respond(ud, f->hd.stream_id);
  return 0;
}

static int on_header(nghttp2_session *s, const nghttp2_frame *f, const uint8_t *n, size_t nl,
                     const uint8_t *v, size_t vl, uint8_t flags, void *ud) {
  (void)s;
  (void)f;
  (void)flags;
  (void)ud;
  pthread_mutex_lock(&fx.lock);
  if (nl == 12 && memcmp(n, "grpc-timeout", 12) == 0)
    snprintf(fx.last_timeout, sizeof(fx.last_timeout), "%.*s", (int)vl, v);
  else if (nl == 10 && memcmp(n, ":authority", 10) == 0)
    snprintf(fx.last_authority, sizeof(fx.last_authority), "%.*s", (int)vl, v);
  pthread_mutex_unlock(&fx.lock);
  return 0;
}

static void *serve_conn(void *arg) {
  conn *c = arg;
  nghttp2_session_callbacks *cb;
  nghttp2_session_callbacks_new(&cb);
  nghttp2_session_callbacks_set_on_frame_recv_callback(cb, on_frame);
  nghttp2_session_callbacks_set_on_header_callback(cb, on_header);
  nghttp2_session_server_new(&c->s, cb, c);
  nghttp2_session_callbacks_del(cb);
  nghttp2_submit_settings(c->s, 0, nullptr, 0);
  conn_flush(c);
  uint8_t buf[65536];
  while (!atomic_load(&fx.stop)) {
    struct pollfd p = {c->fd, POLLIN, 0};
    int pr = poll(&p, 1, 50);
    if (pr < 0)
      break;
    if (pr == 0)
      continue;
    ssize_t n = recv(c->fd, buf, sizeof(buf), 0);
    if (n <= 0 || nghttp2_session_mem_recv(c->s, buf, (size_t)n) < 0)
      break;
    conn_flush(c);
    if (c->close_after_flush) {
      /* close cleanly: half-close and drain what the client still sends, so the kernel sends FIN
       * rather than RST (which could discard the reply before the client reads it) */
      shutdown(c->fd, SHUT_WR);
      uint64_t until = now_ms() + 500;
      while (now_ms() < until) {
        struct pollfd q = {c->fd, POLLIN, 0};
        if (poll(&q, 1, 20) > 0 && recv(c->fd, buf, sizeof(buf), 0) <= 0)
          break;
      }
      break;
    }
  }
  nghttp2_session_del(c->s);
  close(c->fd);
  free(c);
  atomic_fetch_sub(&fx.live_threads, 1);
  return nullptr;
}

static void *accept_loop(void *arg) {
  (void)arg;
  while (!atomic_load(&fx.stop)) {
    struct pollfd p = {fx.lfd, POLLIN, 0};
    if (poll(&p, 1, 50) <= 0)
      continue;
    int fd = accept(fx.lfd, nullptr, nullptr);
    if (fd < 0)
      continue;
    atomic_fetch_add(&fx.connections, 1);
    conn *c = calloc(1, sizeof(*c));
    c->fd = fd;
    atomic_fetch_add(&fx.live_threads, 1);
    pthread_t t;
    if (pthread_create(&t, nullptr, serve_conn, c) != 0) {
      atomic_fetch_sub(&fx.live_threads, 1);
      close(fd);
      free(c);
      continue;
    }
    pthread_detach(t);
  }
  return nullptr;
}

/* Starts the scripted server on a dual-stack loopback-reachable port; returns the port. */
static uint16_t start_fake(scenario scen) {
  fx.scen = scen;
  fx.lfd = socket(AF_INET6, SOCK_STREAM, 0);
  TEST_ASSERT_TRUE(fx.lfd >= 0);
  int one = 1, zero = 0;
  setsockopt(fx.lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  setsockopt(fx.lfd, IPPROTO_IPV6, IPV6_V6ONLY, &zero, sizeof(zero));
  struct sockaddr_in6 a = {.sin6_family = AF_INET6, .sin6_addr = in6addr_any};
  TEST_ASSERT_EQUAL_INT(0, bind(fx.lfd, (struct sockaddr *)&a, sizeof(a)));
  TEST_ASSERT_EQUAL_INT(0, listen(fx.lfd, 16));
  socklen_t al = sizeof(a);
  TEST_ASSERT_EQUAL_INT(0, getsockname(fx.lfd, (struct sockaddr *)&a, &al));
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&fx.acceptor, nullptr, accept_loop, nullptr));
  fx.running = true;
  return ntohs(a.sin6_port);
}

static grpc_client *fake_client(uint16_t port, grpc_client_config cfg) {
  cfg.host = cfg.host != nullptr ? cfg.host : "127.0.0.1";
  cfg.port = port;
  if (cfg.timeout_ms == 0)
    cfg.timeout_ms = 2000;
  grpc_err err = GRPC_OK;
  fx.client = grpc_client_new(&cfg, &err);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
  TEST_ASSERT_NOT_NULL(fx.client);
  return fx.client;
}

static void sleep_ms(unsigned ms) {
  struct timespec ts = {.tv_sec = ms / 1000, .tv_nsec = (long)(ms % 1000) * 1000000L};
  nanosleep(&ts, nullptr);
}

void setUp(void) {
  fx.client = nullptr;
  fx.running = false;
  fx.lfd = -1;
  atomic_store(&fx.stop, false);
  atomic_store(&fx.requests, 0);
  atomic_store(&fx.connections, 0);
  atomic_store(&fx.resets, 0);
  fx.last_timeout[0] = fx.last_authority[0] = '\0';
}

void tearDown(void) {
  grpc_client_free(fx.client);
  fx.client = nullptr;
  if (fx.running) {
    atomic_store(&fx.stop, true);
    pthread_join(fx.acceptor, nullptr);
    fx.running = false;
  }
  if (fx.lfd >= 0)
    close(fx.lfd);
  uint64_t until = now_ms() + 3000;
  while (atomic_load(&fx.live_threads) > 0 && now_ms() < until)
    sleep_ms(5);
}

/* ---- tests --------------------------------------------------------------------------- */

static grpc_err unary(grpc_client_response *r) {
  *r = (grpc_client_response){0};
  return grpc_client_unary(fx.client, "/t.Svc/M", "q", 1, nullptr, 0, 0, r);
}

/* SIGPIPE would kill the process on a write to a closed connection: the library ignores it when
 * it still has its default disposition, and keeps a handler the application installed. */
static void on_sigpipe(int sig) {
  (void)sig;
}

void test_client_sigpipe_policy(void) {
  uint16_t port = start_fake(SC_OK);
  struct sigaction mine = {.sa_handler = on_sigpipe}, cur;
  sigemptyset(&mine.sa_mask);
  sigaction(SIGPIPE, &mine, nullptr);
  fake_client(port, (grpc_client_config){0});
  sigaction(SIGPIPE, nullptr, &cur);
  TEST_ASSERT_TRUE(cur.sa_handler == on_sigpipe);
  grpc_client_free(fx.client);
  fx.client = nullptr;
  signal(SIGPIPE, SIG_DFL);
  fake_client(port, (grpc_client_config){0});
  sigaction(SIGPIPE, nullptr, &cur);
  TEST_ASSERT_TRUE(cur.sa_handler == SIG_IGN);
}

/* A grpc-status that is not a number 0..16 is UNKNOWN, not OK. */
void test_client_malformed_grpc_status(void) {
  fake_client(start_fake(SC_STATUS_ABC), (grpc_client_config){0});
  grpc_client_response r;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, unary(&r));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNKNOWN, r.status);
  grpc_client_response_free(&r);
}

/* grpc-message is percent-decoded. */
void test_client_grpc_message_percent_decoded(void) {
  fake_client(start_fake(SC_PERCENT), (grpc_client_config){0});
  grpc_client_response r;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, unary(&r));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNKNOWN, r.status);
  TEST_ASSERT_EQUAL_STRING("50% caf\xc3\xa9", r.status_message);
  grpc_client_response_free(&r);
}

/* Without grpc-status, the HTTP status maps to a gRPC status (503: UNAVAILABLE). */
void test_client_http_status_mapping(void) {
  fake_client(start_fake(SC_HTTP_503), (grpc_client_config){0});
  grpc_client_response r;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, unary(&r));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAVAILABLE, r.status);
  grpc_client_response_free(&r);
}

/* max_message_size limits each message, not each network chunk: ten small messages in one frame
 * pass, a message of exactly the limit passes. */
void test_client_receive_limit_per_message(void) {
  fake_client(start_fake(SC_BATCH), (grpc_client_config){.max_message_size = 64});
  grpc_client_response r;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, unary(&r));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, r.status);
  grpc_client_response_free(&r);
  tearDown();
  setUp();
  fake_client(start_fake(SC_EXACT_64), (grpc_client_config){.max_message_size = 64});
  TEST_ASSERT_EQUAL_INT(GRPC_OK, unary(&r));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, r.status);
  TEST_ASSERT_NOT_NULL(r.message);
  TEST_ASSERT_EQUAL_UINT32(64, r.message->len);
  grpc_client_response_free(&r);
  /* a limit beyond HTTP/2's largest window still works */
  tearDown();
  setUp();
  fake_client(start_fake(SC_OK), (grpc_client_config){.max_message_size = 0x80000000u});
  TEST_ASSERT_EQUAL_INT(GRPC_OK, unary(&r));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, r.status);
  grpc_client_response_free(&r);
}

/* A message over the limit fails the call, and the client resets the stream so the server stops
 * sending. */
void test_client_limit_resets_stream(void) {
  fake_client(start_fake(SC_HUGE_MESSAGE), (grpc_client_config){.max_message_size = 16});
  grpc_client_response r;
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_LIMIT, unary(&r));
  grpc_client_response_free(&r);
  uint64_t until = now_ms() + 2000;
  while (atomic_load(&fx.resets) == 0 && now_ms() < until)
    sleep_ms(5);
  TEST_ASSERT_TRUE_MESSAGE(atomic_load(&fx.resets) > 0, "the server was never told to stop");
}

/* Compressed responses the client cannot decode fail the call (INTERNAL) instead of reaching the
 * application as if they were plain. */
void test_client_undecodable_compression(void) {
  const scenario bad[] = {SC_UNKNOWN_CODEC, SC_NO_ENCODING, SC_BAD_FLAG};
  for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
    if (i > 0) {
      tearDown();
      setUp();
    }
    fake_client(start_fake(bad[i]), (grpc_client_config){0});
    grpc_client_response r;
    grpc_err rc = unary(&r);
    TEST_ASSERT_TRUE(rc == GRPC_ERR_CODEC || rc == GRPC_ERR_PROTOCOL);
    TEST_ASSERT_EQUAL_INT(GRPC_STATUS_INTERNAL, r.status);
    TEST_ASSERT_NULL(r.message);
    grpc_client_response_free(&r);
  }
}

/* A unary call the server already answered is not retried (it may have run), and the reply that
 * arrived is kept. */
void test_client_no_retry_after_reply(void) {
  fake_client(start_fake(SC_CUT_OFF), (grpc_client_config){.max_retries = 2});
  grpc_client_response r;
  grpc_err rc = unary(&r);
  TEST_ASSERT_TRUE(rc == GRPC_ERR_CLOSED || rc == GRPC_ERR_IO);
  TEST_ASSERT_EQUAL_INT(1, atomic_load(&fx.requests));
  TEST_ASSERT_NOT_NULL(r.message);
  TEST_ASSERT_EQUAL_MEMORY("reply", r.message->data, 5);
  grpc_client_response_free(&r);
}

/* After a GOAWAY the client opens a new connection instead of failing every later call. */
void test_client_reconnects_after_goaway(void) {
  fake_client(start_fake(SC_GOAWAY), (grpc_client_config){0});
  for (int i = 0; i < 3; i++) {
    grpc_client_response r;
    TEST_ASSERT_EQUAL_INT(GRPC_OK, unary(&r));
    TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, r.status);
    grpc_client_response_free(&r);
  }
  TEST_ASSERT_EQUAL_INT(3, atomic_load(&fx.requests));
  TEST_ASSERT_TRUE(atomic_load(&fx.connections) >= 2);
}

/* A deadline counts from the call, not from the end of the previous one (the client's loop clock
 * stops between calls). */
void test_client_deadline_after_idle(void) {
  fake_client(start_fake(SC_OK), (grpc_client_config){.timeout_ms = 1000});
  grpc_client_response r;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, unary(&r));
  grpc_client_response_free(&r);
  sleep_ms(1500);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, unary(&r));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, r.status);
  grpc_client_response_free(&r);
}

/* A request message of 4 GiB or more cannot be framed (the gRPC length prefix is 32 bits): it is
 * refused before any of it is read, instead of being sent with a truncated length. The length is
 * checked first, so a tiny buffer with a huge length is safe here. 64-bit size_t only. */
void test_client_refuses_4gib_message(void) {
  if (sizeof(size_t) <= 4)
    TEST_IGNORE_MESSAGE("size_t cannot express 4 GiB");
  fake_client(start_fake(SC_OK), (grpc_client_config){0});
  grpc_err err = GRPC_OK;
  grpc_client_stream *st = grpc_client_stream_open(fx.client, "/t.Svc/M", nullptr, 0, 2000, &err);
  TEST_ASSERT_NOT_NULL(st);
  static const char tiny[1] = {'x'};
  uint64_t huge = (uint64_t)UINT32_MAX + 1;
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_LIMIT, grpc_client_stream_send(st, tiny, (size_t)huge));
  /* the stream is still usable for a normal message */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(st, "q", 1));
  grpc_client_response r = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_finish(st, &r));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, r.status);
  grpc_client_response_free(&r);
}

/* A call with no deadline at all (none configured, none given) has no grpc-timeout and does not
 * time out. */
void test_client_no_deadline(void) {
  uint16_t port = start_fake(SC_OK);
  grpc_err err = GRPC_OK;
  fx.client = grpc_client_new(&(grpc_client_config){.host = "127.0.0.1", .port = port}, &err);
  TEST_ASSERT_NOT_NULL(fx.client);
  grpc_client_response r = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(fx.client, "/t.Svc/M", "q", 1, nullptr, 0, 0, &r));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, r.status);
  grpc_client_response_free(&r);
  pthread_mutex_lock(&fx.lock);
  bool has_timeout = fx.last_timeout[0] != '\0';
  pthread_mutex_unlock(&fx.lock);
  TEST_ASSERT_FALSE(has_timeout);
}

/* Header values the spec constrains: grpc-timeout has at most 8 digits, and an IPv6 authority
 * is bracketed. max_retries = UINT32_MAX still makes an attempt. */
void test_client_request_headers(void) {
  uint16_t port = start_fake(SC_OK);
  fake_client(port, (grpc_client_config){.max_retries = UINT32_MAX});
  grpc_client_response r = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(fx.client, "/t.Svc/M", "q", 1, nullptr, 0, 4000000000u, &r));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, r.status);
  grpc_client_response_free(&r);
  TEST_ASSERT_EQUAL_INT(1, atomic_load(&fx.requests));
  char timeout[64], authority[128];
  pthread_mutex_lock(&fx.lock);
  snprintf(timeout, sizeof(timeout), "%s", fx.last_timeout);
  pthread_mutex_unlock(&fx.lock);
  TEST_ASSERT_EQUAL_STRING("4000000S", timeout);
  grpc_client_free(fx.client);
  fx.client = nullptr;
  /* over IPv6, when the host has it */
  grpc_err err = GRPC_OK;
  fx.client =
      grpc_client_new(&(grpc_client_config){.host = "::1", .port = port, .timeout_ms = 2000}, &err);
  if (fx.client == nullptr)
    return; /* no IPv6 loopback here */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, unary(&r));
  grpc_client_response_free(&r);
  pthread_mutex_lock(&fx.lock);
  snprintf(authority, sizeof(authority), "%s", fx.last_authority);
  pthread_mutex_unlock(&fx.lock);
  char want[64];
  snprintf(want, sizeof(want), "[::1]:%u", port);
  TEST_ASSERT_EQUAL_STRING(want, authority);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_client_sigpipe_policy);
  RUN_TEST(test_client_malformed_grpc_status);
  RUN_TEST(test_client_grpc_message_percent_decoded);
  RUN_TEST(test_client_http_status_mapping);
  RUN_TEST(test_client_receive_limit_per_message);
  RUN_TEST(test_client_limit_resets_stream);
  RUN_TEST(test_client_undecodable_compression);
  RUN_TEST(test_client_no_retry_after_reply);
  RUN_TEST(test_client_reconnects_after_goaway);
  RUN_TEST(test_client_deadline_after_idle);
  RUN_TEST(test_client_request_headers);
  RUN_TEST(test_client_no_deadline);
  RUN_TEST(test_client_refuses_4gib_message);
  return UNITY_END();
}
