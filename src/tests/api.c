#include "internal/grpc_threads.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "grpc.h"
#include "test.h"

/* the public headers must not leak macros into the application */
#if defined(malloc) || defined(free) || defined(calloc) || defined(realloc) || defined(strdup)
#error "grpc.h redefines allocation functions"
#endif
#if defined(likely) || defined(unlikely) || defined(REALIGN) || defined(COLSTR) || \
    defined(LOG_ERROR) || defined(CONFIG_STREAM_MAX_HEADERS)
#error "grpc.h leaks internal macros"
#endif

#include "core.h"
#include "internal_helpers.h"

void setUp(void) {}
void tearDown(void) {}

static void handler([[maybe_unused]] grpc_call *call, [[maybe_unused]] const grpc_message *request,
                    [[maybe_unused]] void *userdata) {}

static void on_message([[maybe_unused]] grpc_call *call,
                       [[maybe_unused]] const grpc_message *message,
                       [[maybe_unused]] void *userdata) {}

static void on_start([[maybe_unused]] grpc_call *call, [[maybe_unused]] void *userdata) {}

void test_config_validation(void) {
  grpc_err err = GRPC_OK;
  /* nullptr config is invalid */
  TEST_ASSERT_NULL(grpc_server_new_ex(nullptr, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  /* port = 0 binds an OS-assigned ephemeral port */
  grpc_server *ephemeral = grpc_server_new_ex(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF}, &err);
  TEST_ASSERT_NOT_NULL(ephemeral);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
  TEST_ASSERT_TRUE(grpc_server_port(ephemeral) > 0);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_drain(ephemeral, 50));
  grpc_server_free(ephemeral);
  TEST_ASSERT_EQUAL_UINT16(0, grpc_server_port(nullptr));

  /* invalid addresses and limits (rejected before anything is bound) */
  TEST_ASSERT_NULL(grpc_server_new_ex(
      &(grpc_config){.port = 0, .ipv4_addr = "not an address", .log_level = GRPC_LOG_OFF}, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  TEST_ASSERT_NULL(grpc_server_new_ex(
      &(grpc_config){.port = 0, .max_frame_size = 100, .log_level = GRPC_LOG_OFF}, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  /* TLS needs both files or both PEMs */
  TEST_ASSERT_NULL(grpc_server_new_ex(
      &(grpc_config){.port = 0, .tls.cert_file = "cert.pem", .log_level = GRPC_LOG_OFF}, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  TEST_ASSERT_NULL(grpc_server_new_ex(
      &(grpc_config){.port = 0, .tls.cert_pem = "pem", .log_level = GRPC_LOG_OFF}, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);

  /* a configured port is reported as is; take one the OS just handed out rather than a fixed
   * number that concurrent runs could share */
  grpc_server *probe = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(probe);
  uint16_t fixed_port = grpc_server_port(probe);
  TEST_ASSERT_TRUE(fixed_port > 0);
  grpc_server_free(probe);
  /* only the given address family */
  grpc_server *server = grpc_server_new(
      &(grpc_config){.port = fixed_port, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_UINT16(fixed_port, grpc_server_port(server));
  grpc_server_free(server);

  grpc_server_free(nullptr);
}

void test_routes(void) {
  grpc_server *server = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(server);

  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/a.B/C", handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_EXISTS, grpc_server_handle(server, "/a.B/C", handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_handle(server, "no-slash", handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_handle(server, "/a.B/D", nullptr, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_handle_stream(
                                              server, "/a.B/D", &(grpc_stream_handler){}, nullptr));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle_stream(
                   server, "/a.B/D", &(grpc_stream_handler){.on_message = on_message}, nullptr));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle_stream(server, "/a.B/StartOnly",
                                         &(grpc_stream_handler){.on_start = on_start}, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_register_path(server, "/a.B/E", nullptr));

  /* more methods than the old fixed limit of 10 */
  for (int i = 0; i < 100; i++) {
    char path[32];
    snprintf(path, sizeof(path), "/many.S/M%d", i);
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, path, handler, nullptr));
  }

  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_unregister(server, "/a.B/C"));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_NOT_FOUND, grpc_server_unregister(server, "/a.B/C"));
  /* a disabled path can be registered again */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/a.B/C", handler, nullptr));

  grpc_server_free(server);
}

void test_poll_idle(void) {
  grpc_server *server = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(0, grpc_server_poll(server, 0));
  TEST_ASSERT_EQUAL_INT(0, grpc_server_poll(server, 10));
  size_t count = 1;
  grpc_server_events(server, &count);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)count);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_poll(nullptr, 0));
  grpc_server_free(server);
}

typedef struct thread_arg {
  grpc_server *server;
  int *counter;
} thread_arg;

static void count(void *arg) {
  (*(int *)arg)++;
}

static void *post_and_stop(void *arg) {
  thread_arg *a = arg;
  for (int i = 0; i < 100; i++)
    grpc_server_post(a->server, count, a->counter);
  grpc_server_stop(a->server);
  return nullptr;
}

void test_post_and_stop_from_another_thread(void) {
  grpc_server *server = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(server);

  int counter = 0;
  thread_arg arg = {server, &counter};
  pthread_t thread;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&thread, nullptr, post_and_stop, &arg));
  /* returns once the other thread stops it */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_run(server));
  pthread_join(thread, nullptr);
  /* run on the server thread: whatever ran before the stop, the rest at free */
  grpc_server_free(server);
  TEST_ASSERT_EQUAL_INT(100, counter);
}

#ifndef _WIN32
#include <signal.h>

/* The disposition of signum, without changing it. */
static void (*signal_disposition(int signum))(int) {
  struct sigaction current;
  sigaction(signum, nullptr, &current);
  return current.sa_handler;
}

void test_stop_on_signals(void) {
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_stop_on_signals(nullptr));
  TEST_ASSERT_FALSE(grpc_server_stop_requested(nullptr));

  grpc_server *server = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_stop_on_signals(server));
  /* calling it again is harmless */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_stop_on_signals(server));
  TEST_ASSERT_FALSE(grpc_server_stop_requested(server));
  TEST_ASSERT_TRUE(signal_disposition(SIGINT) != SIG_DFL);

  for (int round = 0; round < 2; round++) {
    int signum = round == 0 ? SIGINT : SIGTERM;
    /* handled on the loop: grpc_server_run() returns */
    TEST_ASSERT_EQUAL_INT(0, raise(signum));
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_run(server));
    /* run cleared the stop flag; both signals have their default action again */
    TEST_ASSERT_FALSE(grpc_server_stop_requested(server));
    TEST_ASSERT_TRUE(signal_disposition(SIGINT) == SIG_DFL);
    TEST_ASSERT_TRUE(signal_disposition(SIGTERM) == SIG_DFL);
    /* watched once more for the next round */
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_stop_on_signals(server));
  }

  /* a poll loop sees the stop */
  TEST_ASSERT_EQUAL_INT(0, raise(SIGINT));
  for (int i = 0; i < 100 && !grpc_server_stop_requested(server); i++)
    TEST_ASSERT_TRUE(grpc_server_poll(server, 100) >= 0);
  TEST_ASSERT_TRUE(grpc_server_stop_requested(server));

  /* free closes the watchers while they are active */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_stop_on_signals(server));
  grpc_server_free(server);
  TEST_ASSERT_TRUE(signal_disposition(SIGINT) == SIG_DFL);
}
#endif

static size_t read_log(FILE *file, char *buffer, size_t size) {
  fflush(file);
  rewind(file);
  size_t n = fread(buffer, 1, size - 1, file);
  buffer[n] = '\0';
  return n;
}

#ifdef _WIN32
#include <io.h>
#define dup    _dup
#define dup2   _dup2
#define close  _close
#define fileno _fileno
#else
#include <unistd.h>
#endif

/* Runs ezdump(data, len) with stdout redirected to a temporary file and reads what it printed
 * into buffer. Returns false if the redirection could not be set up. */
static bool capture_stdout_dump(void *data, size_t len, char *buffer, size_t size) {
  extern void ezdump(void *vdata, size_t len);
  FILE *capture = tmpfile();
  if (capture == nullptr)
    return false;
  fflush(stdout);
  int saved = dup(fileno(stdout));
  if (saved < 0 || dup2(fileno(capture), fileno(stdout)) < 0) {
    if (saved >= 0)
      close(saved);
    fclose(capture);
    return false;
  }
  ezdump(data, len);
  fflush(stdout);
  dup2(saved, fileno(stdout));
  close(saved);
  read_log(capture, buffer, size);
  fclose(capture);
  return true;
}

void test_log_level_control(void) {
  FILE *first_log = tmpfile();
  FILE *second_log = tmpfile();
  TEST_ASSERT_NOT_NULL(first_log);
  TEST_ASSERT_NOT_NULL(second_log);

  grpc_server *first = grpc_server_new(&(grpc_config){
      .port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_WARNING, .log_file = first_log});
  TEST_ASSERT_NOT_NULL(first);
  grpc_server *second = grpc_server_new(&(grpc_config){
      .port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_ERROR, .log_file = second_log});
  TEST_ASSERT_NOT_NULL(second);

  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_set_log_level(nullptr, GRPC_LOG_INFO));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_set_log_level(first, (grpc_log_level)-1));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_set_log_level(first, (grpc_log_level)7));

  server_log(first, GRPC_LOG_INFO, "filtered-first\n");
  server_log(first, GRPC_LOG_WARNING, "warning-first\n");
  server_log(second, GRPC_LOG_WARNING, "filtered-second\n");
  server_log(second, GRPC_LOG_ERROR, "error-second\n");
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_set_log_level(first, GRPC_LOG_DEBUG));
  server_log(first, GRPC_LOG_DEBUG, "debug-first\n");
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_set_log_level(second, GRPC_LOG_OFF));
  server_log(second, GRPC_LOG_ERROR, "filtered-off\n");

  char first_contents[4096], second_contents[4096];
  read_log(first_log, first_contents, sizeof(first_contents));
  read_log(second_log, second_contents, sizeof(second_contents));
  TEST_ASSERT_NOT_NULL(strstr(first_contents, "warning-first"));
  TEST_ASSERT_NOT_NULL(strstr(first_contents, "debug-first"));
  TEST_ASSERT_NULL(strstr(first_contents, "filtered-first"));
  TEST_ASSERT_NULL(strstr(first_contents, "error-second"));
  TEST_ASSERT_NOT_NULL(strstr(second_contents, "error-second"));
  TEST_ASSERT_NULL(strstr(second_contents, "filtered-second"));
  TEST_ASSERT_NULL(strstr(second_contents, "filtered-off"));

  grpc_server_free(first);
  grpc_server_free(second);
  fclose(first_log);
  fclose(second_log);
}

void test_strings(void) {
  /* every defined code has its own message, not the fallback */
  for (int e = GRPC_OK; e >= GRPC_ERR_TIMEOUT; e--) {
    const char *str = grpc_strerror((grpc_err)e);
    TEST_ASSERT_NOT_NULL(str);
    TEST_ASSERT_TRUE_MESSAGE(strcmp(str, "unknown error") != 0, "grpc_err without a message");
  }
  TEST_ASSERT_EQUAL_STRING("success", grpc_strerror(GRPC_OK));
  TEST_ASSERT_EQUAL_STRING("already exists", grpc_strerror(GRPC_ERR_EXISTS));
  TEST_ASSERT_EQUAL_STRING("output limit exceeded", grpc_strerror(GRPC_ERR_LIMIT));
  TEST_ASSERT_EQUAL_STRING("compression or decompression failed", grpc_strerror(GRPC_ERR_CODEC));
  TEST_ASSERT_EQUAL_STRING("operation timed out", grpc_strerror(GRPC_ERR_TIMEOUT));
  TEST_ASSERT_EQUAL_STRING("unknown error", grpc_strerror((grpc_err)-1000));

  for (int s = GRPC_STATUS_OK; s <= GRPC_STATUS_UNAUTHENTICATED; s++) {
    const char *str = grpc_status_str((grpc_status)s);
    TEST_ASSERT_NOT_NULL(str);
    TEST_ASSERT_TRUE_MESSAGE(strcmp(str, "(null)") != 0, "grpc_status without a name");
  }
  TEST_ASSERT_EQUAL_STRING("invalid argument", grpc_status_str(GRPC_STATUS_INVALID_ARGUMENT));
  TEST_ASSERT_EQUAL_STRING("aborted", grpc_status_str(GRPC_STATUS_ABORTED));
  TEST_ASSERT_EQUAL_STRING("(null)", grpc_status_str((grpc_status)999));
  /* grpc_license() must match the LICENSE file byte for byte */
  FILE *lf = fopen(GRPC_LICENSE_FILE, "rb");
  TEST_ASSERT_NOT_NULL_MESSAGE(lf, GRPC_LICENSE_FILE);
  static char license_file[4096];
  size_t license_len = fread(license_file, 1, sizeof(license_file) - 1, lf);
  fclose(lf);
  license_file[license_len] = '\0';
  /* a Windows checkout may have CRLF line endings */
  size_t w = 0;
  for (size_t r = 0; r < license_len; r++)
    if (license_file[r] != '\r')
      license_file[w++] = license_file[r];
  license_file[w] = '\0';
  TEST_ASSERT_EQUAL_STRING(license_file, grpc_license());

  /* Internal debug formatting helpers */
  uint8_t sample[20];
  for (size_t i = 0; i < sizeof(sample); i++)
    sample[i] = (uint8_t)(i + 0x1e);
  FILE *f = tmpfile();
  TEST_ASSERT_NOT_NULL(f);
  ezflog(f, "helper test %d\n", 1);
  char logged[256];
  read_log(f, logged, sizeof(logged));
  fclose(f);
  /* "[<timestamp>] helper test 1\n" */
  TEST_ASSERT_EQUAL_CHAR('[', logged[0]);
  char *close_bracket = strstr(logged, "] ");
  TEST_ASSERT_NOT_NULL(close_bracket);
  TEST_ASSERT_TRUE(close_bracket > logged + 1);
  TEST_ASSERT_EQUAL_STRING("helper test 1\n", close_bracket + 2);

  /* ezdump writes to stdout: point stdout at a temporary file while it runs (no assertion in
   * between, since the test harness prints to stdout too) */
  char dumped[512];
  TEST_ASSERT_TRUE(capture_stdout_dump(sample, sizeof(sample), dumped, sizeof(dumped)));
  TEST_ASSERT_EQUAL_STRING(
      "1e 1f 20 21 22 23 24 25  26 27 28 29 2a 2b 2c 2d | .. !\"#$%&'()*+,- |\n"
      "2e 2f 30 31                                      | ./01             |\n",
      dumped);
  TEST_ASSERT_TRUE(capture_stdout_dump(sample, 0, dumped, sizeof(dumped)));
  TEST_ASSERT_EQUAL_STRING("", dumped);

  /* Bind failure on non-local address + GRPC_LOG_TRACE + channelz error paths */
  /* 192.0.2.1 (TEST-NET-1) is normally not a local address, but the bind succeeds where
   * net.ipv4.ip_nonlocal_bind=1 (or the address is configured): then there is nothing to check */
  grpc_err err = GRPC_OK;
  grpc_server *nonlocal = grpc_server_new_ex(
      &(grpc_config){.port = 0, .ipv4_addr = "192.0.2.1", .log_level = GRPC_LOG_OFF}, &err);
  if (nonlocal == nullptr) {
    TEST_ASSERT_EQUAL_INT(GRPC_ERR_IO, err);
  } else {
    TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
    grpc_server_free(nonlocal);
  }
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_channelz_enable(nullptr));
  grpc_server *cz = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_TRACE});
  TEST_ASSERT_NOT_NULL(cz);
  server_log(cz, GRPC_LOG_TRACE, "trace log line\n");
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_channelz_enable(cz));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_EXISTS, grpc_channelz_enable(cz));
  grpc_server_free(cz);
}

void test_messages(void) {
  grpc_message *msg = grpc_message_new("hello", 5);
  TEST_ASSERT_NOT_NULL(msg);
  TEST_ASSERT_EQUAL_UINT32(5, msg->len);
  TEST_ASSERT_EQUAL_UINT8(0, msg->cflag);
  TEST_ASSERT_EQUAL_MEMORY("hello", msg->data, 5);
  TEST_ASSERT_EQUAL_UINT64(0, (uintptr_t)msg->data % alignof(grpc_max_align_t));
  grpc_message_free(msg);

  msg = grpc_message_alloc(1000);
  TEST_ASSERT_NOT_NULL(msg);
  TEST_ASSERT_EQUAL_UINT32(1000, msg->len);
  memset(msg->data, 0xab, 1000);
  grpc_message_free(msg);

  TEST_ASSERT_NULL(grpc_message_new(nullptr, 5));
  msg = grpc_message_new(nullptr, 0);
  TEST_ASSERT_NOT_NULL(msg);
  grpc_message_free(msg);
  grpc_message_free(nullptr);
}

void test_call_null_safety(void) {
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_finish(nullptr, GRPC_STATUS_OK, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_send(nullptr, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_send_data(nullptr, "x", 1));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_reply(nullptr, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_reply_data(nullptr, "x", 1));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID,
                        grpc_call_set_compression(nullptr, GRPC_COMPRESS_GZIP, -1));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_send_async(nullptr, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_send_data_async(nullptr, "x", 1));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_reply_async(nullptr, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_send_metadata_async(nullptr, nullptr, 0));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_add_trailers_async(nullptr, nullptr, 0));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_finish_with_details_async(
                                              nullptr, GRPC_STATUS_INTERNAL, nullptr, nullptr, 0));
  TEST_ASSERT_NULL(grpc_call_metadata(nullptr, "key", nullptr));
  size_t idx = 0;
  TEST_ASSERT_NULL(grpc_call_metadata_next(nullptr, "key", &idx, nullptr));
  TEST_ASSERT_NULL(grpc_call_peer_cert_subject(nullptr));
  TEST_ASSERT_EQUAL_INT64(-1, grpc_call_deadline(nullptr));
  TEST_ASSERT_FALSE(grpc_call_is_cancelled(nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_drain(nullptr, 0));
  grpc_call_unref(nullptr);
}

/* Size arithmetic that would wrap fails instead of producing a small buffer. Each case used to
 * hang (buf_reserve doubling past SIZE_MAX) or overrun the heap (a wrapped allocation size). */
void test_size_safety(void) {
  size_t r = 7;
  TEST_ASSERT_FALSE(grpc_size_add(1, 2, &r));
  TEST_ASSERT_EQUAL_size_t(3, r);
  TEST_ASSERT_TRUE(grpc_size_add(SIZE_MAX, 1, &r));
  TEST_ASSERT_EQUAL_size_t(3, r); /* unchanged on overflow */
  TEST_ASSERT_FALSE(grpc_size_mul(0, SIZE_MAX, &r));
  TEST_ASSERT_EQUAL_size_t(0, r);
  TEST_ASSERT_TRUE(grpc_size_mul(SIZE_MAX / 2 + 1, 2, &r));
  TEST_ASSERT_NULL(grpc_malloc_array(SIZE_MAX / 4 + 1, 8));
  int *p = grpc_malloc_array(4, sizeof(int));
  TEST_ASSERT_NOT_NULL(p);
  TEST_ASSERT_NULL(grpc_realloc_array(p, SIZE_MAX / 2 + 1, sizeof(int)));
  p[3] = 1; /* still owned after the refused realloc */
  grpc_free(p);

  grpc_buf b = {0};
  TEST_ASSERT_EQUAL_INT(0, buf_append(&b, nullptr, 0)); /* no memcpy from a null pointer */
  TEST_ASSERT_NULL(b.data);
  TEST_ASSERT_EQUAL_INT(0, buf_append(&b, "0123456789", 10));
  TEST_ASSERT_EQUAL_INT(1, buf_reserve(&b, SIZE_MAX - 5)); /* len + extra wraps */
  TEST_ASSERT_EQUAL_size_t(10, b.len);
  TEST_ASSERT_EQUAL_MEMORY("0123456789", b.data, 10);
  buf_free(&b);

  /* a length the data cannot have: refused before anything is read */
  const uint8_t small[4] = {1, 2, 3, 4};
  size_t olen = 0;
  TEST_ASSERT_NULL(grpc_metadata_encode_bin(small, SIZE_MAX, &olen));
  TEST_ASSERT_NULL(grpc_metadata_encode_bin(small, SIZE_MAX / 4 * 3, &olen));
  char *ok = grpc_metadata_encode_bin(small, sizeof(small), &olen);
  TEST_ASSERT_EQUAL_STRING("AQIDBA==", ok);
  grpc_free(ok);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_config_validation);
  RUN_TEST(test_routes);
  RUN_TEST(test_poll_idle);
  RUN_TEST(test_post_and_stop_from_another_thread);
#ifndef _WIN32
  RUN_TEST(test_stop_on_signals);
#endif
  RUN_TEST(test_log_level_control);
  RUN_TEST(test_strings);
  RUN_TEST(test_messages);
  RUN_TEST(test_call_null_safety);
  RUN_TEST(test_size_safety);
  return UNITY_END();
}
