#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#endif
#include "internal/grpc_threads.h"
#include "grpc.h"
#include "test.h"
#include "core.h"
#if defined(GRPC_TLS_GNUTLS)
#include <gnutls/gnutls.h>
#endif
#if defined(GRPC_TLS_RUSTLS)
#include <rustls.h>
#include "internal_helpers.h"
#include "internal_tls.h"
#endif
#if defined(GRPC_TLS_OPENSSL) || defined(GRPC_TLS_LIBRESSL) || defined(GRPC_TLS_BORINGSSL) || \
    defined(GRPC_TLS_AWSLC)
#define TLS_OPENSSL_FAMILY 1
#include <openssl/err.h>
#include <openssl/ssl.h>
#include "internal_tls.h"
#endif

/* What a test shares with its server thread, and what it allocates, lives here rather than on its
 * stack: tearDown() stops, joins and frees what a failed assertion left behind. The handler runs
 * on the server thread and records the peer subject under `lock`. */
static struct {
  grpc_server *server;
  pthread_t th;
  bool running;
  grpc_client *client;
  pthread_mutex_t lock;
  char subject[256];
  char *bufs[4];
} fx = {.lock = PTHREAD_MUTEX_INITIALIZER};

static void fixture_cleanup(void) {
  if (fx.running) {
    grpc_server_stop(fx.server);
    pthread_join(fx.th, nullptr);
    fx.running = false;
  }
  grpc_client_free(fx.client);
  fx.client = nullptr;
  grpc_server_free(fx.server);
  fx.server = nullptr;
  for (size_t i = 0; i < sizeof(fx.bufs) / sizeof(fx.bufs[0]); i++) {
    free(fx.bufs[i]);
    fx.bufs[i] = nullptr;
  }
  pthread_mutex_lock(&fx.lock);
  fx.subject[0] = '\0';
  pthread_mutex_unlock(&fx.lock);
}

void setUp(void) {
  fixture_cleanup();
}
void tearDown(void) {
  fixture_cleanup();
}

static void *run_server(void *arg) {
  (void)grpc_server_run((grpc_server *)arg);
  return nullptr;
}

/* Runs fx.server on fx.th. */
static void server_start(void) {
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&fx.th, nullptr, run_server, fx.server));
  fx.running = true;
}

/* Stops fx.th: the test thread owns the server again. */
static void server_stop_thread(void) {
  if (fx.running) {
    grpc_server_stop(fx.server);
    pthread_join(fx.th, nullptr);
    fx.running = false;
  }
}

static void subject_read(char *out, size_t out_len) {
  pthread_mutex_lock(&fx.lock);
  snprintf(out, out_len, "%s", fx.subject);
  pthread_mutex_unlock(&fx.lock);
}

/* Echoes and records the verified client certificate subject. */
static void mtls_echo(grpc_call *call, const grpc_message *req, void *userdata) {
  (void)userdata;
  const char *subj = grpc_call_peer_cert_subject(call);
  pthread_mutex_lock(&fx.lock);
  snprintf(fx.subject, sizeof(fx.subject), "%s", subj != nullptr ? subj : "");
  pthread_mutex_unlock(&fx.lock);
  (void)grpc_call_reply_data(call, req->data, req->len);
}

static grpc_server *new_tls_server(const char *cert, const char *key, grpc_err *err) {
  return grpc_server_new_ex(
      &(grpc_config){
          .port = 0,
          .ipv4_addr = "127.0.0.1",
          .tls = {.cert_file = cert, .key_file = key},
          .log_level = GRPC_LOG_OFF,
      },
      err);
}

void test_tls_backend(void) {
  /* the library must be built with the backend the build asked for */
  TEST_ASSERT_EQUAL_STRING(EXPECTED_TLS_BACKEND, grpc_tls_backend());
  TEST_ASSERT_TRUE(strlen(grpc_tls_backend_version()) > 0);
  printf("TLS backend: %s (%s)\n", grpc_tls_backend(), grpc_tls_backend_version());
}

void test_tls_invalid_files(void) {
  /* port 0 always binds, so a failure here is the TLS configuration's */
  grpc_err err = GRPC_OK;
  TEST_ASSERT_NULL(new_tls_server("nonexistent_cert.pem", "nonexistent_key.pem", &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  /* a key that does not belong to the certificate */
  err = GRPC_OK;
  TEST_ASSERT_NULL(new_tls_server("test_cert.pem", "test_cert.pem", &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  /* and the same server with a matching key starts */
  fx.server = new_tls_server("test_cert.pem", "test_key.pem", &err);
  TEST_ASSERT_NOT_NULL(fx.server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
  grpc_server_free(fx.server);
  fx.server = nullptr;

  /* with both ca_file and ca_pem, every backend uses ca_file (ca_pem only without it): a valid
   * file and an unusable PEM still start (rustls used to take ca_pem) */
  fx.server = grpc_server_new_ex(&(grpc_config){.port = 0,
                                                .ipv4_addr = "127.0.0.1",
                                                .tls = {.cert_file = "test_cert.pem",
                                                        .key_file = "test_key.pem",
                                                        .ca_file = "test_cert.pem",
                                                        .ca_pem = "not a certificate",
                                                        .require_client_cert = true},
                                                .log_level = GRPC_LOG_OFF},
                                 &err);
  TEST_ASSERT_NOT_NULL_MESSAGE(fx.server, "ca_pem was used although ca_file is set");
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
}

static void *fail_realloc([[maybe_unused]] void *p, [[maybe_unused]] size_t n) {
  return nullptr;
}

void test_tls_server(void) {
  grpc_err err = GRPC_OK;
  grpc_server *server = fx.server = new_tls_server("test_cert.pem", "test_key.pem", &err);
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_NOT_NULL(grpc_server_tls_native(server));

  /* Hot-reload with valid PEMs succeeds and swaps the native context; invalid fails safely. */
  void *old_ctx = grpc_server_tls_native(server);
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_INVALID,
      grpc_server_reload_tls(server, &(grpc_tls_config){.cert_file = "nonexistent.pem",
                                                        .key_file = "nonexistent.pem"}));
  TEST_ASSERT_EQUAL_PTR(old_ctx, grpc_server_tls_native(server));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_reload_tls(server, &(grpc_tls_config){.cert_file = "test_cert.pem",
                                                                 .key_file = "test_key.pem"}));
  TEST_ASSERT_NOT_NULL(grpc_server_tls_native(server));
  TEST_ASSERT_TRUE(grpc_server_tls_native(server) != old_ctx);

  /* A reload is all or nothing: when it fails, here because reallocations fail (where that
   * surfaces depends on the backend: growing the retired list, or building the new context when
   * the backend loads certificates through the grpc allocator, as rustls does), the active
   * context is left untouched. Room to retire a context is reserved ahead, so it may also
   * succeed; then it switched completely. */
  old_ctx = grpc_server_tls_native(server);
  grpc_set_allocator(&(grpc_allocator){.realloc = fail_realloc});
  grpc_err nomem = grpc_server_reload_tls(
      server, &(grpc_tls_config){.cert_file = "test_cert.pem", .key_file = "test_key.pem"});
  grpc_set_allocator(nullptr);
  if (nomem == GRPC_OK) {
    TEST_ASSERT_TRUE(grpc_server_tls_native(server) != old_ctx);
  } else {
    TEST_ASSERT_TRUE(nomem == GRPC_ERR_NOMEM || nomem == GRPC_ERR_INVALID);
    TEST_ASSERT_EQUAL_PTR(old_ctx, grpc_server_tls_native(server));
  }

  /* replaced contexts without connections are freed at once: reloads do not pile them up */
  for (int i = 0; i < 50; i++)
    TEST_ASSERT_EQUAL_INT(
        GRPC_OK, grpc_server_reload_tls(server, &(grpc_tls_config){.cert_file = "test_cert.pem",
                                                                   .key_file = "test_key.pem"}));
  TEST_ASSERT_EQUAL_size_t(1, tls_ctx_pool_count(&server->tls_pool));

  grpc_server_free(server);
  fx.server = nullptr;

  server = fx.server = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_NULL(grpc_server_tls_native(server));
  /* a reload never turns a plaintext server into a TLS one */
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_STATE,
      grpc_server_reload_tls(
          server, &(grpc_tls_config){.cert_file = "test_cert.pem", .key_file = "test_key.pem"}));
  TEST_ASSERT_NULL(grpc_server_tls_native(server));
}

static char *slurp_file(const char *path, size_t *out_len) {
  FILE *fp = fopen(path, "rb");
  if (fp == nullptr)
    return nullptr;
  fseek(fp, 0, SEEK_END);
  long sz = ftell(fp);
  rewind(fp);
  if (sz <= 0) {
    fclose(fp);
    return nullptr;
  }
  char *buf = malloc((size_t)sz + 1);
  if (buf == nullptr) {
    fclose(fp);
    return nullptr;
  }
  size_t n = fread(buf, 1, (size_t)sz, fp);
  fclose(fp);
  buf[n] = '\0';
  if (out_len != nullptr)
    *out_len = n;
  return buf;
}

void test_tls_in_memory_pem_and_mtls(void) {
  size_t cert_len = 0, key_len = 0;
  char *cert_pem = fx.bufs[0] = slurp_file("test_cert.pem", &cert_len);
  char *key_pem = fx.bufs[1] = slurp_file("test_key.pem", &key_len);
  TEST_ASSERT_NOT_NULL(cert_pem);
  TEST_ASSERT_NOT_NULL(key_pem);

  grpc_server *server = fx.server = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .tls =
          {
              .cert_pem = cert_pem,
              .cert_pem_len = cert_len,
              .key_pem = key_pem,
              .key_pem_len = key_len,
              .ca_pem = cert_pem,
              .ca_pem_len = cert_len,
              .require_client_cert = true,
          },
      .log_level = GRPC_LOG_OFF,
  });
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_NOT_NULL(grpc_server_tls_native(server));

#ifdef TLS_OPENSSL_FAMILY
  /* An mTLS client against the in-memory-PEM server: the in-memory CA verifies the client
   * certificate, whose subject reaches the handler. (The client's TLS needs an OpenSSL-family
   * backend.) */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/mtls.Svc/Echo", mtls_echo, nullptr));
  server_start();
  grpc_err err = GRPC_OK;
  fx.client = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .timeout_ms = 3000,
          .tls =
              {
                  .enable = true,
                  .ca_file = "test_cert.pem",
                  .cert_file = "test_cert.pem",
                  .key_file = "test_key.pem",
                  .server_name = "localhost",
              },
      },
      &err);
  TEST_ASSERT_NOT_NULL(fx.client);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(fx.client, "/mtls.Svc/Echo", "pem", 3, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32(3, resp.message->len);
  TEST_ASSERT_EQUAL_MEMORY("pem", resp.message->data, 3);
  grpc_client_response_free(&resp);
  char subject[256];
  subject_read(subject, sizeof(subject));
  TEST_ASSERT_NOT_NULL(strstr(subject, "localhost"));
  /* the reloads below run on this thread, so it must own the server again */
  server_stop_thread();
  /* a reload keeps the old context while a connection made from it is open, and frees it with
   * that connection */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_reload_tls(server, &(grpc_tls_config){.cert_file = "test_cert.pem",
                                                                 .key_file = "test_key.pem"}));
  TEST_ASSERT_EQUAL_size_t(2, tls_ctx_pool_count(&server->tls_pool));
  grpc_client_free(fx.client);
  fx.client = nullptr;
  for (int i = 0; i < 200 && tls_ctx_pool_count(&server->tls_pool) > 1; i++)
    (void)grpc_server_poll(server, 10);
  TEST_ASSERT_EQUAL_size_t(1, tls_ctx_pool_count(&server->tls_pool));
#endif

  /* Hot-reload with valid PEMs succeeds and swaps the native context; invalid fails safely. */
  void *old_ctx = grpc_server_tls_native(server);
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_INVALID,
      grpc_server_reload_tls(server, &(grpc_tls_config){.cert_file = "nonexistent.pem",
                                                        .key_file = "nonexistent.pem"}));
  TEST_ASSERT_EQUAL_PTR(old_ctx, grpc_server_tls_native(server));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_reload_tls(server, &(grpc_tls_config){.cert_file = "test_cert.pem",
                                                                 .key_file = "test_key.pem"}));
  TEST_ASSERT_NOT_NULL(grpc_server_tls_native(server));
  TEST_ASSERT_TRUE(grpc_server_tls_native(server) != old_ctx);

  /* Reload validation edge cases + auto-strlen in-memory PEM reload */
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_reload_tls(nullptr, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_reload_tls(server, &(grpc_tls_config){0}));
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_INVALID,
      grpc_server_reload_tls(server,
                             &(grpc_tls_config){.cert_file = "test_cert.pem", .cert_pem = "x"}));
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_INVALID,
      grpc_server_reload_tls(server, &(grpc_tls_config){.cert_file = "test_cert.pem"}));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID,
                        grpc_server_reload_tls(server, &(grpc_tls_config){.cert_pem = "x"}));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_reload_tls(server, &(grpc_tls_config){.cert_pem = cert_pem,
                                                                          .key_pem = key_pem,
                                                                          .ca_pem = cert_pem}));
}

#if defined(GRPC_TLS_OPENSSL) || defined(GRPC_TLS_LIBRESSL) || defined(GRPC_TLS_BORINGSSL) || \
    defined(GRPC_TLS_AWSLC)
#ifndef _WIN32
/* Opens a TCP connection to 127.0.0.1:port with a receive timeout. Returns the fd or -1. */
static int connect_local(uint16_t port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;
  struct timeval tv = {.tv_sec = 5};
  (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  struct sockaddr_in sin = {.sin_family = AF_INET, .sin_port = htons(port)};
  inet_pton(AF_INET, "127.0.0.1", &sin.sin_addr);
  if (connect(fd, (struct sockaddr *)&sin, sizeof(sin)) != 0) {
    close(fd);
    return -1;
  }
  return fd;
}

/* Blocking socket calls with SO_RCVTIMEO can fail with EINTR (seen under ASan on Linux without any
 * signal delivered); OpenSSL reports that as a retryable error. These wrappers repeat the call. */
static bool interrupted(SSL *ssl, int r) {
  int e = SSL_get_error(ssl, r);
  return (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE || e == SSL_ERROR_SYSCALL) &&
         errno == EINTR;
}
static int ssl_connect_r(SSL *ssl) {
  int r, n = 0;
  do {
    errno = 0;
    r = SSL_connect(ssl);
  } while (r != 1 && interrupted(ssl, r) && ++n < 100);
  return r;
}
static int ssl_read_r(SSL *ssl, void *buf, int len) {
  int r, n = 0;
  do {
    errno = 0;
    r = SSL_read(ssl, buf, len);
  } while (r <= 0 && interrupted(ssl, r) && ++n < 100);
  return r;
}
static int ssl_write_r(SSL *ssl, const void *buf, int len) {
  int r, n = 0;
  do {
    errno = 0;
    r = SSL_write(ssl, buf, len);
  } while (r <= 0 && interrupted(ssl, r) && ++n < 100);
  return r;
}
static int ssl_shutdown_r(SSL *ssl) {
  int r, n = 0;
  do {
    errno = 0;
    r = SSL_shutdown(ssl);
  } while (r < 0 && interrupted(ssl, r) && ++n < 100);
  return r;
}

/* Reads until the peer ends the connection or the receive timeout fires. Returns true if the
 * connection ended (close_notify, EOF, reset or a fatal alert) rather than timing out. */
static int last_read_error, last_read_errno;
static bool read_until_closed(SSL *ssl) {
  char buf[4096];
  for (int i = 0; i < 64; i++) {
    int n = ssl_read_r(ssl, buf, sizeof(buf));
    if (n > 0)
      continue;
    int e = SSL_get_error(ssl, n);
    last_read_error = e;
    last_read_errno = errno;
    if (e == SSL_ERROR_ZERO_RETURN || e == SSL_ERROR_SSL)
      return true;
    /* EOF without close_notify, or a reset; EAGAIN is the receive timeout */
    if (e == SSL_ERROR_SYSCALL)
      return errno != EAGAIN && errno != EWOULDBLOCK;
    return false;
  }
  return false;
}
#endif

void test_client_server_mtls_end_to_end(void) {
  grpc_server *server = fx.server = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .tls =
          {
              .cert_file = "test_cert.pem",
              .key_file = "test_key.pem",
              .ca_file = "test_cert.pem",
              .require_client_cert = true,
          },
      .log_level = GRPC_LOG_OFF,
  });
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/mtls.Svc/Echo", mtls_echo, nullptr));
  server_start();

  grpc_err err = GRPC_OK;
  grpc_client *client = fx.client = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .timeout_ms = 3000,
          .tls =
              {
                  .enable = true,
                  .ca_file = "test_cert.pem",
                  .cert_file = "test_cert.pem",
                  .key_file = "test_key.pem",
                  .server_name = "localhost",
              },
      },
      &err);
  TEST_ASSERT_NOT_NULL(client);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);

  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/mtls.Svc/Echo", "secret", 6, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_MEMORY("secret", resp.message->data, 6);
  char seen_subject[256];
  subject_read(seen_subject, sizeof(seen_subject));
  TEST_ASSERT_NOT_NULL(strstr(seen_subject, "localhost"));
  grpc_client_response_free(&resp);

  grpc_client_free(client);
  fx.client = nullptr;

  /* Also exercise client ca_pem (in-memory CA) and insecure_skip_verify */
  size_t ca_len = 0;
  char *ca_pem = fx.bufs[0] = slurp_file("test_cert.pem", &ca_len);
  TEST_ASSERT_NOT_NULL(ca_pem);
  grpc_client *c_pem = fx.client = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .timeout_ms = 3000,
          .tls =
              {
                  .enable = true,
                  .ca_pem = ca_pem,
                  .ca_pem_len = ca_len,
                  .cert_file = "test_cert.pem",
                  .key_file = "test_key.pem",
                  .server_name = "localhost",
              },
      },
      &err);
  TEST_ASSERT_NOT_NULL(c_pem);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(c_pem, "/mtls.Svc/Echo", "pem", 3, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  grpc_client_free(c_pem);
  fx.client = nullptr;

  grpc_client *c_insec = fx.client = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .timeout_ms = 3000,
          .tls =
              {
                  .enable = true,
                  .insecure_skip_verify = true,
                  .cert_file = "test_cert.pem",
                  .key_file = "test_key.pem",
              },
      },
      &err);
  TEST_ASSERT_NOT_NULL(c_insec);
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(c_insec, "/mtls.Svc/Echo", "ok", 2, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  grpc_client_free(c_insec);
  fx.client = nullptr;

  /* Invalid client TLS cert/key/ca_pem config */
  TEST_ASSERT_NULL(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .tls = {.enable = true, .cert_file = "nonexistent.pem", .key_file = "nonexistent.pem"},
      },
      &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  TEST_ASSERT_NULL(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .tls = {.enable = true, .cert_file = "test_cert.pem", .key_file = "nonexistent.pem"},
      },
      &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  err = GRPC_OK;
  TEST_ASSERT_NULL(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .tls = {.enable = true, .ca_pem = "not-a-pem", .ca_pem_len = 9},
      },
      &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);

  /* Client without client cert connecting to require_client_cert=true server -> handshake fail */
  grpc_client *c_nocert = fx.client = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .timeout_ms = 1000,
          .tls = {.enable = true, .insecure_skip_verify = true},
      },
      &err);
  if (c_nocert != nullptr) {
    /* TLS 1.3 delivers the client-cert alert after the client's handshake: on the first RPC */
    grpc_err rpc = grpc_client_unary(c_nocert, "/mtls.Svc/Echo", "x", 1, nullptr, 0, 500, &resp);
    TEST_ASSERT_TRUE(rpc != GRPC_OK || resp.status != GRPC_STATUS_OK);
    grpc_client_response_free(&resp);
    grpc_client_free(c_nocert);
    fx.client = nullptr;
  } else {
    TEST_ASSERT_NOT_EQUAL_INT(GRPC_OK, err);
  }

  /* Invalid in-memory PEMs + multi-cert chain PEM */
  TEST_ASSERT_NULL(grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .tls = {.cert_pem = "not-a-pem", .key_pem = "not-a-key"},
      .log_level = GRPC_LOG_OFF,
  }));
  TEST_ASSERT_NULL(grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .tls = {.cert_file = "test_cert.pem", .key_file = "test_key.pem", .ca_pem = "not-a-ca"},
      .log_level = GRPC_LOG_OFF,
  }));

  size_t clen = 0, klen = 0;
  char *c_buf = fx.bufs[1] = slurp_file("test_cert.pem", &clen);
  char *k_buf = fx.bufs[2] = slurp_file("test_key.pem", &klen);
  TEST_ASSERT_NOT_NULL(c_buf);
  TEST_ASSERT_NOT_NULL(k_buf);
  /* Valid cert_pem with invalid key_pem -> fails key loading */
  TEST_ASSERT_NULL(grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .tls = {.cert_pem = c_buf, .cert_pem_len = clen, .key_pem = "not-a-key", .key_pem_len = 9},
      .log_level = GRPC_LOG_OFF,
  }));
  char *chain = fx.bufs[3] = malloc(clen * 2 + 1);
  TEST_ASSERT_NOT_NULL(chain);
  memcpy(chain, c_buf, clen);
  memcpy(chain + clen, c_buf, clen);
  chain[clen * 2] = '\0';
  grpc_server *chain_srv = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .tls = {.cert_pem = chain, .cert_pem_len = clen * 2, .key_pem = k_buf, .key_pem_len = klen},
      .log_level = GRPC_LOG_OFF,
  });
  TEST_ASSERT_NOT_NULL(chain_srv);
  grpc_server_free(chain_srv);

#ifndef _WIN32
  /* Raw OpenSSL clients. The server's ALPN callback picks h2, and declines ALPN (NOACK) for a
   * client that does not offer it, so the handshake still completes; the connection then speaks
   * HTTP/2 only. An HTTP/1.1 request is rejected with the connection closed, while an h2 client
   * gets the server's SETTINGS and can end the session with a clean close_notify. */
  for (int k = 0; k < 2; k++) {
    const bool h2 = k == 1;
    int fd = connect_local(grpc_server_port(server));
    TEST_ASSERT_TRUE(fd >= 0);
    SSL_CTX *cctx = SSL_CTX_new(TLS_client_method());
    TEST_ASSERT_NOT_NULL(cctx);
    SSL_CTX_set_verify(cctx, SSL_VERIFY_NONE, nullptr);
    TEST_ASSERT_EQUAL_INT(1, SSL_CTX_use_certificate_chain_file(cctx, "test_cert.pem"));
    TEST_ASSERT_EQUAL_INT(1, SSL_CTX_use_PrivateKey_file(cctx, "test_key.pem", SSL_FILETYPE_PEM));
    static const unsigned char alpn_http11[] = {8, 'h', 't', 't', 'p', '/', '1', '.', '1'};
    static const unsigned char alpn_h2[] = {2, 'h', '2'};
    TEST_ASSERT_EQUAL_INT(0, h2 ? SSL_CTX_set_alpn_protos(cctx, alpn_h2, sizeof(alpn_h2))
                                : SSL_CTX_set_alpn_protos(cctx, alpn_http11, sizeof(alpn_http11)));
    SSL *ssl = SSL_new(cctx);
    TEST_ASSERT_NOT_NULL(ssl);
    SSL_set_fd(ssl, fd);
    int connected = ssl_connect_r(ssl);
    const unsigned char *proto = nullptr;
    unsigned int proto_len = 0;
    if (connected == 1)
      SSL_get0_alpn_selected(ssl, &proto, &proto_len);
    bool ok_alpn = h2 ? proto_len == 2 && memcmp(proto, "h2", 2) == 0 : proto_len == 0;
    bool ok_session = false;
    if (connected == 1 && ok_alpn) {
      if (!h2) {
        static const char req[] = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
        ok_session = ssl_write_r(ssl, req, (int)sizeof(req) - 1) > 0 && read_until_closed(ssl);
      } else {
        /* client preface + empty SETTINGS; the reply starts with the server's SETTINGS frame */
        static const char preface[] = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
                                      "\x00\x00\x00\x04\x00\x00\x00\x00\x00";
        unsigned char hdr[9];
        ok_session = ssl_write_r(ssl, preface, (int)sizeof(preface) - 1) > 0 &&
                     ssl_read_r(ssl, hdr, sizeof(hdr)) == (int)sizeof(hdr) && hdr[3] == 0x04 &&
                     (hdr[4] & 0x01) == 0;
        /* clean shutdown: our close_notify goes out, the server ends the connection */
        ok_session = ok_session && ssl_shutdown_r(ssl) >= 0 && read_until_closed(ssl);
      }
    }
    SSL_free(ssl);
    SSL_CTX_free(cctx);
    close(fd);
    TEST_ASSERT_EQUAL_INT_MESSAGE(1, connected, h2 ? "h2 handshake" : "http/1.1 handshake");
    TEST_ASSERT_TRUE_MESSAGE(ok_alpn, h2 ? "h2 selected" : "no ALPN for http/1.1");
    char msg[96];
    snprintf(msg, sizeof(msg), "%s (SSL error %d, errno %d)",
             h2 ? "h2 session + clean shutdown" : "http/1.1 closed", last_read_error,
             last_read_errno);
    TEST_ASSERT_TRUE_MESSAGE(ok_session, msg);
  }
#endif
}

void test_tls_error_isolation(void) {
  grpc_tls_config cfg = {.cert_file = "test_cert.pem", .key_file = "test_key.pem"};
  grpc_tls_ctx *ctx = tls_backend_ctx_new(&cfg);
  TEST_ASSERT_NOT_NULL(ctx);
  grpc_tls_session *session = tls_backend_session_new(ctx);
  TEST_ASSERT_NOT_NULL(session);
  /* A failed connection can leave errors on this same event-loop thread. */
  ERR_put_error(ERR_LIB_SSL, 0, ERR_R_INTERNAL_ERROR, __FILE__, __LINE__);
  TEST_ASSERT_EQUAL_INT(0, tls_backend_handshake(session));
  ERR_put_error(ERR_LIB_SSL, 0, ERR_R_INTERNAL_ERROR, __FILE__, __LINE__);
  char byte;
  TEST_ASSERT_EQUAL_INT(-2, tls_backend_read(session, &byte, 1));
  tls_backend_session_free(session);
  tls_backend_ctx_free(ctx);
}
#endif

#if defined(GRPC_TLS_OPENSSL) || defined(GRPC_TLS_LIBRESSL) || defined(GRPC_TLS_BORINGSSL) || \
    defined(GRPC_TLS_AWSLC)
static void plain_echo(grpc_call *call, const grpc_message *req, [[maybe_unused]] void *ud) {
  (void)grpc_call_reply_data(call, req->data, req->len);
}

/* One unary call through a new TLS client; returns whether it succeeded. */
static bool tls_call_ok(uint16_t port, const char *server_name) {
  grpc_err err = GRPC_OK;
  grpc_client *c = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = port,
          .timeout_ms = 2000,
          .tls = {.enable = true, .ca_file = "test_cert.pem", .server_name = server_name}},
      &err);
  if (c == nullptr)
    return false;
  grpc_client_response resp = {0};
  grpc_err rc = grpc_client_unary(c, "/v.Svc/Echo", "x", 1, nullptr, 0, 2000, &resp);
  bool ok = rc == GRPC_OK && resp.status == GRPC_STATUS_OK;
  grpc_client_response_free(&resp);
  grpc_client_free(c);
  return ok;
}

/* The HTTP/2 client checks that the certificate was issued for the server's name, not only that
 * it chains to a trusted CA. */
void test_client_verifies_server_name(void) {
  grpc_err err = GRPC_OK;
  grpc_server *server = fx.server = new_tls_server("test_cert.pem", "test_key.pem", &err);
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/v.Svc/Echo", plain_echo, nullptr));
  server_start();
  uint16_t port = grpc_server_port(server);
  TEST_ASSERT_TRUE_MESSAGE(tls_call_ok(port, "localhost"), "the right name was refused");
  TEST_ASSERT_FALSE_MESSAGE(tls_call_ok(port, "evil.example.com"),
                            "a certificate for another name was accepted");
  /* by IP address: the certificate (CN=localhost) names no IP address */
  TEST_ASSERT_FALSE_MESSAGE(tls_call_ok(port, nullptr),
                            "a certificate without the IP address was accepted");
}
#endif

#if defined(GRPC_TLS_OPENSSL) || defined(GRPC_TLS_LIBRESSL) || defined(GRPC_TLS_BORINGSSL) || \
    defined(GRPC_TLS_AWSLC)
static uint64_t mono_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

/* A reconnect made inside a call is bounded by that call's deadline, even when the client has no
 * connect timeout of its own: a peer that accepts TCP but never answers the TLS handshake does
 * not block the call forever. */
void test_client_reconnect_bounded_by_call_deadline(void) {
  grpc_err err = GRPC_OK;
  grpc_server *server = fx.server = new_tls_server("test_cert.pem", "test_key.pem", &err);
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/v.Svc/Echo", plain_echo, nullptr));
  server_start();
  uint16_t port = grpc_server_port(server);
  fx.client = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = port,
          .timeout_ms = 0, /* no connect bound of the client's own */
          .tls = {.enable = true, .ca_file = "test_cert.pem", .server_name = "localhost"}},
      &err);
  TEST_ASSERT_NOT_NULL(fx.client);
  /* the server goes away; a listener that never speaks TLS takes its port */
  server_stop_thread();
  grpc_server_free(server);
  fx.server = nullptr;
  int lfd = socket(AF_INET, SOCK_STREAM, 0);
  int one = 1;
  setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  struct sockaddr_in sin = {.sin_family = AF_INET, .sin_port = htons(port)};
  sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  TEST_ASSERT_EQUAL_INT(0, bind(lfd, (struct sockaddr *)&sin, sizeof(sin)));
  TEST_ASSERT_EQUAL_INT(0, listen(lfd, 4));
  grpc_client_response resp = {0};
  uint64_t start = mono_ms();
  grpc_err rc = grpc_client_unary(fx.client, "/v.Svc/Echo", "x", 1, nullptr, 0, 300, &resp);
  uint64_t took = mono_ms() - start;
  grpc_client_response_free(&resp);
  close(lfd);
  TEST_ASSERT_NOT_EQUAL_INT(GRPC_OK, rc);
  TEST_ASSERT_TRUE_MESSAGE(took < 2000, "the reconnect ignored the call's deadline");
}
#endif

#if defined(GRPC_TLS_GNUTLS)
/* A GnuTLS client whose output is held back (to be sent together with an extra record) and whose
 * input comes from a socket, non-blocking while output is pending so the handshake returns. */
typedef struct held_client {
  int fd;
  uint8_t out[16384];
  size_t out_len;
} held_client;

static ssize_t held_push(gnutls_transport_ptr_t ptr, const void *data, size_t n) {
  held_client *c = ptr;
  if (n > sizeof(c->out) - c->out_len) {
    errno = ENOBUFS;
    return -1;
  }
  memcpy(c->out + c->out_len, data, n);
  c->out_len += n;
  return (ssize_t)n;
}

static ssize_t held_pull(gnutls_transport_ptr_t ptr, void *data, size_t n) {
  held_client *c = ptr;
  if (c->out_len > 0) { /* output waiting to be sent first */
    errno = EAGAIN;
    return -1;
  }
  return recv(c->fd, data, n, 0);
}

/* Runs the client handshake until it is done or has output to send; returns the result. */
static int held_step(gnutls_session_t cs) {
  int rv;
  do
    rv = gnutls_handshake(cs);
  while (rv == GNUTLS_E_INTERRUPTED);
  return rv;
}

/* A non-fatal event during the server handshake (here a warning alert) with the client's next
 * flight already buffered behind it must not stall the GnuTLS server: the handshake is retried at
 * once instead of waiting for input that has already arrived. The client (TLS 1.2, where
 * user_canceled is a warning) sends that alert and its second flight in one write; the server
 * must finish the handshake. */
void test_gnutls_handshake_continues_after_warning_alert(void) {
  grpc_err err = GRPC_OK;
  fx.server = new_tls_server("test_cert.pem", "test_key.pem", &err);
  TEST_ASSERT_NOT_NULL(fx.server);
  server_start();

  static held_client hc;
  memset(&hc, 0, sizeof(hc));
  hc.fd = socket(AF_INET, SOCK_STREAM, 0);
  TEST_ASSERT_TRUE(hc.fd >= 0);
  struct sockaddr_in sin = {.sin_family = AF_INET, .sin_port = htons(grpc_server_port(fx.server))};
  sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  TEST_ASSERT_EQUAL_INT(0, connect(hc.fd, (struct sockaddr *)&sin, sizeof(sin)));
  struct timeval tv = {.tv_sec = 2, .tv_usec = 0};
  setsockopt(hc.fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  gnutls_certificate_credentials_t cred;
  TEST_ASSERT_EQUAL_INT(0, gnutls_certificate_allocate_credentials(&cred));
  gnutls_session_t cs;
  TEST_ASSERT_EQUAL_INT(0, gnutls_init(&cs, GNUTLS_CLIENT));
  TEST_ASSERT_EQUAL_INT(0,
                        gnutls_priority_set_direct(cs, "NORMAL:-VERS-ALL:+VERS-TLS1.2", nullptr));
  TEST_ASSERT_EQUAL_INT(0, gnutls_credentials_set(cs, GNUTLS_CRD_CERTIFICATE, cred));
  (void)gnutls_server_name_set(cs, GNUTLS_NAME_DNS, "localhost", 9);
  gnutls_datum_t alpn = {(unsigned char *)"h2", 2};
  (void)gnutls_alpn_set_protocols(cs, &alpn, 1, 0);
  gnutls_transport_set_ptr(cs, &hc);
  gnutls_transport_set_push_function(cs, held_push);
  gnutls_transport_set_pull_function(cs, held_pull);

  /* ClientHello */
  TEST_ASSERT_EQUAL_INT(GNUTLS_E_AGAIN, held_step(cs));
  TEST_ASSERT_TRUE(hc.out_len > 0);
  TEST_ASSERT_EQUAL_INT((int)hc.out_len, (int)send(hc.fd, hc.out, hc.out_len, MSG_NOSIGNAL));
  hc.out_len = 0;
  /* the server's flight in, the client's second flight out (held) */
  int rv = held_step(cs);
  TEST_ASSERT_EQUAL_INT_MESSAGE(GNUTLS_E_AGAIN, rv, gnutls_strerror(rv));
  TEST_ASSERT_TRUE(hc.out_len > 0);
  /* warning alert (user_canceled) and the second flight, in one write */
  static uint8_t wire[16384 + 7];
  const uint8_t alert[] = {0x15, 0x03, 0x03, 0x00, 0x02, 0x01, 90}; /* user_canceled */
  memcpy(wire, alert, sizeof(alert));
  memcpy(wire + sizeof(alert), hc.out, hc.out_len);
  TEST_ASSERT_EQUAL_INT((int)(sizeof(alert) + hc.out_len),
                        (int)send(hc.fd, wire, sizeof(alert) + hc.out_len, MSG_NOSIGNAL));
  hc.out_len = 0;
  /* the server's ChangeCipherSpec + Finished complete the handshake */
  rv = held_step(cs);
  gnutls_deinit(cs);
  gnutls_certificate_free_credentials(cred);
  close(hc.fd);
  TEST_ASSERT_EQUAL_INT_MESSAGE(GNUTLS_E_SUCCESS, rv, "the server handshake stalled");
}
#endif

#if defined(GRPC_TLS_RUSTLS)
static uint32_t
accept_any_server_cert([[maybe_unused]] void *userdata,
                       [[maybe_unused]] const struct rustls_verify_server_cert_params *params) {
  return RUSTLS_RESULT_OK; /* only the ClientHello is needed */
}

static rustls_io_result collect_tls(void *userdata, const uint8_t *buf, size_t n, size_t *out_n) {
  grpc_buf *b = userdata;
  if (buf_append(b, buf, n))
    return ENOMEM;
  *out_n = n;
  return 0;
}

static void *fail_alloc([[maybe_unused]] size_t n) {
  return nullptr;
}
static void *fail_calloc([[maybe_unused]] size_t a, [[maybe_unused]] size_t b) {
  return nullptr;
}

/* When the output buffer cannot grow while rustls hands over ciphertext, the drain reports
 * TLS_DRAIN_NOMEM (rustls keeps what it could not write, so a later drain continues) instead of
 * an error that closes the connection. */
void test_rustls_drain_out_of_memory_is_retryable(void) {
  /* a real ClientHello */
  struct rustls_client_config_builder *cb = rustls_client_config_builder_new();
  TEST_ASSERT_NOT_NULL(cb);
  TEST_ASSERT_EQUAL_INT(
      RUSTLS_RESULT_OK,
      rustls_client_config_builder_dangerous_set_certificate_verifier(cb, accept_any_server_cert));
  const struct rustls_client_config *ccfg = nullptr;
  TEST_ASSERT_EQUAL_INT(RUSTLS_RESULT_OK, rustls_client_config_builder_build(cb, &ccfg));
  struct rustls_connection *client = nullptr;
  TEST_ASSERT_EQUAL_INT(RUSTLS_RESULT_OK, rustls_client_connection_new(ccfg, "localhost", &client));
  grpc_buf hello = {0};
  size_t n = 0;
  TEST_ASSERT_EQUAL_INT(0, rustls_connection_write_tls(client, collect_tls, &hello, &n));
  TEST_ASSERT_TRUE(hello.len > 0);

  /* the server session reads it and has its first flight to send */
  grpc_tls_ctx *ctx = tls_backend_ctx_new(
      &(grpc_tls_config){.cert_file = "test_cert.pem", .key_file = "test_key.pem"});
  TEST_ASSERT_NOT_NULL(ctx);
  grpc_tls_session *server = tls_backend_session_new(ctx);
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(0, tls_backend_feed(server, hello.data, hello.len));
  TEST_ASSERT_TRUE(tls_backend_handshake(server) >= 0);

  /* nothing can be allocated: the flight stays with rustls */
  grpc_buf out = {0};
  grpc_set_allocator(&(grpc_allocator){.malloc = fail_alloc,
                                       .calloc = fail_calloc,
                                       .realloc = fail_realloc,
                                       .aligned_alloc = nullptr});
  int drained = tls_backend_drain(server, &out);
  grpc_set_allocator(nullptr);
  TEST_ASSERT_EQUAL_INT(TLS_DRAIN_NOMEM, drained);
  /* with memory again the same flight comes out */
  TEST_ASSERT_EQUAL_INT(TLS_DRAIN_OK, tls_backend_drain(server, &out));
  TEST_ASSERT_TRUE(out.len > 0);
  TEST_ASSERT_EQUAL_HEX8(0x16, out.data[0]); /* a handshake record: the ServerHello */

  buf_free(&out);
  buf_free(&hello);
  tls_backend_session_free(server);
  tls_backend_ctx_free(ctx);
  rustls_connection_free(client);
  rustls_client_config_free(ccfg);
}
#endif

int main(void) {
  /* unbuffered so a CTest timeout still shows which test was running */
  setvbuf(stdout, nullptr, _IONBF, 0);
#ifndef _WIN32
  signal(SIGPIPE, SIG_IGN);
#endif
  UNITY_BEGIN();
  RUN_TEST(test_tls_backend);
  RUN_TEST(test_tls_invalid_files);
  RUN_TEST(test_tls_server);
  RUN_TEST(test_tls_in_memory_pem_and_mtls);
#if defined(GRPC_TLS_GNUTLS)
  RUN_TEST(test_gnutls_handshake_continues_after_warning_alert);
#endif
#if defined(GRPC_TLS_RUSTLS)
  RUN_TEST(test_rustls_drain_out_of_memory_is_retryable);
#endif
#if defined(GRPC_TLS_OPENSSL) || defined(GRPC_TLS_LIBRESSL) || defined(GRPC_TLS_BORINGSSL) || \
    defined(GRPC_TLS_AWSLC)
  RUN_TEST(test_client_server_mtls_end_to_end);
  RUN_TEST(test_client_verifies_server_name);
  RUN_TEST(test_client_reconnect_bounded_by_call_deadline);
  RUN_TEST(test_tls_error_isolation);
#endif
  return UNITY_END();
}
