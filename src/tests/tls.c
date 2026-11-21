#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "grpc.h"
#include "unity.h"
#if defined(GRPC_TLS_OPENSSL) || defined(GRPC_TLS_LIBRESSL) || defined(GRPC_TLS_BORINGSSL) || defined(GRPC_TLS_AWSLC)
#include <openssl/err.h>
#include <openssl/ssl.h>
#include "internal_tls.h"
#endif

void setUp(void) {}
void tearDown(void) {}

static grpc_server *new_tls_server(const char *cert, const char *key) {
  return grpc_server_new(&(grpc_config){
      .port = 19995,
      .ipv4_addr = "127.0.0.1",
      .tls = {.cert_file = cert, .key_file = key},
      .log_level = GRPC_LOG_OFF,
  });
}

void test_tls_backend(void) {
  /* the library must be built with the backend the build asked for */
  TEST_ASSERT_EQUAL_STRING(EXPECTED_TLS_BACKEND, grpc_tls_backend());
  TEST_ASSERT_TRUE(strlen(grpc_tls_backend_version()) > 0);
  printf("TLS backend: %s (%s)\n", grpc_tls_backend(), grpc_tls_backend_version());
}

void test_tls_invalid_files(void) {
  TEST_ASSERT_NULL(new_tls_server("nonexistent_cert.pem", "nonexistent_key.pem"));
  /* a key that does not belong to the certificate */
  TEST_ASSERT_NULL(new_tls_server("test_cert.pem", "test_cert.pem"));
}

void test_tls_server(void) {
  grpc_server *server = new_tls_server("test_cert.pem", "test_key.pem");
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_NOT_NULL(grpc_server_tls_native(server));

  /* Hot-reload with valid PEMs succeeds and swaps the native context; invalid fails safely. */
  void *old_ctx = grpc_server_tls_native(server);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID,
                        grpc_server_reload_tls(server, &(grpc_tls_config){
                                                           .cert_file = "nonexistent.pem",
                                                           .key_file = "nonexistent.pem"}));
  TEST_ASSERT_EQUAL_PTR(old_ctx, grpc_server_tls_native(server));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_reload_tls(server, &(grpc_tls_config){
                                                                    .cert_file = "test_cert.pem",
                                                                    .key_file = "test_key.pem"}));
  TEST_ASSERT_NOT_NULL(grpc_server_tls_native(server));
  TEST_ASSERT_TRUE(grpc_server_tls_native(server) != old_ctx);

  grpc_server_free(server);

  server = grpc_server_new(
      &(grpc_config){.port = 19995, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_NULL(grpc_server_tls_native(server));
  grpc_server_free(server);
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
  char *cert_pem = slurp_file("test_cert.pem", &cert_len);
  char *key_pem = slurp_file("test_key.pem", &key_len);
  TEST_ASSERT_NOT_NULL(cert_pem);
  TEST_ASSERT_NOT_NULL(key_pem);

  grpc_server *server = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .tls = {
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

  /* Hot-reload with valid PEMs succeeds and swaps the native context; invalid fails safely. */
  void *old_ctx = grpc_server_tls_native(server);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID,
                        grpc_server_reload_tls(server, &(grpc_tls_config){
                                                           .cert_file = "nonexistent.pem",
                                                           .key_file = "nonexistent.pem"}));
  TEST_ASSERT_EQUAL_PTR(old_ctx, grpc_server_tls_native(server));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_reload_tls(server, &(grpc_tls_config){
                                                                    .cert_file = "test_cert.pem",
                                                                    .key_file = "test_key.pem"}));
  TEST_ASSERT_NOT_NULL(grpc_server_tls_native(server));
  TEST_ASSERT_TRUE(grpc_server_tls_native(server) != old_ctx);

  grpc_server_free(server);

  free(cert_pem);
  free(key_pem);
}

#if defined(GRPC_TLS_OPENSSL) || defined(GRPC_TLS_LIBRESSL) || defined(GRPC_TLS_BORINGSSL) || defined(GRPC_TLS_AWSLC)
#include "internal/grpc_threads.h"

static void *run_mtls_server(void *arg) {
  (void)grpc_server_run((grpc_server *)arg);
  return nullptr;
}

static void mtls_echo(grpc_call *call, const grpc_message *req, void *userdata) {
  char *subject_out = userdata;
  const char *subj = grpc_call_peer_cert_subject(call);
  if (subj != nullptr)
    snprintf(subject_out, 128, "%s", subj);
  (void)grpc_call_reply_data(call, req->data, req->len);
}

void test_client_server_mtls_end_to_end(void) {
  char seen_subject[128] = {0};
  grpc_server *server = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .tls = {
          .cert_file = "test_cert.pem",
          .key_file = "test_key.pem",
          .ca_file = "test_cert.pem",
          .require_client_cert = true,
      },
      .log_level = GRPC_LOG_OFF,
  });
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/mtls.Svc/Echo", mtls_echo, seen_subject));

  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_mtls_server, server));

  grpc_err err = GRPC_OK;
  grpc_client *client = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .timeout_ms = 3000,
          .tls = {
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
  TEST_ASSERT_NOT_NULL(strstr(seen_subject, "localhost"));
  grpc_client_response_free(&resp);

  grpc_client_free(client);

  /* Also exercise client ca_pem (in-memory CA) and insecure_skip_verify */
  size_t ca_len = 0;
  char *ca_pem = slurp_file("test_cert.pem", &ca_len);
  TEST_ASSERT_NOT_NULL(ca_pem);
  grpc_client *c_pem = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .timeout_ms = 3000,
          .tls = {
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
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(c_pem, "/mtls.Svc/Echo", "pem", 3, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  grpc_client_free(c_pem);

  grpc_client *c_insec = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .timeout_ms = 3000,
          .tls = {
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
  free(ca_pem);

  /* Invalid client TLS cert/key config */
  TEST_ASSERT_NULL(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .tls = {.enable = true, .cert_file = "nonexistent.pem", .key_file = "nonexistent.pem"},
      },
      &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);

  grpc_server_stop(server);
  pthread_join(th, nullptr);
  grpc_server_free(server);
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

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_tls_backend);
  RUN_TEST(test_tls_invalid_files);
  RUN_TEST(test_tls_server);
  RUN_TEST(test_tls_in_memory_pem_and_mtls);
#if defined(GRPC_TLS_OPENSSL) || defined(GRPC_TLS_LIBRESSL) || defined(GRPC_TLS_BORINGSSL) || defined(GRPC_TLS_AWSLC)
  RUN_TEST(test_client_server_mtls_end_to_end);
  RUN_TEST(test_tls_error_isolation);
#endif
  return UNITY_END();
}
