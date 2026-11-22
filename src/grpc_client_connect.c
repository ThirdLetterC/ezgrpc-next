/* grpc_client_connect.c - client subchannel lifecycle: TLS context, connect, disconnect
 *
 * Resolving and connecting a subchannel (TCP, Unix socket or HTTP/3), its TLS handshake and HTTP/2
 * session setup, tearing it down, and parsing configured targets. Part of the client described in
 * grpc_client.c.
 */

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core.h"
#include "grpc_client.h"
#include "grpc_compression.h"
#include "internal_helpers.h"
#include "internal_client.h"
#include "internal_http3.h"

/* ---- TLS helpers (OpenSSL-compatible backends) ------------------------------ */

#if defined(GRPC_CLIENT_HAS_OPENSSL)
/* Adds every certificate in a PEM buffer to the context's trust store. Returns true if at least
 * one certificate was added. */
static bool client_load_ca_pem(SSL_CTX *ctx, const char *pem, size_t len) {
  if (pem == nullptr || len == 0 || len > INT_MAX)
    return false;
  BIO *bio = BIO_new_mem_buf(pem, (int)len);
  if (bio == nullptr)
    return false;
  X509_STORE *store = SSL_CTX_get_cert_store(ctx);
  int loaded = 0;
  X509 *cert;
  while ((cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr)) != nullptr) {
    if (X509_STORE_add_cert(store, cert) > 0)
      loaded++;
    X509_free(cert);
  }
  ERR_clear_error();
  BIO_free(bio);
  return loaded > 0;
}

SSL_CTX *client_ssl_ctx_new(const grpc_client_config *cfg) {
  SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
  if (ctx == nullptr)
    return nullptr;
  if (!SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION)) {
    SSL_CTX_free(ctx);
    return nullptr;
  }
  static const unsigned char alpn[] = {2, 'h', '2'};
  if (SSL_CTX_set_alpn_protos(ctx, alpn, sizeof(alpn)) != 0) {
    SSL_CTX_free(ctx);
    return nullptr;
  }
  if (cfg->tls.insecure_skip_verify) {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
  } else {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
    if (cfg->tls.ca_file != nullptr) {
      if (SSL_CTX_load_verify_locations(ctx, cfg->tls.ca_file, nullptr) <= 0) {
        SSL_CTX_free(ctx);
        return nullptr;
      }
    } else if (cfg->tls.ca_pem != nullptr) {
      size_t len = cfg->tls.ca_pem_len ? cfg->tls.ca_pem_len : strlen(cfg->tls.ca_pem);
      if (!client_load_ca_pem(ctx, cfg->tls.ca_pem, len)) {
        SSL_CTX_free(ctx);
        return nullptr;
      }
    } else {
      (void)SSL_CTX_set_default_verify_paths(ctx);
    }
  }
  if (cfg->tls.cert_file != nullptr || cfg->tls.key_file != nullptr) {
    if (cfg->tls.cert_file == nullptr || cfg->tls.key_file == nullptr ||
        SSL_CTX_use_certificate_chain_file(ctx, cfg->tls.cert_file) <= 0 ||
        SSL_CTX_use_PrivateKey_file(ctx, cfg->tls.key_file, SSL_FILETYPE_PEM) <= 0 ||
        !SSL_CTX_check_private_key(ctx)) {
      SSL_CTX_free(ctx);
      return nullptr;
    }
  }
  return ctx;
}
#endif

/* Connect completion: records the status that sub_connect's wait loop polls for. */
static void on_sub_connect(uv_connect_t *req, int status) {
  client_subchannel *s = req->data;
  s->connect_status = status;
  s->connected = (status == 0);
}

/* Close callback used while abandoning a failed connect attempt in sub_connect. */
static void on_sub_handle_closed(uv_handle_t *handle) {
  client_subchannel *s = handle->data;
  s->handle_close_done = true;
}

/* Timer callback: the connect + TLS handshake deadline (config.timeout_ms) elapsed. */
static void on_sub_connect_deadline(uv_timer_t *timer) {
  client_subchannel *s = timer->data;
  s->connect_deadline_hit = true;
}

/* ---- Subchannel lifecycle --------------------------------------------------- */

void sub_disconnect(client_subchannel *s) {
  s->closed = true;
  if (s->h3c != nullptr)
    h3c_disconnect(s);
  if (s->ngsession != nullptr) {
    nghttp2_session_del(s->ngsession);
    s->ngsession = nullptr;
  }
#if defined(GRPC_CLIENT_HAS_OPENSSL)
  if (s->ssl != nullptr) {
    SSL_free(s->ssl);
    s->ssl = nullptr;
  }
  s->tls_handshake_done = false;
#endif
  if (s->loop_initialized) {
    if (s->handle_initialized) {
      uv_close(sub_handle(s), nullptr);
      s->handle_initialized = false;
    }
    if (s->timer_initialized) {
      uv_close((uv_handle_t *)&s->timer, nullptr);
      s->timer_initialized = false;
    }
    uv_run(&s->loop, UV_RUN_DEFAULT);
    uv_loop_close(&s->loop);
    s->loop_initialized = false;
  }
  s->connected = false;
  s->is_writing = false;
  s->obuf.len = 0;
  s->wbuf.len = 0;
  s->tls_obuf.len = 0;
}

bool sub_usable(client_subchannel *s) {
  if (!s->connected || s->closed || (s->ngsession == nullptr && s->h3c == nullptr))
    return false;
  /* QUIC first: whether the connection went silent for too long must be judged before reading
   * the packets that queued up since the last call, which would look like fresh activity */
  if (s->h3c != nullptr && !h3c_usable(s))
    return false;
  uv_update_time(&s->loop);
  (void)uv_run(&s->loop, UV_RUN_NOWAIT);
  if (s->closed)
    return false;
  if (s->h3c != nullptr) /* again: a GOAWAY may have arrived just now */
    return h3c_usable(s);
  return s->ngsession != nullptr && nghttp2_session_check_request_allowed(s->ngsession) != 0;
}

grpc_err sub_connect(client_subchannel *s, uint32_t bound_ms) {
  if (sub_usable(s))
    return GRPC_OK;
  sub_disconnect(s);
  s->closed = false;
  s->fail_err = GRPC_OK;

  if (uv_loop_init(&s->loop) != 0)
    return GRPC_ERR_IO;
  s->loop_initialized = true;
  if (uv_timer_init(&s->loop, &s->timer) != 0)
    goto fail_io;
  s->timer.data = s;
  s->timer_initialized = true;

  /* Bound connect + TLS handshake by the default deadline so an unresponsive
   * peer cannot block the caller forever. */
  s->connect_deadline_hit = false;
  if (bound_ms > 0)
    uv_timer_start(&s->timer, on_sub_connect_deadline, bound_ms, 0);

  if (s->parent->config.transport == GRPC_TRANSPORT_HTTP3) {
    if (h3c_connect(s) != GRPC_OK)
      goto fail_io;
    uv_timer_stop(&s->timer);
    h3c_flush(s);
    return GRPC_OK;
  }

  uv_connect_t conn_req = {.data = s};
  s->connect_status = -1;
  if (s->is_unix) {
    if (uv_pipe_init(&s->loop, &s->handle.pipe, 0) != 0)
      goto fail_io;
    s->handle.pipe.data = s;
    s->handle_initialized = true;
    uv_pipe_connect(&conn_req, &s->handle.pipe, s->unix_path, on_sub_connect);
    while (!s->connected && s->connect_status == -1 && !s->connect_deadline_hit)
      uv_run(&s->loop, UV_RUN_ONCE);
    if (!s->connected)
      goto fail_io;
  } else {
    struct sockaddr_storage ss = {0};
    struct addrinfo *res = nullptr;
    struct addrinfo single_ai = {0};
    if (uv_ip4_addr(s->host, s->port, (struct sockaddr_in *)&ss) == 0) {
      single_ai.ai_addr = (struct sockaddr *)&ss;
      single_ai.ai_addrlen = sizeof(struct sockaddr_in);
    } else if (uv_ip6_addr(s->host, s->port, (struct sockaddr_in6 *)&ss) == 0) {
      single_ai.ai_addr = (struct sockaddr *)&ss;
      single_ai.ai_addrlen = sizeof(struct sockaddr_in6);
    } else {
      struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM};
      char port_str[16];
      snprintf(port_str, sizeof(port_str), "%u", s->port);
      if (getaddrinfo(s->host, port_str, &hints, &res) != 0 || res == nullptr) {
        if (res != nullptr)
          freeaddrinfo(res);
        goto fail_io;
      }
    }
    struct addrinfo *ai = res != nullptr ? res : &single_ai;
    for (; ai != nullptr; ai = ai->ai_next) {
      s->connected = false;
      s->connect_status = -1;
      if (uv_tcp_init(&s->loop, &s->handle.tcp) != 0)
        continue;
      s->handle.tcp.data = s;
      s->handle_initialized = true;
      if (uv_tcp_connect(&conn_req, &s->handle.tcp, ai->ai_addr, on_sub_connect) == 0) {
        while (!s->connected && s->connect_status == -1 && !s->connect_deadline_hit)
          uv_run(&s->loop, UV_RUN_ONCE);
        if (s->connected)
          break;
      }
      /* Closing cancels a pending connect; libuv completes conn_req (on our stack)
       * before the close callback, so wait for that rather than for the loop to go
       * idle (the deadline timer keeps it alive). */
      s->handle_close_done = false;
      uv_close((uv_handle_t *)&s->handle.tcp, on_sub_handle_closed);
      while (!s->handle_close_done)
        uv_run(&s->loop, UV_RUN_ONCE);
      s->handle_initialized = false;
      if (s->connect_deadline_hit)
        break;
    }
    if (res != nullptr)
      freeaddrinfo(res);
    if (!s->connected)
      goto fail_io;
  }

  if (!s->is_unix)
    (void)uv_tcp_nodelay(&s->handle.tcp, 1);

#if defined(GRPC_CLIENT_HAS_OPENSSL)
  if (s->parent->ssl_ctx != nullptr) {
    s->ssl = SSL_new(s->parent->ssl_ctx);
    if (s->ssl == nullptr)
      goto fail_io;
    BIO *rbio = BIO_new(BIO_s_mem());
    BIO *wbio = BIO_new(BIO_s_mem());
    if (rbio == nullptr || wbio == nullptr) {
      BIO_free(rbio);
      BIO_free(wbio);
      goto fail_io;
    }
    BIO_set_mem_eof_return(rbio, -1);
    SSL_set_bio(s->ssl, rbio, wbio);
    const char *sni =
        s->parent->config.tls.server_name != nullptr ? s->parent->config.tls.server_name : s->host;
    if (sni != nullptr && sni[0] != '\0') {
      /* the certificate must also be issued for this name (or IP address), not just chain to a
       * trusted CA; SNI carries DNS names only */
      unsigned char addr[16];
      bool is_ip = uv_inet_pton(AF_INET, sni, addr) == 0 || uv_inet_pton(AF_INET6, sni, addr) == 0;
      bool verify = !s->parent->config.tls.insecure_skip_verify;
      if (!is_ip)
        (void)SSL_set_tlsext_host_name(s->ssl, grpc_unconst(sni)); /* a macro casting to void * */
      if (verify && (is_ip ? X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(s->ssl), sni) != 1
                           : SSL_set1_host(s->ssl, sni) != 1))
        goto fail_io;
    }
    SSL_set_connect_state(s->ssl);
  }
#endif

  nghttp2_session_callbacks *cbs = nullptr;
  if (nghttp2_session_callbacks_new(&cbs) != 0) {
    sub_disconnect(s);
    return GRPC_ERR_NOMEM;
  }
  client_h2_set_callbacks(cbs);
  nghttp2_mem mem = grpc_nghttp2_mem; /* copied by nghttp2 */
  int rv = nghttp2_session_client_new3(&s->ngsession, cbs, s, nullptr, &mem);
  nghttp2_session_callbacks_del(cbs);
  if (rv != 0) {
    sub_disconnect(s);
    return rv == NGHTTP2_ERR_NOMEM ? GRPC_ERR_NOMEM : GRPC_ERR_PROTOCOL;
  }

  nghttp2_settings_entry iv[] = {
      {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 100},
      /* a whole message fits the stream window, within HTTP/2's 2^31 - 1 limit */
      {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE,
       s->parent->config.max_message_size > NGHTTP2_MAX_WINDOW_SIZE
           ? (uint32_t)NGHTTP2_MAX_WINDOW_SIZE
           : s->parent->config.max_message_size},
  };
  rv = nghttp2_submit_settings(s->ngsession, NGHTTP2_FLAG_NONE, iv, 2);
  if (rv != 0) {
    sub_disconnect(s);
    return rv == NGHTTP2_ERR_NOMEM ? GRPC_ERR_NOMEM : GRPC_ERR_PROTOCOL;
  }
  if (sub_read_start(s) != 0)
    goto fail_io;

#if defined(GRPC_CLIENT_HAS_OPENSSL)
  if (s->ssl != nullptr) {
    ERR_clear_error();
    (void)SSL_do_handshake(s->ssl);
    sub_flush(s);
    while (!s->tls_handshake_done && !s->closed && !s->connect_deadline_hit)
      uv_run(&s->loop, UV_RUN_ONCE);
    if (!s->tls_handshake_done || s->closed)
      goto fail_io;
  }
#endif
  uv_timer_stop(&s->timer);
  sub_flush(s);
  if (s->closed) {
    /* the connection preface or SETTINGS could not be queued */
    grpc_err err = s->fail_err != GRPC_OK ? s->fail_err : GRPC_ERR_IO;
    sub_disconnect(s);
    return err;
  }
  return GRPC_OK;

fail_io:
  sub_disconnect(s);
  return GRPC_ERR_IO;
}

grpc_err client_parse_target(const char *target, char **out_host, uint16_t *out_port,
                             char **out_unix) {
  *out_host = nullptr;
  *out_port = 0;
  *out_unix = nullptr;
  if (target == nullptr || target[0] == '\0')
    return GRPC_ERR_INVALID;
  if (strncmp(target, "unix:", 5) == 0) {
    const char *p = target + 5;
    if (strncmp(p, "//", 2) == 0)
      p += 2;
    if (p[0] == '\0')
      return GRPC_ERR_INVALID;
    *out_unix = grpc_strdup(p);
    return *out_unix != nullptr ? GRPC_OK : GRPC_ERR_NOMEM;
  }
  const char *colon = nullptr;
  const char *host_start = target;
  size_t host_len = 0;
  if (target[0] == '[') {
    const char *rb = strchr(target, ']');
    if (rb == nullptr || rb[1] != ':')
      return GRPC_ERR_INVALID;
    host_start = target + 1;
    host_len = (size_t)(rb - target - 1);
    colon = rb + 1;
  } else {
    colon = strrchr(target, ':');
    if (colon == nullptr || colon == target)
      return GRPC_ERR_INVALID;
    host_len = (size_t)(colon - target);
  }
  long p = strtol(colon + 1, nullptr, 10);
  if (p <= 0 || p > 65535)
    return GRPC_ERR_INVALID;
  *out_host = grpc_strndup(host_start, host_len);
  if (*out_host == nullptr)
    return GRPC_ERR_NOMEM;
  *out_port = (uint16_t)p;
  return GRPC_OK;
}

grpc_err client_init_subchannel(client_subchannel *s, grpc_client *parent, const char *host,
                                uint16_t port, const char *unix_path) {
  s->parent = parent;
  if (unix_path != nullptr) {
    s->unix_path = grpc_strdup(unix_path);
    if (s->unix_path == nullptr)
      return GRPC_ERR_NOMEM;
    s->is_unix = true;
    snprintf(s->authority, sizeof(s->authority), "localhost");
  } else {
    s->host = grpc_strdup(host != nullptr ? host : "127.0.0.1");
    if (s->host == nullptr)
      return GRPC_ERR_NOMEM;
    s->port = port;
    /* an IPv6 address is bracketed in the authority, as in a URL */
    snprintf(s->authority, sizeof(s->authority),
             strchr(s->host, ':') != nullptr ? "[%s]:%u" : "%s:%u", s->host, s->port);
  }
  return GRPC_OK;
}
