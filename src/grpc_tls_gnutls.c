/* grpc_tls_gnutls.c - TLS backend for GnuTLS.
 *
 * Implements the tls_backend_* contract of internal_tls.h. The server context holds the
 * certificate credentials, a priority cache limited to TLS 1.2 and 1.3, and the client
 * certificate policy, which is applied to each session.
 *
 * GnuTLS never touches the socket: custom transport push/pull functions move ciphertext through
 * the session's grpc_tls_io queues, which the libuv event loop fills (feed) and empties (drain).
 * An empty input queue is reported to GnuTLS as EAGAIN, so its calls return GNUTLS_E_AGAIN
 * instead of blocking.
 */
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <gnutls/gnutls.h>
#include <gnutls/x509.h>
#include "core.h"
#include "internal_tls.h"
#include "internal_tls_gnutls.h"

/* Server context. verify_client is set when a CA is configured. */
struct grpc_tls_ctx {
  gnutls_certificate_credentials_t cred;
  gnutls_priority_t priority;
  bool verify_client;
  bool require_client_cert;
};

/* Per-connection state: the GnuTLS session and its ciphertext queues. */
struct grpc_tls_session {
  gnutls_session_t session;
  grpc_tls_io io;
};

/* Returns "gnutls". */
const char *tls_backend_name(void) {
  return "gnutls";
}

/* Returns the run-time library version string. */
const char *tls_backend_version(void) {
  return gnutls_check_version(nullptr);
}

/* Transport pull function: copies up to `len` buffered ciphertext bytes into `buf`. With nothing
 * buffered it sets EAGAIN and returns -1, so GnuTLS returns GNUTLS_E_AGAIN rather than treating
 * it as EOF (a 0 return). */
static ssize_t io_pull(gnutls_transport_ptr_t ptr, void *buf, size_t len) {
  grpc_tls_session *tls = ptr;
  size_t n = tls_io_pull(&tls->io, buf, len);
  if (n == 0) {
    gnutls_transport_set_errno(tls->session, EAGAIN);
    return -1;
  }
  return (ssize_t)n;
}

/* Transport pull-timeout function: never waits; returns 1 if ciphertext is buffered, 0 if not. */
static int io_pull_timeout(gnutls_transport_ptr_t ptr, [[maybe_unused]] unsigned int ms) {
  grpc_tls_session *tls = ptr;
  return tls_io_pending(&tls->io) > 0 ? 1 : 0;
}

/* Transport push function: appends all of `buf` to the output queue. Returns `len`, or -1 with
 * ENOMEM if the queue cannot grow. */
static ssize_t io_push(gnutls_transport_ptr_t ptr, const void *buf, size_t len) {
  grpc_tls_session *tls = ptr;
  if (tls_io_push(&tls->io, buf, len)) {
    gnutls_transport_set_errno(tls->session, ENOMEM);
    return -1;
  }
  return (ssize_t)len;
}

/* Loads the server certificate chain and key from cfg (PEM text or files) into cred, and the CA
 * used to verify client certificates when one is configured. Shared with the QUIC context in
 * grpc_quic_tls_gnutls.c. Returns false if anything fails to load. */
bool tls_gnutls_load_server_credentials(gnutls_certificate_credentials_t cred,
                                        const grpc_tls_config *cfg) {
  int key_res = -1;
  if (cfg->cert_pem != nullptr && cfg->key_pem != nullptr) {
    gnutls_datum_t c = {.data = grpc_unconst(cfg->cert_pem),
                        .size = (unsigned int)cfg->cert_pem_len};
    gnutls_datum_t k = {.data = grpc_unconst(cfg->key_pem), .size = (unsigned int)cfg->key_pem_len};
    key_res = gnutls_certificate_set_x509_key_mem(cred, &c, &k, GNUTLS_X509_FMT_PEM);
  } else if (cfg->cert_file != nullptr && cfg->key_file != nullptr) {
    key_res = gnutls_certificate_set_x509_key_file(cred, cfg->cert_file, cfg->key_file,
                                                   GNUTLS_X509_FMT_PEM);
  }
  if (key_res < 0)
    return false;
  if (cfg->ca_file != nullptr || cfg->ca_pem != nullptr) {
    int ca_res;
    if (cfg->ca_file != nullptr) {
      ca_res = gnutls_certificate_set_x509_trust_file(cred, cfg->ca_file, GNUTLS_X509_FMT_PEM);
    } else {
      gnutls_datum_t ca = {.data = grpc_unconst(cfg->ca_pem),
                           .size = (unsigned int)cfg->ca_pem_len};
      ca_res = gnutls_certificate_set_x509_trust_mem(cred, &ca, GNUTLS_X509_FMT_PEM);
    }
    if (ca_res <= 0)
      return false;
  }
  return true;
}

/* Creates the server context from `cfg`: the certificate and key from PEM buffers (preferred when
 * both are set) or from files, and a priority cache that allows only TLS 1.2 and 1.3. With a CA
 * file or PEM, the trust list is loaded (at least one certificate is required) and client
 * certificates are requested, or required when cfg->require_client_cert is set. Returns nullptr
 * on any error, after freeing what was allocated. */
grpc_tls_ctx *tls_backend_ctx_new(const grpc_tls_config *cfg) {
  if (cfg == nullptr)
    return nullptr;
  grpc_tls_ctx *ctx = grpc_calloc(1, sizeof(*ctx));
  if (ctx == nullptr) {
    return nullptr;
  }
  if (gnutls_certificate_allocate_credentials(&ctx->cred) < 0) {
    tls_backend_ctx_free(ctx);
    return nullptr;
  }
  if (!tls_gnutls_load_server_credentials(ctx->cred, cfg) ||
      gnutls_priority_init(&ctx->priority, "NORMAL:-VERS-ALL:+VERS-TLS1.3:+VERS-TLS1.2", nullptr) <
          0) {
    tls_backend_ctx_free(ctx);
    return nullptr;
  }
  if (cfg->ca_file != nullptr || cfg->ca_pem != nullptr) {
    ctx->verify_client = true;
    ctx->require_client_cert = cfg->require_client_cert;
  }
  return ctx;
}

/* Frees the priority cache, the credentials and the context; also handles a partially built
 * context. The credentials are not reference counted: sessions created from this context must be
 * gone before it is freed. */
void tls_backend_ctx_free(grpc_tls_ctx *ctx) {
  if (ctx->priority != nullptr)
    gnutls_priority_deinit(ctx->priority);
  if (ctx->cred != nullptr)
    gnutls_certificate_free_credentials(ctx->cred);
  grpc_free(ctx);
}

/* Returns the gnutls_certificate_credentials_t of the context. */
void *tls_backend_ctx_native(grpc_tls_ctx *ctx) {
  return ctx->cred;
}

/* Creates a non-blocking server session that uses the context's priorities and credentials,
 * offers h2 through ALPN, applies the client certificate policy (verified during the handshake),
 * and routes its transport through the grpc_tls_io queues. Returns nullptr on failure. */
grpc_tls_session *tls_backend_session_new(grpc_tls_ctx *ctx) {
  grpc_tls_session *tls = grpc_calloc(1, sizeof(*tls));
  if (tls == nullptr) {
    return nullptr;
  }
  if (gnutls_init(&tls->session, GNUTLS_SERVER | GNUTLS_NONBLOCK) < 0) {
    grpc_free(tls);
    return nullptr;
  }

  /* clients that do not offer h2 still connect, as with the other backends */
  gnutls_datum_t alpn = {.data = grpc_unconst("h2"), .size = 2};
  if (gnutls_priority_set(tls->session, ctx->priority) < 0 ||
      gnutls_credentials_set(tls->session, GNUTLS_CRD_CERTIFICATE, ctx->cred) < 0 ||
      gnutls_alpn_set_protocols(tls->session, &alpn, 1, GNUTLS_ALPN_SERVER_PRECEDENCE) < 0) {
    tls_backend_session_free(tls);
    return nullptr;
  }
  if (ctx->verify_client) {
    gnutls_certificate_server_set_request(
        tls->session, ctx->require_client_cert ? GNUTLS_CERT_REQUIRE : GNUTLS_CERT_REQUEST);
    gnutls_session_set_verify_cert(tls->session, nullptr, 0);
  }
  gnutls_transport_set_ptr(tls->session, tls);
  gnutls_transport_set_pull_function(tls->session, io_pull);
  gnutls_transport_set_pull_timeout_function(tls->session, io_pull_timeout);
  gnutls_transport_set_push_function(tls->session, io_push);
  return tls;
}

/* Frees the GnuTLS session, its ciphertext queues and the wrapper. */
void tls_backend_session_free(grpc_tls_session *tls) {
  gnutls_deinit(tls->session);
  tls_io_free(&tls->io);
  grpc_free(tls);
}

/* Appends received ciphertext to the input queue. Returns 0 on success, 1 on allocation
 * failure. */
int tls_backend_feed(grpc_tls_session *tls, const void *buf, size_t len) {
  return tls_io_feed(&tls->io, buf, len);
}

/* Moves the queued output ciphertext to the end of `out`. Returns 0 on success (also when nothing
 * is queued), 1 on allocation failure. */
int tls_backend_drain(grpc_tls_session *tls, grpc_buf *out) {
  return tls_io_drain(&tls->io, out);
}

/* Runs gnutls_handshake() on the ciphertext fed so far. Returns 1 when complete, -1 on a fatal
 * error, and 0 on any non-fatal result (GNUTLS_E_AGAIN when more input is needed). */
int tls_backend_handshake(grpc_tls_session *tls) {
  /* GNUTLS_E_AGAIN means more ciphertext is needed; other non-fatal codes (an interrupted call,
   * a warning alert) mean "call again now": waiting for input instead could stall the handshake
   * with its input already buffered. Bounded, so a peer cannot keep it spinning. */
  for (int tries = 0; tries < 16; tries++) {
    int ret = gnutls_handshake(tls->session);
    if (ret == GNUTLS_E_SUCCESS)
      return 1;
    if (ret == GNUTLS_E_AGAIN)
      return 0;
    if (gnutls_error_is_fatal(ret))
      return -1;
  }
  return 0;
}

/* Decrypts up to `len` bytes into `buf`. Returns the byte count, -2 on GNUTLS_E_AGAIN (more
 * ciphertext is needed), 0 on the peer's close_notify, -1 on a fatal error. Retries on
 * GNUTLS_E_INTERRUPTED and other non-fatal codes. */
ssize_t tls_backend_read(grpc_tls_session *tls, void *buf, size_t len) {
  for (;;) {
    ssize_t ret = gnutls_record_recv(tls->session, buf, len);
    if (ret >= 0) {
      /* 0 is the peer's close_notify */
      return ret;
    }
    if (ret == GNUTLS_E_AGAIN) {
      return -2;
    }
    if (ret == GNUTLS_E_INTERRUPTED || !gnutls_error_is_fatal((int)ret)) {
      /* e.g. a TLS 1.2 renegotiation request, which is ignored */
      continue;
    }
    return -1;
  }
}

/* Encrypts all of `buf` into the output queue, retrying on GNUTLS_E_INTERRUPTED. Returns `len`,
 * or -1 on error. */
ssize_t tls_backend_write(grpc_tls_session *tls, const void *buf, size_t len) {
  /* io_push never blocks, so gnutls_record_send() only fails on errors */
  size_t written = 0;
  while (written < len) {
    ssize_t ret = gnutls_record_send(tls->session, (const u8 *)buf + written, len - written);
    if (ret < 0) {
      if (ret == GNUTLS_E_INTERRUPTED)
        continue;
      return -1;
    }
    written += (size_t)ret;
  }
  return (ssize_t)written;
}

/* Queues a close_notify alert with gnutls_bye(GNUTLS_SHUT_WR), which does not wait for the peer's
 * reply; the result is ignored (best effort). The caller drains the alert. */
void tls_backend_close_notify(grpc_tls_session *tls) {
  (void)gnutls_bye(tls->session, GNUTLS_SHUT_WR);
}

/* OpenSSL's short names for the usual subject attributes; others print as their OID. */
static const struct {
  const char *oid;
  const char *name;
} subject_attr_names[] = {
    {"2.5.4.3", "CN"},
    {"2.5.4.4", "SN"},
    {"2.5.4.5", "serialNumber"},
    {"2.5.4.6", "C"},
    {"2.5.4.7", "L"},
    {"2.5.4.8", "ST"},
    {"2.5.4.9", "street"},
    {"2.5.4.10", "O"},
    {"2.5.4.11", "OU"},
    {"2.5.4.12", "title"},
    {"2.5.4.17", "postalCode"},
    {"2.5.4.42", "GN"},
    {"1.2.840.113549.1.9.1", "emailAddress"},
    {"0.9.2342.19200300.100.1.1", "UID"},
    {"0.9.2342.19200300.100.1.25", "DC"},
};

/* Appends n bytes to buf[*off]; false when they do not fit (with room for the NUL). */
static bool subject_put(char *buf, size_t len, size_t *off, const char *s, size_t n) {
  if (n >= len - *off)
    return false;
  memcpy(buf + *off, s, n);
  *off += n;
  return true;
}

int tls_gnutls_peer_subject(gnutls_session_t session, char *buf, size_t len) {
  if (len == 0)
    return -1;
  unsigned int list_size = 0;
  const gnutls_datum_t *certs = gnutls_certificate_get_peers(session, &list_size);
  if (certs == nullptr || list_size == 0)
    return -1;
  gnutls_x509_crt_t crt;
  if (gnutls_x509_crt_init(&crt) < 0)
    return -1;
  int res = -1;
  gnutls_x509_dn_t dn;
  if (gnutls_x509_crt_import(crt, &certs[0], GNUTLS_X509_FMT_DER) < 0 ||
      gnutls_x509_crt_get_subject(crt, &dn) < 0)
    goto out;
  size_t off = 0;
  for (int irdn = 0;; irdn++) {
    gnutls_x509_ava_st ava;
    int rv = gnutls_x509_dn_get_rdn_ava(dn, irdn, 0, &ava);
    if (rv == GNUTLS_E_ASN1_ELEMENT_NOT_FOUND)
      break;
    if (rv < 0)
      goto out;
    for (int iava = 0; rv >= 0; rv = gnutls_x509_dn_get_rdn_ava(dn, irdn, ++iava, &ava)) {
      /* the OID is text; its datum may count a terminating NUL */
      size_t oid_len = strnlen((const char *)ava.oid.data, ava.oid.size);
      const char *name = nullptr;
      for (size_t k = 0; k < sizeof(subject_attr_names) / sizeof(subject_attr_names[0]); k++) {
        if (strlen(subject_attr_names[k].oid) == oid_len &&
            memcmp(subject_attr_names[k].oid, ava.oid.data, oid_len) == 0) {
          name = subject_attr_names[k].name;
          break;
        }
      }
      if (!subject_put(buf, len, &off, iava == 0 ? "/" : "+", 1) ||
          !subject_put(buf, len, &off, name != nullptr ? name : (const char *)ava.oid.data,
                       name != nullptr ? strlen(name) : oid_len) ||
          !subject_put(buf, len, &off, "=", 1))
        goto out;
      for (unsigned int b = 0; b < ava.value.size; b++) {
        unsigned char ch = ava.value.data[b];
        char esc[5];
        bool printable = ch >= 0x20 && ch <= 0x7E;
        if (!printable)
          snprintf(esc, sizeof(esc), "\\x%02X", ch);
        if (!subject_put(buf, len, &off, printable ? (const char *)&ava.value.data[b] : esc,
                         printable ? 1 : 4))
          goto out;
      }
    }
  }
  buf[off] = '\0';
  res = 0;
out:
  gnutls_x509_crt_deinit(crt);
  return res;
}

/* Writes the subject of the peer certificate (see tls_gnutls_peer_subject()). */
int tls_backend_peer_subject(grpc_tls_session *tls, char *buf, size_t len) {
  return tls_gnutls_peer_subject(tls->session, buf, len);
}
