/* grpc_tls_openssl.c - TLS backend for the OpenSSL API: OpenSSL, LibreSSL, BoringSSL and AWS-LC.
 *
 * Implements the tls_backend_* contract of internal_tls.h. The SSL_CTX and SSL objects are used
 * directly as the opaque grpc_tls_ctx and grpc_tls_session, so no wrapper is allocated.
 *
 * Each connection runs over a pair of memory BIOs instead of a socket BIO: feed writes received
 * ciphertext into the read BIO, drain empties the write BIO, and the libuv event loop moves the
 * bytes. A compile-time check makes sure the headers found belong to the fork that was requested,
 * since the forks share the <openssl/...> include names.
 */
#include <limits.h>
#include <string.h>
#include <openssl/ssl.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include "core.h"
#include "internal_tls.h"

/* make sure the headers in the include path belong to the requested library */
#if defined(GRPC_TLS_AWSLC)
#if !defined(OPENSSL_IS_AWSLC)
#error "TLS=awslc, but the <openssl/ssl.h> found is not AWS-LC. Set TLS_PREFIX."
#endif
static const char BACKEND_NAME[] = "awslc";
#elif defined(GRPC_TLS_BORINGSSL)
#if !defined(OPENSSL_IS_BORINGSSL) || defined(OPENSSL_IS_AWSLC)
#error "TLS=boringssl, but the <openssl/ssl.h> found is not BoringSSL. Set TLS_PREFIX."
#endif
static const char BACKEND_NAME[] = "boringssl";
#elif defined(GRPC_TLS_LIBRESSL)
#if !defined(LIBRESSL_VERSION_NUMBER)
#error "TLS=libressl, but the <openssl/ssl.h> found is not LibreSSL. Set TLS_PREFIX."
#endif
static const char BACKEND_NAME[] = "libressl";
#elif defined(GRPC_TLS_OPENSSL)
#if defined(LIBRESSL_VERSION_NUMBER) || defined(OPENSSL_IS_BORINGSSL)
#error "TLS=openssl, but the <openssl/ssl.h> found is not OpenSSL."
#endif
static const char BACKEND_NAME[] = "openssl";
#else
#error "grpc_tls_openssl.c needs one of GRPC_TLS_{OPENSSL,LIBRESSL,BORINGSSL,AWSLC}"
#endif

/* the library objects are used directly as the opaque grpc types */
static inline SSL_CTX *ssl_ctx(grpc_tls_ctx *ctx) {
  return (SSL_CTX *)ctx;
}

/* Converts the opaque session handle back to the SSL object it is. */
static inline SSL *ssl(grpc_tls_session *tls) {
  return (SSL *)tls;
}

/* Returns the name of the fork this file was built for (e.g. "openssl", "boringssl"). */
const char *tls_backend_name(void) {
  return BACKEND_NAME;
}

/* Returns the run-time library version string from OpenSSL_version(). */
const char *tls_backend_version(void) {
  return OpenSSL_version(OPENSSL_VERSION);
}

/* ALPN callback: picks "h2" from the client's length-prefixed protocol list, or declines ALPN
 * (SSL_TLSEXT_ERR_NOACK) so that clients that do not offer h2 still connect. Done by hand because
 * SSL_select_next_proto() differs between the OpenSSL forks. A malformed list stops the scan. */
static int alpn_select_cb([[maybe_unused]] SSL *s, const unsigned char **out, unsigned char *outlen,
                          const unsigned char *in, unsigned int inlen, [[maybe_unused]] void *arg) {
  for (unsigned int i = 0; i < inlen;) {
    unsigned int len = in[i];
    if (i + 1 + len > inlen)
      break;
    if (len == 2 && memcmp(in + i + 1, "h2", 2) == 0) {
      *out = in + i + 1;
      *outlen = 2;
      return SSL_TLSEXT_ERR_OK;
    }
    i += 1 + len;
  }
  return SSL_TLSEXT_ERR_NOACK;
}

/* Loads an in-memory PEM certificate chain: the first certificate becomes the server certificate,
 * the following ones are added as extra chain certificates (SSL_CTX_add_extra_chain_cert() takes
 * ownership of them). Returns false on error. The final PEM read fails at the end of the data,
 * which leaves an error on the queue, so the queue is cleared to not confuse later SSL calls. */
static bool load_pem_chain(SSL_CTX *ctx, const char *pem, size_t pem_len) {
  if (pem == nullptr || pem_len == 0 || pem_len > INT_MAX)
    return false;
  BIO *bio = BIO_new_mem_buf(pem, (int)pem_len);
  if (bio == nullptr)
    return false;
  X509 *leaf = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
  if (leaf == nullptr || SSL_CTX_use_certificate(ctx, leaf) <= 0) {
    X509_free(leaf);
    BIO_free(bio);
    return false;
  }
  X509_free(leaf);
  X509 *ca;
  while ((ca = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr)) != nullptr) {
    if (SSL_CTX_add_extra_chain_cert(ctx, ca) <= 0) {
      X509_free(ca);
      BIO_free(bio);
      return false;
    }
  }
  ERR_clear_error();
  BIO_free(bio);
  return true;
}

/* Loads the first private key of an in-memory PEM buffer (unencrypted; no passphrase callback is
 * given) as the server key. Returns false on error. */
static bool load_pem_key(SSL_CTX *ctx, const char *pem, size_t pem_len) {
  if (pem == nullptr || pem_len == 0 || pem_len > INT_MAX)
    return false;
  BIO *bio = BIO_new_mem_buf(pem, (int)pem_len);
  if (bio == nullptr)
    return false;
  EVP_PKEY *pkey = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
  BIO_free(bio);
  if (pkey == nullptr)
    return false;
  bool ok = SSL_CTX_use_PrivateKey(ctx, pkey) > 0;
  EVP_PKEY_free(pkey);
  return ok;
}

/* Adds every certificate of an in-memory PEM buffer to the context's trust store, used to verify
 * client certificates. Returns true if at least one certificate was added. Clears the error queue
 * left by the final, failing PEM read and by any rejected certificate. */
static bool load_ca_pem(SSL_CTX *ctx, const char *pem, size_t pem_len) {
  if (pem == nullptr || pem_len == 0 || pem_len > INT_MAX)
    return false;
  BIO *bio = BIO_new_mem_buf(pem, (int)pem_len);
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

/* Creates a server SSL_CTX from `cfg`: TLS 1.2 minimum, the certificate chain and key from PEM
 * buffers (preferred when both are set) or from files, checked to match each other. With a CA
 * file or PEM, client certificates are requested and verified, and required when
 * cfg->require_client_cert is set. Installs the h2 ALPN callback. Returns nullptr on any error. */
grpc_tls_ctx *tls_backend_ctx_new(const grpc_tls_config *cfg) {
  if (cfg == nullptr)
    return nullptr;
  SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
  if (ctx == nullptr)
    return nullptr;

  if (!SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION)) {
    SSL_CTX_free(ctx);
    return nullptr;
  }

  bool ok = false;
  if (cfg->cert_pem != nullptr && cfg->key_pem != nullptr) {
    ok = load_pem_chain(ctx, cfg->cert_pem, cfg->cert_pem_len) &&
         load_pem_key(ctx, cfg->key_pem, cfg->key_pem_len);
  } else if (cfg->cert_file != nullptr && cfg->key_file != nullptr) {
    ok = SSL_CTX_use_certificate_chain_file(ctx, cfg->cert_file) > 0 &&
         SSL_CTX_use_PrivateKey_file(ctx, cfg->key_file, SSL_FILETYPE_PEM) > 0;
  }
  if (!ok || !SSL_CTX_check_private_key(ctx)) {
    SSL_CTX_free(ctx);
    return nullptr;
  }

  if (cfg->ca_file != nullptr || cfg->ca_pem != nullptr) {
    bool ca_ok = cfg->ca_file != nullptr
                     ? SSL_CTX_load_verify_locations(ctx, cfg->ca_file, nullptr) > 0
                     : load_ca_pem(ctx, cfg->ca_pem, cfg->ca_pem_len);
    if (!ca_ok) {
      SSL_CTX_free(ctx);
      return nullptr;
    }
    int mode = SSL_VERIFY_PEER | (cfg->require_client_cert ? SSL_VERIFY_FAIL_IF_NO_PEER_CERT : 0);
    SSL_CTX_set_verify(ctx, mode, nullptr);
  }

  SSL_CTX_set_alpn_select_cb(ctx, alpn_select_cb, nullptr);
  return (grpc_tls_ctx *)ctx;
}

/* Releases the context's reference. Each SSL created from it holds its own reference, so live
 * sessions keep the SSL_CTX alive. */
void tls_backend_ctx_free(grpc_tls_ctx *ctx) {
  SSL_CTX_free(ssl_ctx(ctx));
}

/* Returns the SSL_CTX itself. */
void *tls_backend_ctx_native(grpc_tls_ctx *ctx) {
  return ssl_ctx(ctx);
}

/* Creates a server-side SSL connected to two memory BIOs: the read BIO receives the ciphertext
 * fed in, the write BIO collects the ciphertext to send. Memory BIOs keep socket I/O in the event
 * loop. The SSL owns both BIOs after SSL_set_bio(). Returns nullptr on allocation failure. */
grpc_tls_session *tls_backend_session_new(grpc_tls_ctx *ctx) {
  SSL *s = SSL_new(ssl_ctx(ctx));
  if (s == nullptr) {
    return nullptr;
  }

  BIO *rbio = BIO_new(BIO_s_mem());
  BIO *wbio = BIO_new(BIO_s_mem());
  if (rbio == nullptr || wbio == nullptr) {
    BIO_free(rbio);
    BIO_free(wbio);
    SSL_free(s);
    return nullptr;
  }
  /* an empty read BIO means "wait for more data", not EOF */
  BIO_set_mem_eof_return(rbio, -1);
  SSL_set_bio(s, rbio, wbio);
  SSL_set_accept_state(s);
  return (grpc_tls_session *)s;
}

/* Frees the SSL together with its BIOs. */
void tls_backend_session_free(grpc_tls_session *tls) {
  SSL_free(ssl(tls));
}

/* Appends received ciphertext to the read BIO, in chunks of at most INT_MAX bytes. Returns 0 on
 * success, 1 if the BIO rejects the data (allocation failure). */
int tls_backend_feed(grpc_tls_session *tls, const void *buf, size_t len) {
  BIO *rbio = SSL_get_rbio(ssl(tls));
  while (len > 0) {
    int n = BIO_write(rbio, buf, len > INT_MAX ? INT_MAX : (int)len);
    if (n <= 0) {
      return 1;
    }
    buf = (const u8 *)buf + n;
    len -= (size_t)n;
  }
  return 0;
}

/* Moves all ciphertext pending in the write BIO to the end of `out`. Returns 0 on success (also
 * when nothing is pending), 1 on allocation or BIO failure. */
int tls_backend_drain(grpc_tls_session *tls, grpc_buf *out) {
  BIO *wbio = SSL_get_wbio(ssl(tls));
  size_t pending;
  while ((pending = BIO_ctrl_pending(wbio)) > 0) {
    if (buf_reserve(out, pending)) {
      return 1;
    }
    int n = BIO_read(wbio, out->data + out->len, pending > INT_MAX ? INT_MAX : (int)pending);
    if (n <= 0) {
      return 1;
    }
    out->len += (size_t)n;
  }
  return 0;
}

/* Runs SSL_do_handshake() on the ciphertext fed so far. Returns 1 when complete, 0 when it needs
 * more input (WANT_READ/WANT_WRITE), -1 on error. The thread's error queue is cleared first:
 * SSL_get_error() also looks at it, so a stale entry left by unrelated OpenSSL calls could turn a
 * want-read into an error. */
int tls_backend_handshake(grpc_tls_session *tls) {
  ERR_clear_error();
  int ret = SSL_do_handshake(ssl(tls));
  if (ret == 1) {
    return 1;
  }
  int err = SSL_get_error(ssl(tls), ret);
  return err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE ? 0 : -1;
}

/* Decrypts up to `len` bytes (capped at INT_MAX) into `buf`. Returns the byte count, -2 on
 * WANT_READ/WANT_WRITE (more ciphertext is needed), 0 on the peer's close_notify, -1 on error.
 * Clears the error queue first, for the same reason as tls_backend_handshake(). */
ssize_t tls_backend_read(grpc_tls_session *tls, void *buf, size_t len) {
  ERR_clear_error();
  int ret = SSL_read(ssl(tls), buf, len > INT_MAX ? INT_MAX : (int)len);
  if (ret > 0) {
    return ret;
  }
  switch (SSL_get_error(ssl(tls), ret)) {
  case SSL_ERROR_WANT_READ:
  case SSL_ERROR_WANT_WRITE:
    return -2;
  case SSL_ERROR_ZERO_RETURN:
    return 0;
  default:
    return -1;
  }
}

/* Encrypts all of `buf` into the write BIO, looping in chunks of at most INT_MAX bytes. Returns
 * `len`, or -1 on error. */
ssize_t tls_backend_write(grpc_tls_session *tls, const void *buf, size_t len) {
  /* a memory BIO accepts everything, so SSL_write() only fails on errors */
  size_t written = 0;
  while (written < len) {
    size_t n = len - written;
    ERR_clear_error();
    int ret = SSL_write(ssl(tls), (const u8 *)buf + written, n > INT_MAX ? INT_MAX : (int)n);
    if (ret <= 0) {
      return -1;
    }
    written += (size_t)ret;
  }
  return (ssize_t)written;
}

/* Writes a close_notify alert into the write BIO with SSL_shutdown(), without waiting for the
 * peer's reply; the result is ignored (best effort). The caller drains the alert. */
void tls_backend_close_notify(grpc_tls_session *tls) {
  ERR_clear_error();
  (void)SSL_shutdown(ssl(tls));
}

/* Writes the peer certificate's subject in X509_NAME_oneline() form (e.g. "/CN=client") into
 * `buf`, truncated to fit. Returns 0 on success, -1 if there is no peer certificate or on error.
 * SSL_get_peer_certificate() returns a new reference, released here. */
int tls_backend_peer_subject(grpc_tls_session *tls, char *buf, size_t len) {
  if (len == 0)
    return -1;
  X509 *cert = SSL_get_peer_certificate(ssl(tls));
  if (cert == nullptr)
    return -1;
  X509_NAME *subject = X509_get_subject_name(cert);
  char *res = subject != nullptr
                  ? X509_NAME_oneline(subject, buf, (int)(len > INT_MAX ? INT_MAX : len))
                  : nullptr;
  X509_free(cert);
  return res != nullptr ? 0 : -1;
}
