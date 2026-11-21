/* TLS backend for the OpenSSL API: OpenSSL, LibreSSL, BoringSSL and AWS-LC.
 * The connection runs over memory BIOs; the event loop moves the ciphertext. */
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

static inline SSL *ssl(grpc_tls_session *tls) {
  return (SSL *)tls;
}

const char *tls_backend_name(void) {
  return BACKEND_NAME;
}

const char *tls_backend_version(void) {
  return OpenSSL_version(OPENSSL_VERSION);
}

/* picks "h2" from the client's list. done by hand because
 * SSL_select_next_proto() differs between the OpenSSL forks */
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

void tls_backend_ctx_free(grpc_tls_ctx *ctx) {
  SSL_CTX_free(ssl_ctx(ctx));
}

void *tls_backend_ctx_native(grpc_tls_ctx *ctx) {
  return ssl_ctx(ctx);
}

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

void tls_backend_session_free(grpc_tls_session *tls) {
  SSL_free(ssl(tls));
}

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

int tls_backend_handshake(grpc_tls_session *tls) {
  ERR_clear_error();
  int ret = SSL_do_handshake(ssl(tls));
  if (ret == 1) {
    return 1;
  }
  int err = SSL_get_error(ssl(tls), ret);
  return err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE ? 0 : -1;
}

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

void tls_backend_close_notify(grpc_tls_session *tls) {
  ERR_clear_error();
  (void)SSL_shutdown(ssl(tls));
}

int tls_backend_peer_subject(grpc_tls_session *tls, char *buf, size_t len) {
  if (len == 0)
    return -1;
  X509 *cert = SSL_get_peer_certificate(ssl(tls));
  if (cert == nullptr)
    return -1;
  X509_NAME *subject = X509_get_subject_name(cert);
  char *res = subject != nullptr ? X509_NAME_oneline(subject, buf, (int)(len > INT_MAX ? INT_MAX : len))
                                 : nullptr;
  X509_free(cert);
  return res != nullptr ? 0 : -1;
}
