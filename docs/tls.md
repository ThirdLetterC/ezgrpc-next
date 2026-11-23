# TLS & mTLS

## Supported Backends

Select one TLS library at configure time with `-DGRPC_TLS_BACKEND=<backend>`:

| `GRPC_TLS_BACKEND` | Library | How to Obtain | Notes |
|:---|:---|:---|:---|
| `openssl` | OpenSSL 3.0+ | Built by default (3.5); `libssl-dev` with `-DGRPC_SYSTEM_DEPENDENCIES=openssl` | Default |
| `libressl` | LibreSSL 4.3 | `scripts/build-tls-backend.sh libressl <dir>` | Use with `-DGRPC_TLS_PREFIX` |
| `boringssl` | BoringSSL | `scripts/build-tls-backend.sh boringssl <dir>` | Use with `-DGRPC_TLS_PREFIX` |
| `awslc` | AWS-LC 5.10 | `scripts/build-tls-backend.sh awslc <dir>` | Use with `-DGRPC_TLS_PREFIX` |
| `wolfssl` | wolfSSL 5.6 | `libwolfssl-dev` | Requires `--enable-alpn` |
| `gnutls` | GnuTLS 3.8 | `libgnutls28-dev` | |
| `rustls` | rustls-ffi 0.15 | `scripts/build-tls-backend.sh rustls <dir>` | Rejects ALPN without `h2` |
| `none` | None | — | TLS calls return `GRPC_ERR_UNSUPPORTED` |

```bash
scripts/build-tls-backend.sh awslc "$HOME/tls/awslc"
cmake -S . -B build-awslc -DGRPC_TLS_BACKEND=awslc -DGRPC_TLS_PREFIX="$HOME/tls/awslc"
cmake --build build-awslc --parallel
```

Query the active backend at runtime with `grpc_tls_backend()` and `grpc_tls_backend_version()`.

HTTP/3 (QUIC) uses the same backend through ngtcp2's crypto helper. It works with OpenSSL 3.5+, BoringSSL, AWS-LC, wolfSSL and GnuTLS, but not with LibreSSL or rustls. QUIC connections use TLS 1.3 with ALPN `h3` and load the same certificates; see [HTTP/3](http3.md).

---

## Configuring TLS (Files or In-Memory PEMs)

All backends enforce TLS 1.2+, validate that the private key matches the certificate, and negotiate `h2` via ALPN:

```c
/* From files */
grpc_server *server = grpc_server_new(&(grpc_config){
    .port = 50051,
    .tls = {
        .cert_file = "server.crt",
        .key_file = "server.key",
    },
});

/* Or directly from in-memory PEM buffers */
grpc_server *mem_server = grpc_server_new(&(grpc_config){
    .port = 50051,
    .tls = {
        .cert_pem = cert_pem_str,
        .cert_pem_len = cert_pem_len, /* 0 uses strlen() */
        .key_pem = key_pem_str,
        .key_pem_len = key_pem_len,
    },
});
```

---

## Mutual TLS (mTLS)

Supply a CA certificate (`ca_file` or `ca_pem`) and optionally set `require_client_cert = true`. Inspect the verified client certificate subject inside any handler with `grpc_call_peer_cert_subject()`:

```c
grpc_server *server = grpc_server_new(&(grpc_config){
    .port = 50051,
    .tls = {
        .cert_file = "server.crt",
        .key_file = "server.key",
        .ca_file = "ca.crt",
        .require_client_cert = true,
    },
});

static void handler(grpc_call *call, const grpc_message *req, void *userdata) {
  (void)req;
  (void)userdata;
  const char *subject = grpc_call_peer_cert_subject(call); /* e.g. "/CN=client-a" */
  printf("authenticated peer: %s\n", subject ? subject : "(none)");
  (void)grpc_call_reply_data(call, "ok", 2);
}
```

---

## Zero-Downtime Certificate Hot-Reload

Rotate certificates without restarting the server or dropping existing connections:

```c
grpc_err err = grpc_server_reload_tls(server, &(grpc_tls_config){
    .cert_file = "new_server.crt",
    .key_file = "new_server.key",
    .ca_file = "ca.crt",
    .require_client_cert = true,
});
```

If loading fails, the previous TLS configuration remains active and `grpc_server_reload_tls()` returns an error. Reloading only applies to a server started with TLS: on a plaintext server it returns `GRPC_ERR_STATE` rather than switching new connections to TLS. Retired contexts are retained safely for in-flight sessions and freed on `grpc_server_free()`.

---

## Native Backend Escape Hatch

For backend-specific tuning not exposed in `grpc_tls_config`, `grpc_server_tls_native(server)` returns:
- OpenSSL / LibreSSL / BoringSSL / AWS-LC: `SSL_CTX *`. The bundled OpenSSL is linked into `libgrpc.so` without exporting its symbols, so an application using this context through the shared library must link the same OpenSSL: the installed static `libssl.a`/`libcrypto.a` (`<libdir>/ezgrpc-next/`, which `grpc::static` brings), or build with `-DGRPC_SYSTEM_DEPENDENCIES=openssl`
- wolfSSL: `WOLFSSL_CTX *`
- GnuTLS: `gnutls_certificate_credentials_t`
- rustls: `const struct rustls_server_config *`
