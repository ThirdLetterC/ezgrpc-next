# HTTP/3 (QUIC)

ezGRPC-next can serve and call gRPC over HTTP/3 on QUIC, alongside HTTP/2. The server listens on UDP next to its TCP listeners, and the client can use QUIC instead of TCP. Both use the same handlers, interceptors, deadlines, compression, health checks, reflection, statistics and connection limits as HTTP/2.

HTTP/3 is built on [ngtcp2](https://github.com/ngtcp2/ngtcp2) (QUIC) and [nghttp3](https://github.com/ngtcp2/nghttp3) (HTTP/3 and QPACK). Its handshake is always TLS 1.3 with ALPN `h3`.

---

## Building

HTTP/3 is an optional feature: `-DGRPC_HTTP3=AUTO|ON|OFF`. `AUTO`, the default, enables it when ngtcp2 ≥ 1.0, nghttp3 ≥ 1.0 and the ngtcp2 crypto helper for the selected TLS backend are found.

| `GRPC_TLS_BACKEND` | ngtcp2 crypto helper | Notes |
|:---|:---|:---|
| `openssl` | `libngtcp2_crypto_ossl` | Needs OpenSSL **3.5 or newer** (its QUIC TLS API). Most distributions ship 3.0, so `scripts/build-tls-backend.sh openssl <dir>` builds 3.5 |
| `boringssl`, `awslc` | `libngtcp2_crypto_boringssl` | |
| `wolfssl` | `libngtcp2_crypto_wolfssl` | wolfSSL built with `--enable-quic`; distribution packages usually are |
| `gnutls` | `libngtcp2_crypto_gnutls` | GnuTLS 3.7.5 or newer |
| `libressl`, `rustls`, `none` | — | No QUIC support: HTTP/3 is unavailable |

Distribution packages of ngtcp2 and nghttp3 are often too old; Ubuntu 24.04 ships pre-1.0 releases. `scripts/build-quic-deps.sh` builds pinned, checksum-verified releases into the same prefix as the TLS library:

```bash
scripts/build-tls-backend.sh openssl "$HOME/quic"     # OpenSSL 3.5 (skip for wolfssl / gnutls)
scripts/build-quic-deps.sh  openssl "$HOME/quic"      # ngtcp2 + nghttp3 for that backend
cmake -S . -B build-h3 -DGRPC_HTTP3=ON \
  -DGRPC_TLS_BACKEND=openssl -DGRPC_TLS_PREFIX="$HOME/quic" -DOPENSSL_ROOT_DIR="$HOME/quic"
cmake --build build-h3 --parallel
```

`just http3 <backend>` performs all of the above into `.tls/<backend>` and runs the tests.

At run time, `grpc_http3_available()` reports whether HTTP/3 is built in, and `grpc_http3_versions()` returns the library versions, e.g. `"ngtcp2/1.26.0 nghttp3/1.19.0 crypto=ossl"`.

---

## Server

Enable `config.http3` on a TLS server. The QUIC listener binds the same IPv4/IPv6 addresses as the TCP listeners, on the TCP port unless `http3.port` is set:

```c
grpc_server *server = grpc_server_new(&(grpc_config){
    .port = 50051,
    .tls = {.cert_file = "server.crt", .key_file = "server.key"},
    .http3 = {
        .enable = true,
        .advertise_alt_svc = true, /* HTTP/2 responses carry alt-svc: h3=":50051" */
    },
});
printf("HTTP/2 on tcp/%u, HTTP/3 on udp/%u\n", grpc_server_port(server),
       grpc_server_http3_port(server));
```

| `grpc_http3_config` field | Meaning |
|:---|:---|
| `enable` | Also accept gRPC over HTTP/3. Requires `config.tls` and an IP address (not a Unix socket only) |
| `port` | UDP port; `0` uses the TCP port |
| `max_idle_timeout_ms` | QUIC idle timeout; `0` uses `max_connection_idle_ms`, else 30 s |
| `advertise_alt_svc` | Add `alt-svc: h3=":<port>"` to HTTP/2 responses |
| `require_retry` | Validate client addresses with a QUIC Retry before allocating connection state (one extra round trip; mitigates address spoofing) |

How the other server settings apply to HTTP/3:

- `max_connections` counts TCP and QUIC connections together.
- `max_concurrent_streams` limits the request streams of each QUIC connection.
- `max_message_size` sets the per-stream flow-control window, as with HTTP/2.
- `keepalive_time_ms` turns on QUIC keep-alive (PING frames from ngtcp2).
- `max_connection_age_ms` and `max_connection_idle_ms` send an HTTP/3 GOAWAY, then close the connection.
- `grpc_server_drain()` stops accepting new QUIC connections and sends GOAWAY to the existing ones.
- `grpc_server_reload_tls()` swaps the QUIC TLS context too. Existing connections keep the context they started with.
- `grpc_call_peer()` and `grpc_call_peer_cert_subject()` work as with TLS over TCP.

---

## Client

Set `transport = GRPC_TRANSPORT_HTTP3`. TLS is required:

```c
grpc_err err;
grpc_client *client = grpc_client_new(&(grpc_client_config){
    .host = "127.0.0.1",
    .port = 50051,
    .transport = GRPC_TRANSPORT_HTTP3,
    .tls = {.enable = true, .ca_file = "ca.crt", .server_name = "localhost"},
}, &err);
```

Everything else is the same as over HTTP/2: unary and streaming calls, deadlines, retries, compression, round-robin across `targets`, mTLS (`tls.cert_file` / `tls.key_file`), and one RPC at a time per connection. With HTTP/3, client TLS works on every QUIC-capable backend, not only the OpenSSL family as with HTTP/2.

| Error | Cause |
|:---|:---|
| `GRPC_ERR_UNSUPPORTED` | The library was built without HTTP/3 |
| `GRPC_ERR_INVALID` | `tls.enable` is false, or a Unix-socket target was given |
| `GRPC_ERR_IO` | The QUIC handshake failed or timed out (`timeout_ms`) |

With TLS 1.3, a server that requires a client certificate may reject a client without one only after the client finished its handshake. The rejection then surfaces on the first RPC rather than in `grpc_client_new()`.

---

## Limitations

- **No 0-RTT** (early data) and no connection migration: the server advertises `disable_active_migration`.
- **No draining period:** a closed connection is freed right after its CONNECTION_CLOSE is sent, without waiting out the QUIC draining period.
- **Datagrams are sent with `uv_udp_try_send`**, one packet at a time, with no GSO or pacing beyond ngtcp2's. A full socket buffer drops the packet and QUIC loss recovery resends it.
- **Windows builds keep HTTP/3 off.** The CI does not install ngtcp2/nghttp3 there.
- **Python `grpcio` has no HTTP/3.** HTTP/3 is tested with the library's own client (`src/tests/http3.c`). For interoperability checks, use .NET `Grpc.Net.Client` (which supports HTTP/3) or `curl --http3` with a gRPC-framed body.
