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
- `reuse_port` lets several servers (one per thread) share the UDP port, as with the TCP listeners; see [Connections on the move](#connections-on-the-move).

### Connections on the move

- **Migration.** Clients may move to a new address, either deliberately (active migration with a new connection ID) or because a NAT rebinds them. ngtcp2 validates the new path, and the server answers there. `grpc_call_peer()` reports the address of the connection's current path.
- **Servers sharing a port.** With `reuse_port`, the kernel hands each datagram to one of the servers by the client's address, so after a client's address changes its packets may reach a server that does not have the connection. The servers of a process keep a shared table of their connection IDs: such a datagram is passed to the loop of the server that owns the connection, which answers from the same port.
- **Closing period.** After a connection closes, its connection IDs stay known for three probe timeouts, at least 1 second (RFC 9000 section 10.2). If the server sent the CONNECTION_CLOSE, a client that missed it gets it again on its next packet, instead of waiting for its idle timeout. If the client closed first, its late packets are dropped. The connection slot is free meanwhile.

### Sending

Packets of a flush go out in batches. On Linux, each batch is one `sendmsg()` with `UDP_SEGMENT` (generic segmentation offload). If the route cannot segment, the server stops trying and sends packet by packet. When a socket buffer is full, datagrams wait in libuv's send queue in order, up to 4 MiB per server (and per client connection), instead of being dropped. Pacing follows ngtcp2's send quantum and timer. The client sends the same way.

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

## Example

[`examples/http3/`](../examples/http3/) has a server and a client:

- **`server.c`** serves a small Greeter (unary, server-streaming and bidirectional calls, plus health checking) over HTTP/3 and HTTP/2 on one port number, with Alt-Svc.
- **`client.c`** calls it over HTTP/3.

The CMake build creates a self-signed certificate for `localhost` next to them:

```bash
cmake --preset http3 && cmake --build --preset http3
cd build/http3/generated/http3-example
../../http3_server.bin 50443 &
../../http3_client.bin localhost:50443
```

`ctest -R integration_http3_example` runs the same exchange as a test.

---

## Limitations

- **No 0-RTT** (early data), deliberately. A client's first flight could carry requests, but an attacker on the path can replay early data, and gRPC calls are not idempotent in general. Accepting it safely would need the application to mark replay-safe methods, plus single-use session tickets in every TLS backend. Every connection does a full 1-RTT handshake.
- **Servers sharing a port** forward datagrams only within one process. Separate processes on one port (`SO_REUSEPORT` across processes) still lose a connection whose client moves to an address the kernel maps to another process.
- **The client** does not migrate on its own, and frees a connection as soon as it closes it. Its loop runs only during calls, so there is no closing period on that side.
- **GSO** is Linux-only. Other systems send packet by packet.
- **Windows builds keep HTTP/3 off.** The CI does not install ngtcp2/nghttp3 there.
- **Python `grpcio` has no HTTP/3.** HTTP/3 is tested with the library's own client (`src/tests/http3.c`). For interoperability checks, use .NET `Grpc.Net.Client` (which supports HTTP/3) or `curl --http3` with a gRPC-framed body.
