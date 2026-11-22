#pragma once

/* internal_http3_server.h - state shared by the HTTP/3 server transport's files
 *
 * grpc_http3_server.c (helpers, transport ops, server lifecycle), grpc_http3_server_callbacks.c
 * (nghttp3 and ngtcp2 callbacks) and grpc_http3_server_conn.c (connections and incoming packets).
 * Compiled only with GRPC_ENABLE_HTTP3.
 */
#include <nghttp3/nghttp3.h>
#include <ngtcp2/ngtcp2.h>
#include <ngtcp2/ngtcp2_crypto.h>

#include "core.h"
#include "internal_http3.h"
#include "internal_quic_tls.h"
#include "tls_ctx_pool.h"

/* Length of the connection IDs the server issues. */
enum { H3_SCID_LEN = 18 };
/* Buckets of the connection-ID table (a power of two). */
enum { H3_CID_BUCKETS = 4096 };
/* Size of one response send chunk. */
enum { H3_CHUNK = 16384 };
/* Largest UDP payload written. */
enum { H3_MAX_UDP_PAYLOAD = 1452 };
/* How long a Retry token stays valid. */
#define H3_RETRY_TOKEN_TIMEOUT (10 * NGTCP2_SECONDS)
/* A connection that has not completed its handshake by then is dropped, so unanswered Initials
 * (possibly from spoofed addresses) do not hold a connection slot for the whole idle timeout. */
#define H3_HANDSHAKE_TIMEOUT (10 * NGTCP2_SECONDS)
/* Shortest closing / draining period: RFC 9000 asks for at least three PTOs, which on a fast path
 * is a few milliseconds; a closed connection's record is small. */
#define H3_MIN_LINGER (1 * NGTCP2_SECONDS)
/* Most bytes of datagrams waiting in the UDP send queue (per server). Congestion control keeps
 * a connection's data in flight within its window, so this is only reached under overload. */
enum { H3_SEND_QUEUE_MAX = 4 * 1024 * 1024 };

typedef struct grpc_quic_conn grpc_quic_conn;

/* One entry of the connection-ID table. */
typedef struct cid_entry cid_entry;
struct cid_entry {
  ngtcp2_cid cid;
  grpc_quic_conn *qc;
  cid_entry *next;
};

/* The HTTP/3 state of a server (server->h3). */
typedef struct grpc_h3_server {
  grpc_server *server;
  uv_udp_t udp4;
  uv_udp_t udp6;
  bool udp4_active;
  bool udp6_active;
  /* Bound local addresses, used as the local side of every path. */
  struct sockaddr_storage local4;
  struct sockaddr_storage local6;
  uint16_t port;
  bool accepting;

  /* the active QUIC TLS context and replaced ones still used by live connections */
  tls_ctx_pool tls_pool;

  /* closed connections in their closing / draining period (grpc_quic_conn.lingering) */
  grpc_quic_conn *lingering;

  /* With config.reuse_port, servers on other loops share the UDP port, and the kernel picks the
   * socket by address 4-tuple: after a client's address changes, its packets may reach a server
   * that does not know the connection. This server's connection IDs are then also listed in a
   * process-wide registry, and such packets are forwarded to the owner's loop. */
  bool cluster;

  /* Datagrams the kernel could not take right away (a full socket buffer) are queued with
   * uv_udp_send() instead of dropped, up to H3_SEND_QUEUE_MAX bytes; libuv keeps them in order,
   * since uv_udp_try_send() fails while its queue is not empty. */
  size_t send_queued_bytes;
  /* counters (loop thread; read by tests): datagrams queued, and dropped with the queue full */
  uint64_t stat_send_queued;
  uint64_t stat_send_dropped;
  /* batches sent with one GSO call (Linux), and whether GSO turned out unusable here */
  uint64_t stat_gso_sends;
  bool gso_off;

  /* Connections whose client address is not validated yet (no Retry token, handshake not
   * complete). Above half the free connection slots, new clients must answer a Retry first. */
  size_t nb_unvalidated;

  /* Secret for stateless reset and Retry tokens, random per server. */
  uint8_t secret[32];

  cid_entry *cids[H3_CID_BUCKETS];

  /* Datagram buffers; the loop is single-threaded and each datagram is handled synchronously. */
  uint8_t rbuf[65536];
  uint8_t sbuf[H3_MAX_UDP_PAYLOAD];
  /* packets of one flush, sent together with GSO where available */
  uint8_t gsobuf[GRPC_H3_GSO_BUF];
} grpc_h3_server;

/* One QUIC connection (session->quic). */
struct grpc_quic_conn {
  grpc_session *session;
  grpc_h3_server *h3s;
  ngtcp2_conn *conn;
  nghttp3_conn *h3;
  grpc_quic_tls_session *tls;
  ngtcp2_crypto_conn_ref conn_ref;
  uv_udp_t *udp;
  struct sockaddr_storage local;
  socklen_t locallen;
  struct sockaddr_storage remote;
  socklen_t remotelen;
  uv_timer_t timer;
  /* Error sent with CONNECTION_CLOSE. */
  ngtcp2_ccerr ccerr;
  /* Connection IDs registered in the table for this connection. */
  ngtcp2_cid *cids;
  size_t nb_cids;
  size_t cap_cids;
  /* the HTTP/3 GOAWAY went out (h3_goaway() sends it once) */
  bool goaway_sent;
  /* counted in h3s->nb_unvalidated until the handshake completes or the connection closes */
  bool unvalidated;
  /* the QUIC TLS context `tls` was created from (released to h3s->tls_pool) */
  void *tls_ctx_used;

  /* Closing / draining period (RFC 9000 section 10.2): after the connection closed, only its
   * connection IDs stay routed here for a while (h3s->lingering list); the session slot, streams,
   * HTTP/3, TLS and ngtcp2 state are already gone and `session` is nullptr. In the closing state
   * (we sent CONNECTION_CLOSE, kept in close_pkt) late packets are answered with it again; in
   * the draining state (the peer closed: close_pkt is nullptr) they are dropped. */
  bool lingering;
  /* the peer's CONNECTION_CLOSE arrived (ngtcp2 reported NGTCP2_ERR_DRAINING) */
  bool peer_closed;
  uint8_t *close_pkt;
  size_t close_pkt_len;
  /* packets received while closing; the close is resent on the 1st, 2nd, 4th, 8th, ... */
  uint64_t linger_rx;
  grpc_quic_conn *linger_prev;
  grpc_quic_conn *linger_next;
};

/* Per-stream transport state (grpc_stream.transport). */
typedef struct h3_chunk h3_chunk;
struct h3_chunk {
  h3_chunk *next;
  size_t len;
  uint8_t data[H3_CHUNK];
};
typedef struct h3_stream {
  /* Body bytes handed to nghttp3 and not yet acknowledged, oldest first. */
  h3_chunk *head;
  h3_chunk *tail;
  size_t head_acked;
  /* Response headers waiting for the body (nghttp3 submits headers and body together). */
  grpc_nv *headers;
  size_t nb_headers;
  bool response_submitted;
  /* The body is complete; the data reader returns EOF. */
  bool body_done;
} h3_stream;

extern const grpc_transport_ops grpc_h3_transport_ops;

/* ---- helpers (grpc_http3_server.c) ---- */
/* Current time for ngtcp2 / nghttp3, in nanoseconds of the monotonic clock. */
ngtcp2_tstamp h3_now(void);

/* Fills dest with random bytes from the OS CSPRNG. */
void h3_random(uint8_t *dest, size_t len);

/* Returns the connection owning `cid`, or nullptr. */
grpc_quic_conn *h3_cid_lookup(grpc_h3_server *h3s, const uint8_t *data, size_t len);

/* Routes `cid` to qc and remembers it for removal. Returns 0, or -1 when out of memory. */
int h3_cid_add(grpc_quic_conn *qc, const ngtcp2_cid *cid);

/* Removes the table entry routing `cid` to qc. */
void h3_cid_remove(grpc_quic_conn *qc, const ngtcp2_cid *cid);

/* Sends one datagram on the connection's socket. A full socket buffer drops it; QUIC loss
 * recovery retransmits. */
void h3_udp_send(grpc_server *server, uv_udp_t *udp, const struct sockaddr *to, const uint8_t *data,
                 size_t len);

/* Sends a packet ngtcp2 wrote for `path` (the path it chose, which changes when the client's
 * address does, e.g. after NAT rebinding): to the path's remote address, from the socket of the
 * path's local address family. The connection remembers the latest remote address for its
 * CONNECTION_CLOSE. */
void qc_send(grpc_quic_conn *qc, const ngtcp2_path *path, const uint8_t *data, size_t len);

/* Sends `len` bytes of packets of `gsolen` bytes each (the last may be shorter) on `path`: with one
 * GSO call where possible, else packet by packet through qc_send(). */
void qc_send_batch(grpc_quic_conn *qc, const ngtcp2_path *path, const uint8_t *data, size_t len,
                   size_t gsolen);

/* ---- callbacks (grpc_http3_server_callbacks.c) ---- */

/* The response body reader handed to nghttp3_conn_submit_response(). */
extern const nghttp3_data_reader h3_data_reader;

/* Sets the server's own ngtcp2 callbacks in cb (the ngtcp2_crypto ones are set by the caller). */
void h3_server_set_quic_callbacks(ngtcp2_callbacks *cb);
/* Frees a stream's send chunks and parked headers. */
void h3_stream_free(grpc_stream *stream);

/* ---- connections (grpc_http3_server_conn.c) ---- */

/* Starts receiving datagrams on a bound UDP handle of the server. Returns uv_udp_recv_start()'s
 * result. */
int h3_server_udp_recv_start(uv_udp_t *udp);
/* close op: sends CONNECTION_CLOSE (unless the connection is draining or closed by idle timeout),
 * stops routing its connection IDs, detaches every call with UNAVAILABLE, frees nghttp3, ngtcp2
 * and TLS state, and closes the timer; its close callback returns the slot. */
void h3_session_close(grpc_session *session);

/* flush op: writes everything ngtcp2 and nghttp3 have to send, packet by packet, then re-arms
 * the timer. Errors close the session. */
void h3_session_flush(grpc_session *session);

/* With h3s->cluster: when another server lists the connection ID `dcid`, posts a copy of the
 * datagram to that server's loop and returns true. */
bool h3_forward_to_owner(grpc_h3_server *h3s, const uint8_t *dcid, size_t dcidlen,
                         const uint8_t *data, size_t len, const struct sockaddr *from);

/* Handles one received datagram: routes it by destination connection ID (creating a connection
 * for a new client, or forwarding it to the server that owns the connection on another loop
 * unless `forwarded`) and feeds it to ngtcp2. `udp` is the socket it arrived on (for a forwarded
 * datagram, this server's socket of the same family, sharing the port). */
void h3_handle_datagram(grpc_h3_server *h3s, uv_udp_t *udp, const uint8_t *data, size_t len,
                        const struct sockaddr *addr, bool forwarded);

/* Ends a closed connection's closing / draining period now: unroutes its connection IDs and frees
 * it (server shutdown, or the period elapsed). */
void h3_linger_end(grpc_quic_conn *qc);

/* ngtcp2 moves a connection to a new client path as soon as a non-probing packet arrives on it
 * (active migration or NAT rebinding) and validates the path meanwhile, falling back if that fails.
 * Whatever path is current, its address is the peer that handlers and logs see. */
void h3_qc_note_path(grpc_quic_conn *qc);
