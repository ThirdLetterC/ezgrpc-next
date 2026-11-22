#pragma once

/* internal_http3_client.h - state shared by the HTTP/3 client transport's files
 *
 * grpc_http3_client.c (helpers, I/O and the hooks grpc_client.c calls) and
 * grpc_http3_client_callbacks.c (nghttp3 and ngtcp2 callbacks). Compiled only with
 * GRPC_ENABLE_HTTP3.
 */
#include <nghttp3/nghttp3.h>
#include <ngtcp2/ngtcp2.h>
#include <ngtcp2/ngtcp2_crypto.h>

#include "core.h"
#include "internal_client.h"
#include "internal_http3.h"
#include "internal_quic_tls.h"

enum { H3C_CID_LEN = 18 };
enum { H3C_CHUNK = 16384 };
enum { H3C_MAX_UDP_PAYLOAD = 1452 };
/* Most bytes of datagrams waiting in a connection's UDP send queue. */
enum { H3C_SEND_QUEUE_MAX = 4 * 1024 * 1024 };

/* A request-body chunk handed to nghttp3, freed once acknowledged. */
typedef struct h3c_chunk h3c_chunk;
struct h3c_chunk {
  h3c_chunk *next;
  size_t len;
  uint8_t data[H3C_CHUNK];
};

/* Per-stream state (grpc_client_stream.h3), also the nghttp3 stream user data. It outlives the
 * RPC when the application frees the stream before nghttp3 closes it (st becomes nullptr and the
 * state moves to the connection's orphan list until h3c_on_stream_close() or disconnect). */
typedef struct h3c_stream h3c_stream;
struct h3c_stream {
  grpc_client_stream *st;
  h3c_chunk *head;
  h3c_chunk *tail;
  size_t head_acked;
  /* nghttp3 closed the stream; nothing references the chunks any more. */
  bool closed;
  h3c_stream *orphan_prev;
  h3c_stream *orphan_next;
};

/* The QUIC connection of a subchannel (client_subchannel.h3c). */
struct h3c_conn {
  client_subchannel *s;
  ngtcp2_conn *conn;
  nghttp3_conn *h3;
  grpc_quic_tls_session *tls;
  ngtcp2_crypto_conn_ref conn_ref;
  uv_udp_t udp;
  uv_timer_t timer;
  int open_handles;
  struct sockaddr_storage local;
  socklen_t locallen;
  struct sockaddr_storage remote;
  socklen_t remotelen;
  ngtcp2_ccerr ccerr;
  bool handshake_done;
  /* when the last packet arrived (h3c_now()), to tell an idled-out connection from a live one */
  ngtcp2_tstamp last_rx;
  /* the server sent GOAWAY (or its shutdown notice): no new request goes on this connection */
  bool goaway_received;
  /* datagrams the kernel could not take at once, queued with uv_udp_send() (as on the server) */
  size_t send_queued_bytes;
  uint64_t stat_send_queued;
  uint64_t stat_send_dropped;
  /* batches sent with one GSO call (Linux), and whether GSO turned out unusable here */
  uint64_t stat_gso_sends;
  bool gso_off;
  /* Stream states whose RPC was freed before nghttp3 closed the stream. */
  h3c_stream *orphans;
  uint8_t rbuf[65536];
  uint8_t sbuf[H3C_MAX_UDP_PAYLOAD];
  /* packets of one flush, sent together with GSO where available */
  uint8_t gsobuf[GRPC_H3_GSO_BUF];
};
typedef struct h3c_conn h3c_conn;

/* ---- helpers (grpc_http3_client.c) ---- */
/* Fills dest with random bytes from the OS CSPRNG. */
void h3c_random(uint8_t *dest, size_t len);

/* The active stream if it is `stream_id`, else nullptr. */
grpc_client_stream *h3c_active(h3c_conn *hc, int64_t stream_id);

/* Returns flow-control credit for consumed bytes. */
void h3c_consume(h3c_conn *hc, int64_t stream_id, size_t n);

/* Unlinks an orphaned stream state from the connection and frees it with its chunks. */
void h3c_orphan_free(h3c_conn *hc, h3c_stream *hs);

/* ---- callbacks (grpc_http3_client_callbacks.c) ---- */

/* The request body reader handed to nghttp3_conn_submit_request(). */
extern const nghttp3_data_reader h3c_data_reader;

/* Sets the client's own ngtcp2 callbacks in cb (the ngtcp2_crypto ones are set by the caller). */
void h3c_set_quic_callbacks(ngtcp2_callbacks *cb);
