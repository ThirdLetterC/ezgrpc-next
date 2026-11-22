/* grpc_client_response.c - interpreting a response for the client, whatever the transport
 *
 * Header fields (grpc-status, grpc-message, grpc-encoding, metadata) and body bytes, split into
 * messages and decompressed, as delivered by the HTTP/2 callbacks (grpc_client_io.c) and the
 * HTTP/3 transport (grpc_http3_client.c).
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

/* Appends a copy of a header name/value to a grpc_metadata array grown with grpc_realloc.
 * Allocation failures drop the entry silently. */
static void md_list_append(grpc_metadata **arr, size_t *count, const uint8_t *name, size_t nlen,
                           const uint8_t *val, size_t vlen) {
  grpc_metadata *next = grpc_realloc(*arr, (*count + 1) * sizeof(*next));
  if (next == nullptr)
    return;
  *arr = next;
  char *k = grpc_strndup((const char *)name, nlen);
  char *v = grpc_strndup((const char *)val, vlen);
  if (k == nullptr || v == nullptr) {
    grpc_free(k);
    grpc_free(v);
    return;
  }
  (*arr)[(*count)++] = (grpc_metadata){.key = k, .key_len = nlen, .value = v, .value_len = vlen};
}

/* Parses a grpc-status value: 1-2 decimal digits naming a status 0..16; anything else (empty,
 * signs, spaces, letters, out of range) is UNKNOWN, as the gRPC spec requires. */
static grpc_status parse_grpc_status(const uint8_t *value, size_t len) {
  if (len == 0 || len > 2)
    return GRPC_STATUS_UNKNOWN;
  unsigned v = 0;
  for (size_t i = 0; i < len; i++) {
    if (value[i] < '0' || value[i] > '9')
      return GRPC_STATUS_UNKNOWN;
    v = v * 10 + (unsigned)(value[i] - '0');
  }
  return v <= 16 ? (grpc_status)v : GRPC_STATUS_UNKNOWN;
}

/* The gRPC status for an HTTP status when the response carries no grpc-status (the gRPC spec's
 * "HTTP to gRPC Status Code Mapping"). */
static grpc_status status_from_http(unsigned http) {
  switch (http) {
  case 400:
    return GRPC_STATUS_INTERNAL;
  case 401:
    return GRPC_STATUS_UNAUTHENTICATED;
  case 403:
    return GRPC_STATUS_PERMISSION_DENIED;
  case 404:
    return GRPC_STATUS_UNIMPLEMENTED;
  case 429:
  case 502:
  case 503:
  case 504:
    return GRPC_STATUS_UNAVAILABLE;
  default:
    return GRPC_STATUS_UNKNOWN;
  }
}

/* Decodes the percent-encoding of grpc-message (%XX; malformed escapes are kept as they are)
 * into a NUL-terminated grpc_malloc string, or nullptr when out of memory. */
static int hex_digit(uint8_t c) {
  return c >= '0' && c <= '9'   ? c - '0'
         : c >= 'A' && c <= 'F' ? c - 'A' + 10
         : c >= 'a' && c <= 'f' ? c - 'a' + 10
                                : -1;
}

static char *percent_decode(const uint8_t *value, size_t len) {
  char *out = grpc_malloc(len + 1);
  if (out == nullptr)
    return nullptr;
  size_t o = 0;
  for (size_t i = 0; i < len; i++) {
    int hi = value[i] == '%' && i + 2 < len ? hex_digit(value[i + 1]) : -1;
    int lo = hi >= 0 ? hex_digit(value[i + 2]) : -1;
    if (lo >= 0) {
      out[o++] = (char)(hi * 16 + lo);
      i += 2;
    } else {
      out[o++] = (char)value[i];
    }
  }
  out[o] = '\0';
  return out;
}

void client_resp_header(grpc_client_stream *st, const uint8_t *name, size_t namelen,
                        const uint8_t *value, size_t valuelen, bool trailer) {
  if (st->rejected) /* the outcome is decided; trailers arriving after it do not change it */
    return;
  if (namelen == 7 && memcmp(name, ":status", 7) == 0) {
    unsigned http = 0;
    for (size_t i = 0; i < valuelen && value[i] >= '0' && value[i] <= '9'; i++)
      http = http * 10 + (unsigned)(value[i] - '0');
    /* without grpc-status, a non-200 response maps to a gRPC status (grpc-status overrides) */
    if (http != 200 && !st->grpc_status_seen)
      st->resp.status = status_from_http(http);
  } else if (namelen == 11 && memcmp(name, "grpc-status", 11) == 0) {
    st->resp.status = parse_grpc_status(value, valuelen);
    st->grpc_status_seen = true;
  } else if (namelen == 12 && memcmp(name, "grpc-message", 12) == 0) {
    grpc_free(st->resp.status_message);
    st->resp.status_message = percent_decode(value, valuelen);
  } else if (namelen == 13 && memcmp(name, "grpc-encoding", 13) == 0) {
    bool supported = true;
    if (valuelen == 4 && memcmp(value, "gzip", 4) == 0)
      st->resp_encoding = GRPC_COMPRESS_GZIP;
    else if (valuelen == 7 && memcmp(value, "deflate", 7) == 0)
      st->resp_encoding = GRPC_COMPRESS_DEFLATE;
    else if (valuelen == 4 && memcmp(value, "zstd", 4) == 0)
      st->resp_encoding = GRPC_COMPRESS_ZSTD;
    else if (valuelen == 6 && memcmp(value, "snappy", 6) == 0)
      st->resp_encoding = GRPC_COMPRESS_SNAPPY;
    else {
      st->resp_encoding = GRPC_COMPRESS_IDENTITY;
      supported = valuelen == 8 && memcmp(value, "identity", 8) == 0;
    }
#if !defined(GRPC_ENABLE_ZLIB)
    if (st->resp_encoding == GRPC_COMPRESS_GZIP || st->resp_encoding == GRPC_COMPRESS_DEFLATE)
      supported = false;
#endif
#if !defined(GRPC_ENABLE_ZSTD)
    if (st->resp_encoding == GRPC_COMPRESS_ZSTD)
      supported = false;
#endif
#if !defined(GRPC_ENABLE_SNAPPY)
    if (st->resp_encoding == GRPC_COMPRESS_SNAPPY)
      supported = false;
#endif
    st->resp_encoding_unsupported = !supported;
  }

  if (!trailer)
    md_list_append(&st->resp.headers, &st->resp.nb_headers, name, namelen, value, valuelen);
  else
    md_list_append(&st->resp.trailers, &st->resp.nb_trailers, name, namelen, value, valuelen);
}

/* Fails the stream on a response it cannot accept: err for the caller, and an INTERNAL status
 * (unless a limit was exceeded) with `why` as its message. */
static void stream_reject(grpc_client_stream *st, grpc_err err, const char *why) {
  st->err = err;
  st->stream_done = true;
  st->rejected = true;
  if (err != GRPC_ERR_LIMIT && err != GRPC_ERR_NOMEM) {
    st->resp.status = GRPC_STATUS_INTERNAL;
    grpc_free(st->resp.status_message);
    st->resp.status_message = grpc_strdup(why);
  }
}

/* Splits complete length-prefixed gRPC messages off the front of recv_buf. Every message,
 * including an incomplete one, must declare at most max_message_size bytes (GRPC_ERR_LIMIT
 * otherwise, as soon as its prefix arrived). Compressed messages (flag 1) are decompressed with
 * the response's grpc-encoding within max_message_size; a compressed message without a usable
 * grpc-encoding, corrupt compressed data or a flag other than 0/1 fail the stream (status
 * INTERNAL, GRPC_ERR_CODEC / GRPC_ERR_PROTOCOL), as the gRPC spec requires. Each message goes to
 * on_message (then freed) when set, otherwise to recv_queue. Leaves a partial trailing message in
 * recv_buf. */
static void stream_drain_messages(grpc_client_stream *st) {
  client_subchannel *s = st->sub;
  uint64_t max_size = s->parent->config.max_message_size;
  size_t off = 0;
  while (st->recv_buf.len - off >= 5) {
    uint8_t cflag = st->recv_buf.data[off];
    uint32_t mlen = ntohl(uread_u32(st->recv_buf.data + off + 1));
    if (mlen > max_size) {
      stream_reject(st, GRPC_ERR_LIMIT, "response message larger than max_message_size");
      return;
    }
    if (cflag > 1) {
      stream_reject(st, GRPC_ERR_PROTOCOL, "invalid compressed flag");
      return;
    }
    if (st->recv_buf.len - off < 5 + (size_t)mlen)
      break;
    const uint8_t *payload = st->recv_buf.data + off + 5;
    grpc_message *msg = nullptr;
    if (cflag == 1) {
      if (st->resp_encoding == GRPC_COMPRESS_IDENTITY || st->resp_encoding_unsupported) {
        stream_reject(st, GRPC_ERR_CODEC, "compressed response without a supported grpc-encoding");
        return;
      }
      uint8_t *decomp = nullptr;
      size_t dlen = 0;
      grpc_err derr = grpc_decompress_limited(st->resp_encoding, payload, mlen, &decomp, &dlen,
                                              (size_t)max_size);
      if (derr != GRPC_OK) {
        stream_reject(st, derr, "failed to decompress the response");
        return;
      }
      msg = message_create(0, decomp, dlen);
      grpc_free(decomp);
    } else {
      msg = message_create(0, payload, mlen);
    }
    if (msg == nullptr) {
      stream_reject(st, GRPC_ERR_NOMEM, "out of memory");
      return;
    }
    if (st->on_message != nullptr) {
      st->on_message(msg, st->on_message_userdata);
      grpc_message_free(msg);
    } else if (msg_queue_push(&st->recv_queue, msg) != GRPC_OK) {
      grpc_message_free(msg);
      stream_reject(st, GRPC_ERR_NOMEM, "out of memory");
      return;
    }
    off += 5 + (size_t)mlen;
  }
  if (off > 0) {
    memmove(st->recv_buf.data, st->recv_buf.data + off, st->recv_buf.len - off);
    st->recv_buf.len -= off;
  }
}

int client_resp_data(grpc_client_stream *st, const uint8_t *data, size_t len) {
  if (st->stream_done)
    return 0;
  if (buf_append(&st->recv_buf, data, len)) {
    st->err = GRPC_ERR_NOMEM;
    st->stream_done = true;
    return -1;
  }
  stream_drain_messages(st);
  return 0;
}
