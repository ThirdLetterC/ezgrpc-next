/* grpc_metadata.c - Base64 codec for binary ("-bin") metadata values
 *
 * gRPC carries binary metadata values Base64-encoded in HTTP/2 headers whose key ends in "-bin".
 * The encoder always emits padded standard Base64; the decoder is lenient and accepts padded or
 * unpadded input. Both return buffers from grpc_malloc that the caller must release with
 * grpc_free (not libc free: on MSVC the default allocator uses _aligned_malloc/_aligned_free).
 */

#include <stdlib.h>
#include <string.h>
#include "internal/grpc_platform.h"
#include "grpc_metadata.h"
#include "grpc_mem.h"
#include "core.h"

/* standard Base64 alphabet (RFC 4648 section 4), not the URL-safe variant */
static const char b64_table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/* Encodes in_len bytes as padded standard Base64 into a NUL-terminated grpc_malloc buffer and
 * stores the encoded length (without the NUL) in *out_len when out_len is non-null. Returns
 * nullptr if data is null or allocation fails. */
GRPC_API char *grpc_metadata_encode_bin(const uint8_t *data, size_t in_len, size_t *out_len) {
  if (data == nullptr) {
    return nullptr;
  }

  size_t olen = 4 * ((in_len + 2) / 3);
  char *out = grpc_malloc(olen + 1);
  if (out == nullptr) {
    return nullptr;
  }

  size_t i = 0, j = 0;
  while (i < in_len) {
    size_t rem = in_len - i;
    uint32_t octet_a = (uint8_t)data[i++];
    uint32_t octet_b = (rem > 1) ? (uint8_t)data[i++] : 0;
    uint32_t octet_c = (rem > 2) ? (uint8_t)data[i++] : 0;
    uint32_t triple = (octet_a << 16) | (octet_b << 8) | octet_c;

    out[j++] = b64_table[(triple >> 18) & 0x3F];
    out[j++] = b64_table[(triple >> 12) & 0x3F];
    out[j++] = (rem > 1) ? b64_table[(triple >> 6) & 0x3F] : '=';
    out[j++] = (rem > 2) ? b64_table[triple & 0x3F] : '=';
  }
  out[j] = '\0';

  if (out_len != nullptr) {
    *out_len = j;
  }
  return out;
}

/* Maps a Base64 alphabet character to its 6-bit value, or returns -1 for any other byte. */
static inline int b64_char_val(char c) {
  if (c >= 'A' && c <= 'Z')
    return c - 'A';
  if (c >= 'a' && c <= 'z')
    return c - 'a' + 26;
  if (c >= '0' && c <= '9')
    return c - '0' + 52;
  if (c == '+')
    return 62;
  if (c == '/')
    return 63;
  return -1;
}

grpc_err base64_decode(const char *in, size_t len, bool concatenated, uint8_t **out,
                       size_t *out_len) {
  *out = nullptr;
  *out_len = 0;
  /* every 4 characters carry 3 bytes; a final group of 2 or 3 characters carries 1 or 2 */
  uint8_t *dec = grpc_malloc(len / 4 * 3 + 3);
  if (dec == nullptr)
    return GRPC_ERR_NOMEM;
  size_t j = 0;
  uint32_t quad = 0;  /* 6-bit values of the current group */
  size_t nq = 0;      /* characters of the current group seen, padding included */
  size_t npad = 0;    /* '=' in the current group */
  bool ended = false; /* a padded group ended the value (one-value mode) */

  for (size_t i = 0; i < len; i++) {
    char c = in[i];
    if (ended)
      goto invalid; /* nothing may follow the padding of a single value */
    if (c == '=') {
      if (nq < 2) /* "=..." or "x=..": no complete byte before the padding */
        goto invalid;
      npad++;
      nq++;
    } else {
      int v = b64_char_val(c);
      if (v < 0 || npad > 0) /* outside the alphabet, or data after padding in a group */
        goto invalid;
      quad = (quad << 6) | (uint32_t)v;
      nq++;
    }
    if (nq == 4) {
      size_t data_chars = 4 - npad;
      quad <<= 6 * npad;
      dec[j++] = (uint8_t)(quad >> 16);
      if (data_chars >= 3)
        dec[j++] = (uint8_t)(quad >> 8);
      if (data_chars == 4)
        dec[j++] = (uint8_t)quad;
      if (npad > 0 && !concatenated)
        ended = true;
      quad = 0;
      nq = 0;
      npad = 0;
    }
  }
  if (nq > 0) {
    /* an unpadded final group: 2 or 3 characters, never 1, and no partial padding */
    if (npad > 0 || nq == 1)
      goto invalid;
    quad <<= 6 * (4 - nq);
    dec[j++] = (uint8_t)(quad >> 16);
    if (nq == 3)
      dec[j++] = (uint8_t)(quad >> 8);
  }
  *out = dec;
  *out_len = j;
  return GRPC_OK;

invalid:
  grpc_free(dec);
  return GRPC_ERR_INVALID;
}

/* Strict Base64 decode of one -bin value (see base64_decode()). Empty input yields a 1-byte
 * allocation with *out_len set to 0. */
GRPC_API uint8_t *grpc_metadata_decode_bin(const char *base64_str, size_t in_len, size_t *out_len) {
  if (base64_str == nullptr)
    return nullptr;
  uint8_t *out = nullptr;
  size_t n = 0;
  if (base64_decode(base64_str, in_len, false, &out, &n) != GRPC_OK)
    return nullptr;
  if (out_len != nullptr)
    *out_len = n;
  return out;
}
