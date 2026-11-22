/* grpc_status_details.c - google.rpc.Status encoding for grpc-status-details-bin
 *
 * Implements the gRPC Rich Error Model: google.rpc.Status { int32 code = 1; string message = 2;
 * repeated google.protobuf.Any details = 3; } is serialized by hand and base64-encoded into the
 * grpc-status-details-bin trailer, and decoded from it. The decoders skip unknown fields and
 * understand single-byte tags only. Every buffer they return is allocated with grpc_malloc() and
 * must be released with grpc_free() (or grpc_status_details_free()), never with libc free().
 *
 * grpc_call_finish_with_details() runs on the server thread like grpc_call_finish(); the _async
 * variant encodes on the caller's thread and posts the rest to the server thread.
 */

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include "core.h"

/* Returns the number of bytes `val` takes as a protobuf varint. */
static size_t varint_size(uint64_t val) {
  size_t size = 0;
  do {
    size++;
    val >>= 7;
  } while (val != 0);
  return size;
}

/* Writes `val` as a protobuf varint to `buf`, which must have room for varint_size(val) bytes.
 * Returns the number of bytes written. */
static size_t write_varint(uint8_t *buf, uint64_t val) {
  size_t len = 0;
  do {
    uint8_t b = (uint8_t)(val & 0x7F);
    val >>= 7;
    if (val != 0)
      b |= 0x80;
    buf[len++] = b;
  } while (val != 0);
  return len;
}

/* Reads a protobuf varint at *seek, advancing it, without reading past `len`. A truncated or
 * overlong varint is not reported; the bits read so far are returned. */
static uint64_t read_varint(const uint8_t *buf, size_t len, size_t *seek) {
  uint64_t val = 0;
  size_t shift = 0;
  while (*seek < len) {
    uint8_t b = buf[(*seek)++];
    /* bits past the 64th are dropped: shifting by >= 64 is undefined behavior */
    if (shift < 64)
      val |= (uint64_t)(b & 0x7F) << shift;
    if (!(b & 0x80))
      break;
    shift += 7;
  }
  return val;
}

/* Returns seek advanced by n bytes, clamped to limit. Requires seek <= limit. Comparing n with
 * the remaining length (instead of seek + n with limit) cannot overflow on a hostile length. */
static size_t skip_bytes(size_t seek, uint64_t n, size_t limit) {
  return n <= limit - seek ? seek + (size_t)n : limit;
}

/* read_varint() that reports a varint cut off by `len` (or by nothing at all) in *ok. */
static uint64_t read_varint_checked(const uint8_t *buf, size_t len, size_t *seek, bool *ok) {
  size_t start = *seek;
  uint64_t val = read_varint(buf, len, seek);
  *ok = *seek > start && !(buf[*seek - 1] & 0x80);
  return val;
}

/* Skips the value of a field whose tag was just read, without going past `end`. Returns false
 * for a value that overruns `end` or a wire type protobuf does not allow (3 and 4 are the
 * deprecated groups, 6 and 7 do not exist). */
static bool skip_field(const uint8_t *raw, size_t end, size_t *seek, unsigned wire) {
  bool ok = true;
  switch (wire) {
  case 0:
    (void)read_varint_checked(raw, end, seek, &ok);
    return ok;
  case 1:
  case 5: {
    size_t n = wire == 1 ? 8 : 4;
    if (end - *seek < n)
      return false;
    *seek += n;
    return true;
  }
  case 2: {
    uint64_t n = read_varint_checked(raw, end, seek, &ok);
    if (!ok || n > end - *seek)
      return false;
    *seek += (size_t)n;
    return true;
  }
  default:
    return false;
  }
}

/* The type_url and value of one google.protobuf.Any. */
typedef struct any_fields {
  char *url;
  uint8_t *value;
  size_t value_len;
} any_fields;

static void any_fields_free(any_fields *a) {
  grpc_free(a->url);
  grpc_free(a->value);
  *a = (any_fields){0};
}

/* Parses the Any in raw[seek, end) into *a (zeroed by the caller). A malformed field ends the Any,
 * keeping what was parsed; a repeated field replaces the earlier one. Returns GRPC_OK or
 * GRPC_ERR_NOMEM (then *a is freed). */
static grpc_err parse_any(const uint8_t *raw, size_t seek, size_t end, any_fields *a) {
  while (seek < end) {
    bool ok = true;
    uint64_t tag = read_varint_checked(raw, end, &seek, &ok);
    uint64_t field = tag >> 3;
    unsigned wire = (unsigned)(tag & 0x07);
    if (!ok || field == 0)
      break;
    if ((field == 1 || field == 2) && wire == 2) {
      uint64_t n = read_varint_checked(raw, end, &seek, &ok);
      if (!ok || n > end - seek)
        break;
      size_t len = (size_t)n;
      if (field == 1) {
        char *u = grpc_malloc(len + 1);
        if (u == nullptr) {
          any_fields_free(a);
          return GRPC_ERR_NOMEM;
        }
        memcpy(u, raw + seek, len);
        u[len] = '\0';
        grpc_free(a->url);
        a->url = u;
      } else {
        uint8_t *v = len > 0 ? grpc_malloc(len) : nullptr;
        if (len > 0 && v == nullptr) {
          any_fields_free(a);
          return GRPC_ERR_NOMEM;
        }
        if (len > 0)
          memcpy(v, raw + seek, len);
        grpc_free(a->value);
        a->value = v;
        a->value_len = len;
      }
      seek += len;
    } else if (!skip_field(raw, end, &seek, wire)) {
      break;
    }
  }
  return GRPC_OK;
}

/* Sizes the message in a first pass, then writes it into one buffer and base64-encodes it. The
 * code is always written; empty message, type_url and value fields are omitted. The code is
 * encoded as its uint32 bit pattern, so a negative code takes 5 bytes instead of the 10 that
 * protobuf uses for negative int32. Returns nullptr on allocation failure. */
GRPC_API char *grpc_status_details_encode(int32_t code, const char *message,
                                          const grpc_status_detail_entry *details,
                                          size_t nb_details, size_t *out_len) {
  size_t msg_len = message ? strlen(message) : 0;

  // Calculate total protobuf byte size
  // Field 1: code (tag 1, wire 0)
  size_t total_size = 1 + varint_size((uint32_t)code);

  // Field 2: message (tag 2, wire 2)
  if (msg_len > 0) {
    total_size += 1 + varint_size(msg_len) + msg_len;
  }

  // Field 3: details (repeated Any)
  for (size_t i = 0; i < nb_details; i++) {
    size_t url_len = details[i].type_url ? strlen(details[i].type_url) : 0;
    size_t val_len = details[i].value_len;

    size_t any_inner_size = 0;
    if (url_len > 0) {
      any_inner_size += 1 + varint_size(url_len) + url_len;
    }
    if (val_len > 0) {
      any_inner_size += 1 + varint_size(val_len) + val_len;
    }

    total_size += 1 + varint_size(any_inner_size) + any_inner_size;
  }

  uint8_t *pb_buf = grpc_malloc(total_size);
  if (pb_buf == nullptr) {
    return nullptr;
  }

  size_t offset = 0;

  // 1. Write Field 1: code
  pb_buf[offset++] = (1 << 3) | 0; // 0x08
  offset += write_varint(pb_buf + offset, (uint32_t)code);

  // 2. Write Field 2: message
  if (msg_len > 0) {
    pb_buf[offset++] = (2 << 3) | 2; // 0x12
    offset += write_varint(pb_buf + offset, msg_len);
    memcpy(pb_buf + offset, message, msg_len);
    offset += msg_len;
  }

  // 3. Write Field 3: details
  for (size_t i = 0; i < nb_details; i++) {
    size_t url_len = details[i].type_url ? strlen(details[i].type_url) : 0;
    size_t val_len = details[i].value_len;

    size_t any_inner_size = 0;
    if (url_len > 0) {
      any_inner_size += 1 + varint_size(url_len) + url_len;
    }
    if (val_len > 0) {
      any_inner_size += 1 + varint_size(val_len) + val_len;
    }

    // Status.details tag (3, wire 2)
    pb_buf[offset++] = (3 << 3) | 2; // 0x1A
    offset += write_varint(pb_buf + offset, any_inner_size);

    // Any.type_url (field 1, wire 2)
    if (url_len > 0) {
      pb_buf[offset++] = (1 << 3) | 2; // 0x0A
      offset += write_varint(pb_buf + offset, url_len);
      memcpy(pb_buf + offset, details[i].type_url, url_len);
      offset += url_len;
    }

    // Any.value (field 2, wire 2)
    if (val_len > 0) {
      pb_buf[offset++] = (2 << 3) | 2; // 0x12
      offset += write_varint(pb_buf + offset, val_len);
      memcpy(pb_buf + offset, details[i].value, val_len);
      offset += val_len;
    }
  }

  // Base64 encode for trailing metadata
  char *b64 = grpc_metadata_encode_bin(pb_buf, offset, out_len);
  grpc_free(pb_buf);
  return b64;
}

/* Lenient decoder: returns GRPC_ERR_INVALID only when the input is null or not Base64 (and
 * GRPC_ERR_NOMEM when it cannot be decoded for lack of memory). Decoding stops quietly at a
 * malformed field, keeping what was read; an Any that overruns the input is read up to its end.
 * Only the first Any is reported, its type_url and value together. Allocation failures leave the
 * affected outputs null. A repeated code or message replaces the earlier one. */
GRPC_API grpc_err grpc_status_details_decode(const char *base64_str, int32_t *out_code,
                                             char **out_message, char **out_type_url,
                                             uint8_t **out_detail_value, size_t *out_detail_len) {
  if (base64_str == nullptr) {
    return GRPC_ERR_INVALID;
  }

  size_t raw_len = 0;
  uint8_t *raw = nullptr;
  grpc_err derr = base64_decode(base64_str, strlen(base64_str), false, &raw, &raw_len);
  if (derr != GRPC_OK)
    return derr; /* GRPC_ERR_INVALID (not Base64) or GRPC_ERR_NOMEM */

  if (out_code)
    *out_code = 0;
  if (out_message)
    *out_message = nullptr;
  if (out_type_url)
    *out_type_url = nullptr;
  if (out_detail_value)
    *out_detail_value = nullptr;
  if (out_detail_len)
    *out_detail_len = 0;

  bool have_any = false;
  size_t seek = 0;
  while (seek < raw_len) {
    bool ok = true;
    uint64_t tag = read_varint_checked(raw, raw_len, &seek, &ok);
    uint64_t field_num = tag >> 3;
    unsigned wire_type = (unsigned)(tag & 0x07);
    if (!ok || field_num == 0)
      break;

    if (field_num == 1 && wire_type == 0) {
      uint64_t c = read_varint_checked(raw, raw_len, &seek, &ok);
      if (!ok)
        break;
      if (out_code)
        *out_code = (int32_t)c;
    } else if (field_num == 2 && wire_type == 2) {
      uint64_t mlen = read_varint_checked(raw, raw_len, &seek, &ok);
      if (!ok)
        break;
      if (mlen <= raw_len - seek && out_message) {
        char *m = grpc_malloc((size_t)mlen + 1);
        if (m) {
          memcpy(m, raw + seek, (size_t)mlen);
          m[mlen] = '\0';
          grpc_free(*out_message); /* a repeated field replaces the earlier one */
          *out_message = m;
        }
      }
      seek = skip_bytes(seek, mlen, raw_len);
    } else if (field_num == 3 && wire_type == 2) {
      uint64_t any_len = read_varint_checked(raw, raw_len, &seek, &ok);
      if (!ok)
        break;
      size_t any_end = skip_bytes(seek, any_len, raw_len);
      if (!have_any) {
        have_any = true;
        any_fields a = {0};
        if (parse_any(raw, seek, any_end, &a) == GRPC_OK) {
          /* the type_url and value of the same entry, or neither */
          if (out_type_url) {
            *out_type_url = a.url;
            a.url = nullptr;
          }
          if (out_detail_value) {
            *out_detail_value = a.value;
            a.value = nullptr;
            if (out_detail_len)
              *out_detail_len = a.value_len;
          }
          any_fields_free(&a);
        }
      }
      seek = any_end;
    } else if (!skip_field(raw, raw_len, &seek, wire_type)) {
      break;
    }
  }

  grpc_free(raw);
  return GRPC_OK;
}

/* Frees the message and every detail's type_url and value, then zeroes *details. */
GRPC_API void grpc_status_details_free(grpc_status_details *details) {
  if (details == nullptr)
    return;
  grpc_free(details->message);
  for (size_t i = 0; i < details->nb_details; i++) {
    grpc_free((void *)details->details[i].type_url);
    grpc_free((void *)details->details[i].value);
  }
  grpc_free(details->details);
  *details = (grpc_status_details){};
}

/* Strict counterpart of grpc_status_details_decode() that keeps every Any entry. Input that is
 * not a valid google.rpc.Status fails with GRPC_ERR_INVALID: a truncated tag or varint, field
 * number 0, a wire type protobuf does not allow, or a field (message, Any, unknown field) that
 * overruns the input. Unknown fields of valid wire types are skipped. A malformed field inside an
 * Any ends that Any but keeps what was parsed. On any error *out is freed and zeroed. */
GRPC_API grpc_err grpc_status_details_decode_all(const char *base64_str, grpc_status_details *out) {
  if (base64_str == nullptr || out == nullptr)
    return GRPC_ERR_INVALID;
  *out = (grpc_status_details){};

  size_t raw_len = 0;
  uint8_t *raw = nullptr;
  grpc_err derr = base64_decode(base64_str, strlen(base64_str), false, &raw, &raw_len);
  if (derr != GRPC_OK) /* GRPC_ERR_INVALID (not Base64) or GRPC_ERR_NOMEM */
    return derr;

  grpc_err err = GRPC_OK;
  size_t cap = 0;
  size_t seek = 0;
  while (seek < raw_len && err == GRPC_OK) {
    bool ok = true;
    uint64_t tag = read_varint_checked(raw, raw_len, &seek, &ok);
    uint64_t field_num = tag >> 3;
    unsigned wire_type = (unsigned)(tag & 0x07);
    if (!ok || field_num == 0) {
      err = GRPC_ERR_INVALID;
      break;
    }

    if (field_num == 1 && wire_type == 0) {
      out->code = (int32_t)read_varint_checked(raw, raw_len, &seek, &ok);
      if (!ok)
        err = GRPC_ERR_INVALID;
    } else if (field_num == 2 && wire_type == 2) {
      uint64_t mlen = read_varint_checked(raw, raw_len, &seek, &ok);
      if (!ok || mlen > raw_len - seek) {
        err = GRPC_ERR_INVALID;
        break;
      }
      grpc_free(out->message);
      out->message = grpc_malloc((size_t)mlen + 1);
      if (out->message == nullptr) {
        err = GRPC_ERR_NOMEM;
        break;
      }
      memcpy(out->message, raw + seek, (size_t)mlen);
      out->message[mlen] = '\0';
      seek += (size_t)mlen;
    } else if (field_num == 3 && wire_type == 2) {
      uint64_t any_len = read_varint_checked(raw, raw_len, &seek, &ok);
      if (!ok || any_len > raw_len - seek) {
        err = GRPC_ERR_INVALID;
        break;
      }
      size_t any_end = seek + (size_t)any_len;
      any_fields a = {0};
      err = parse_any(raw, seek, any_end, &a);
      if (err != GRPC_OK)
        break;
      seek = any_end;
      if (out->nb_details == cap) {
        size_t next_cap = cap ? cap * 2 : 4;
        grpc_status_detail_entry *arr = grpc_realloc(out->details, next_cap * sizeof(*arr));
        if (arr == nullptr) {
          any_fields_free(&a);
          err = GRPC_ERR_NOMEM;
          break;
        }
        out->details = arr;
        cap = next_cap;
      }
      out->details[out->nb_details++] = (grpc_status_detail_entry){
          .type_url = a.url,
          .value = a.value,
          .value_len = a.value_len,
      };
    } else if (!skip_field(raw, raw_len, &seek, wire_type)) {
      err = GRPC_ERR_INVALID;
    }
  }

  grpc_free(raw);
  if (err != GRPC_OK)
    grpc_status_details_free(out);
  return err;
}

/* Encodes the status as a grpc-status-details-bin trailer, adds it and finishes the call. The
 * trailer is added even if the finish then fails. */
GRPC_API grpc_err grpc_call_finish_with_details(grpc_call *call, grpc_status status,
                                                const char *message,
                                                const grpc_status_detail_entry *details,
                                                size_t nb_details) {
  if (call == nullptr || (details == nullptr && nb_details > 0))
    return GRPC_ERR_INVALID;

  size_t b64_len = 0;
  char *b64_status =
      grpc_status_details_encode((int32_t)status, message, details, nb_details, &b64_len);
  if (b64_status == nullptr)
    return GRPC_ERR_NOMEM;

  grpc_metadata trailer = {
      .key = GRPC_STATUS_DETAILS_TRAILER,
      .key_len = strlen(GRPC_STATUS_DETAILS_TRAILER),
      .value = b64_status,
      .value_len = b64_len,
  };
  grpc_err err = grpc_call_add_trailers(call, &trailer, 1);
  grpc_free(b64_status);
  if (err != GRPC_OK)
    return err;
  return grpc_call_finish(call, status, message);
}

typedef struct async_details_op {
  grpc_call *call;
  grpc_status status;
  char *message;
  char *b64_status;
  size_t b64_len;
} async_details_op;

/* Server-thread side of grpc_call_finish_with_details_async(): adds the pre-encoded trailer and,
 * if that succeeds, finishes the call; then drops the reference and frees the op. Errors are
 * ignored. */
static void run_async_details_op(void *arg) {
  async_details_op *op = arg;
  grpc_metadata trailer = {
      .key = GRPC_STATUS_DETAILS_TRAILER,
      .key_len = strlen(GRPC_STATUS_DETAILS_TRAILER),
      .value = op->b64_status,
      .value_len = op->b64_len,
  };
  if (grpc_call_add_trailers(op->call, &trailer, 1) == GRPC_OK)
    (void)grpc_call_finish(op->call, op->status, op->message);
  grpc_call_unref(op->call);
  grpc_free(op->message);
  grpc_free(op->b64_status);
  grpc_free(op);
}

/* Encodes the details and copies `message` on the caller's thread, so the caller's buffers may
 * go away on return, then takes a call reference and posts the op to the server thread. */
GRPC_API grpc_err grpc_call_finish_with_details_async(grpc_call *call, grpc_status status,
                                                      const char *message,
                                                      const grpc_status_detail_entry *details,
                                                      size_t nb_details) {
  if (call == nullptr || (details == nullptr && nb_details > 0))
    return GRPC_ERR_INVALID;

  async_details_op *op = grpc_calloc(1, sizeof(*op));
  if (op == nullptr)
    return GRPC_ERR_NOMEM;
  op->call = call;
  op->status = status;
  op->b64_status =
      grpc_status_details_encode((int32_t)status, message, details, nb_details, &op->b64_len);
  if (op->b64_status == nullptr) {
    grpc_free(op);
    return GRPC_ERR_NOMEM;
  }
  if (message != nullptr && (op->message = grpc_strdup(message)) == nullptr) {
    grpc_free(op->b64_status);
    grpc_free(op);
    return GRPC_ERR_NOMEM;
  }

  grpc_call_ref(call);
  grpc_err err = grpc_server_post(call->server, run_async_details_op, op);
  if (err != GRPC_OK) {
    grpc_call_unref(call);
    grpc_free(op->message);
    grpc_free(op->b64_status);
    grpc_free(op);
  }
  return err;
}
