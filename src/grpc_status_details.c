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
    val |= (uint64_t)(b & 0x7F) << shift;
    if (!(b & 0x80))
      break;
    shift += 7;
  }
  return val;
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

/* Lenient decoder: returns GRPC_ERR_INVALID only when the input is null or base64 decoding
 * fails.
 * Out-of-bounds fields are skipped and allocation failures leave the output null, both silently.
 * If a field repeats, the last one wins and earlier allocations leak. */
GRPC_API grpc_err grpc_status_details_decode(const char *base64_str, int32_t *out_code,
                                             char **out_message, char **out_type_url,
                                             uint8_t **out_detail_value, size_t *out_detail_len) {
  if (base64_str == nullptr) {
    return GRPC_ERR_INVALID;
  }

  size_t raw_len = 0;
  uint8_t *raw = grpc_metadata_decode_bin(base64_str, strlen(base64_str), &raw_len);
  if (raw == nullptr) {
    return GRPC_ERR_INVALID;
  }

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

  size_t seek = 0;
  while (seek < raw_len) {
    uint8_t tag = raw[seek++];
    uint8_t field_num = tag >> 3;
    uint8_t wire_type = tag & 0x07;

    if (field_num == 1 && wire_type == 0) {
      // code
      uint64_t c = read_varint(raw, raw_len, &seek);
      if (out_code)
        *out_code = (int32_t)c;
    } else if (field_num == 2 && wire_type == 2) {
      // message
      size_t mlen = (size_t)read_varint(raw, raw_len, &seek);
      if (seek + mlen <= raw_len && out_message) {
        char *m = grpc_malloc(mlen + 1);
        if (m) {
          memcpy(m, raw + seek, mlen);
          m[mlen] = '\0';
          *out_message = m;
        }
      }
      seek += mlen;
    } else if (field_num == 3 && wire_type == 2) {
      // Any
      size_t any_len = (size_t)read_varint(raw, raw_len, &seek);
      size_t any_end = seek + any_len;
      while (seek < any_end && seek < raw_len) {
        uint8_t any_tag = raw[seek++];
        uint8_t any_field = any_tag >> 3;
        uint8_t any_wire = any_tag & 0x07;

        if (any_field == 1 && any_wire == 2) {
          size_t ulen = (size_t)read_varint(raw, raw_len, &seek);
          if (seek + ulen <= any_end && out_type_url) {
            char *u = grpc_malloc(ulen + 1);
            if (u) {
              memcpy(u, raw + seek, ulen);
              u[ulen] = '\0';
              *out_type_url = u;
            }
          }
          seek += ulen;
        } else if (any_field == 2 && any_wire == 2) {
          size_t vlen = (size_t)read_varint(raw, raw_len, &seek);
          if (seek + vlen <= any_end && out_detail_value) {
            uint8_t *v = grpc_malloc(vlen);
            if (v) {
              memcpy(v, raw + seek, vlen);
              *out_detail_value = v;
              if (out_detail_len)
                *out_detail_len = vlen;
            }
          }
          seek += vlen;
        } else if (any_wire == 0) {
          read_varint(raw, raw_len, &seek);
        } else if (any_wire == 2) {
          size_t sk = (size_t)read_varint(raw, raw_len, &seek);
          seek += sk;
        }
      }
    } else if (wire_type == 0) {
      read_varint(raw, raw_len, &seek);
    } else if (wire_type == 2) {
      size_t sk = (size_t)read_varint(raw, raw_len, &seek);
      seek += sk;
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

/* Strict counterpart of grpc_status_details_decode() that keeps every Any entry. A message or
 * Any that overruns the input fails with GRPC_ERR_INVALID; a malformed field inside an Any ends
 * that Any but keeps what was parsed. On any error *out is freed and zeroed. */
GRPC_API grpc_err grpc_status_details_decode_all(const char *base64_str, grpc_status_details *out) {
  if (base64_str == nullptr || out == nullptr)
    return GRPC_ERR_INVALID;
  *out = (grpc_status_details){};

  size_t raw_len = 0;
  uint8_t *raw = grpc_metadata_decode_bin(base64_str, strlen(base64_str), &raw_len);
  if (raw == nullptr)
    return GRPC_ERR_INVALID;

  size_t cap = 0;
  size_t seek = 0;
  while (seek < raw_len) {
    uint8_t tag = raw[seek++];
    uint8_t field_num = tag >> 3;
    uint8_t wire_type = tag & 0x07;

    if (field_num == 1 && wire_type == 0) {
      out->code = (int32_t)read_varint(raw, raw_len, &seek);
    } else if (field_num == 2 && wire_type == 2) {
      size_t mlen = (size_t)read_varint(raw, raw_len, &seek);
      if (seek + mlen > raw_len) {
        grpc_free(raw);
        grpc_status_details_free(out);
        return GRPC_ERR_INVALID;
      }
      grpc_free(out->message);
      out->message = grpc_malloc(mlen + 1);
      if (out->message == nullptr) {
        grpc_free(raw);
        grpc_status_details_free(out);
        return GRPC_ERR_NOMEM;
      }
      memcpy(out->message, raw + seek, mlen);
      out->message[mlen] = '\0';
      seek += mlen;
    } else if (field_num == 3 && wire_type == 2) {
      size_t any_len = (size_t)read_varint(raw, raw_len, &seek);
      if (seek + any_len > raw_len) {
        grpc_free(raw);
        grpc_status_details_free(out);
        return GRPC_ERR_INVALID;
      }
      size_t any_end = seek + any_len;
      char *url = nullptr;
      uint8_t *val = nullptr;
      size_t vlen = 0;
      while (seek < any_end) {
        uint8_t any_tag = raw[seek++];
        uint8_t any_field = any_tag >> 3;
        uint8_t any_wire = any_tag & 0x07;
        if (any_field == 1 && any_wire == 2) {
          size_t ulen = (size_t)read_varint(raw, any_end, &seek);
          if (seek + ulen > any_end)
            break;
          grpc_free(url);
          url = grpc_malloc(ulen + 1);
          if (url == nullptr) {
            grpc_free(val);
            grpc_free(raw);
            grpc_status_details_free(out);
            return GRPC_ERR_NOMEM;
          }
          memcpy(url, raw + seek, ulen);
          url[ulen] = '\0';
          seek += ulen;
        } else if (any_field == 2 && any_wire == 2) {
          size_t len = (size_t)read_varint(raw, any_end, &seek);
          if (seek + len > any_end)
            break;
          grpc_free(val);
          val = len > 0 ? grpc_malloc(len) : nullptr;
          if (len > 0 && val == nullptr) {
            grpc_free(url);
            grpc_free(raw);
            grpc_status_details_free(out);
            return GRPC_ERR_NOMEM;
          }
          if (len > 0)
            memcpy(val, raw + seek, len);
          vlen = len;
          seek += len;
        } else if (any_wire == 0) {
          read_varint(raw, any_end, &seek);
        } else if (any_wire == 2) {
          size_t sk = (size_t)read_varint(raw, any_end, &seek);
          seek = seek + sk <= any_end ? seek + sk : any_end;
        } else {
          break;
        }
      }
      seek = any_end;
      if (out->nb_details == cap) {
        size_t next_cap = cap ? cap * 2 : 4;
        grpc_status_detail_entry *arr = grpc_realloc(out->details, next_cap * sizeof(*arr));
        if (arr == nullptr) {
          grpc_free(url);
          grpc_free(val);
          grpc_free(raw);
          grpc_status_details_free(out);
          return GRPC_ERR_NOMEM;
        }
        out->details = arr;
        cap = next_cap;
      }
      out->details[out->nb_details++] = (grpc_status_detail_entry){
          .type_url = url,
          .value = val,
          .value_len = vlen,
      };
    } else if (wire_type == 0) {
      read_varint(raw, raw_len, &seek);
    } else if (wire_type == 2) {
      size_t sk = (size_t)read_varint(raw, raw_len, &seek);
      seek = seek + sk <= raw_len ? seek + sk : raw_len;
    } else {
      break;
    }
  }

  grpc_free(raw);
  return GRPC_OK;
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
