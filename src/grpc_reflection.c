/* grpc_reflection.c - the gRPC server reflection service (v1 and v1alpha)
 *
 * Serves ServerReflectionInfo on both the v1 and v1alpha paths with the same stream handler.
 * Each request message gets one response: list_services answers with the services of the
 * enabled routes (plus the v1alpha reflection service itself), file_by_filename and
 * file_containing_symbol answer with a FileDescriptorProto registered through
 * grpc_reflection_add_file_descriptor(), or a NOT_FOUND error_response. Other request kinds are
 * not supported. The stream is finished when the client half-closes.
 *
 * Protobuf is parsed and built by hand with single-byte tags. Descriptors are copied into a
 * per-server reflection_state owned by the server and freed with it; the state is not
 * synchronized, so register descriptors before the server runs. Symbols and service names are
 * limited to MAX_SYMBOL_LEN - 1 bytes.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "internal/grpc_threads.h"
#include "core.h"

enum { MAX_SYMBOL_LEN = 128 };

typedef struct descriptor_entry {
  char symbol[MAX_SYMBOL_LEN];
  uint8_t *data;
  size_t len;
} descriptor_entry;

/* per server, used on the server thread */
struct reflection_state {
  descriptor_entry *entries;
  size_t nb_entries;
};

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

/* Returns the server's reflection state, creating it on first use; nullptr if out of memory. */
static struct reflection_state *reflection_state_get(grpc_server *server) {
  if (server->reflection == nullptr)
    server->reflection = grpc_calloc(1, sizeof(*server->reflection));
  return server->reflection;
}

/* Frees the reflection state and its descriptor copies when the server is freed. */
void reflection_free(grpc_server *server) {
  struct reflection_state *state = server->reflection;
  if (state == nullptr)
    return;
  for (size_t i = 0; i < state->nb_entries; i++)
    grpc_free(state->entries[i].data);
  grpc_free(state->entries);
  grpc_free(state);
  server->reflection = nullptr;
}

/* Copies `descriptor`; the key is truncated to MAX_SYMBOL_LEN - 1 bytes. Does not require
 * grpc_reflection_enable() first, and duplicate keys are kept (the first match wins). */
GRPC_API grpc_err grpc_reflection_add_file_descriptor(grpc_server *server,
                                                      const char *symbol_or_filename,
                                                      const uint8_t *descriptor, size_t len) {
  if (server == nullptr || symbol_or_filename == nullptr || descriptor == nullptr || len == 0) {
    return GRPC_ERR_INVALID;
  }
  struct reflection_state *state = reflection_state_get(server);
  if (state == nullptr)
    return GRPC_ERR_NOMEM;

  descriptor_entry *entries =
      grpc_realloc(state->entries, (state->nb_entries + 1) * sizeof(*entries));
  if (entries == nullptr)
    return GRPC_ERR_NOMEM;
  state->entries = entries;

  uint8_t *copy = grpc_malloc(len);
  if (copy == nullptr)
    return GRPC_ERR_NOMEM;
  memcpy(copy, descriptor, len);

  descriptor_entry *e = &state->entries[state->nb_entries++];
  strncpy(e->symbol, symbol_or_filename, sizeof(e->symbol) - 1);
  e->symbol[sizeof(e->symbol) - 1] = '\0';
  e->data = copy;
  e->len = len;
  return GRPC_OK;
}

/* Fills `services` with the distinct service names ("pkg.Service" of "/pkg.Service/Method") of
 * the enabled routes, skipping names of MAX_SYMBOL_LEN bytes or more, and adds the v1alpha
 * reflection service if missing. Stops at `max_services`. Returns the count. */
static size_t collect_services(grpc_server *server, char services[][MAX_SYMBOL_LEN],
                               size_t max_services) {
  size_t count = 0;

  for (size_t i = 0; i < server->nb_routes && count < max_services; i++) {
    if (server->routes[i]->disabled)
      continue;
    const char *p = server->routes[i]->path;
    if (p == nullptr || *p != '/')
      continue;

    const char *service_start = p + 1;
    const char *service_end = strchr(service_start, '/');
    if (service_end == nullptr)
      continue;

    size_t s_len = (size_t)(service_end - service_start);
    if (s_len == 0 || s_len >= MAX_SYMBOL_LEN)
      continue;

    char svc[MAX_SYMBOL_LEN];
    memcpy(svc, service_start, s_len);
    svc[s_len] = '\0';

    // Check if already in list
    bool dup = false;
    for (size_t k = 0; k < count; k++) {
      if (strcmp(services[k], svc) == 0) {
        dup = true;
        break;
      }
    }
    if (!dup) {
      strcpy(services[count++], svc);
    }
  }

  // Always ensure reflection service itself is advertised
  const char *refl_svc = "grpc.reflection.v1alpha.ServerReflection";
  bool has_refl = false;
  for (size_t k = 0; k < count; k++) {
    if (strcmp(services[k], refl_svc) == 0) {
      has_refl = true;
      break;
    }
  }
  if (!has_refl && count < max_services) {
    strcpy(services[count++], refl_svc);
  }

  return count;
}

/* Serializes a ServerReflectionResponse with original_request (field 2, when non-empty) and
 * list_services_response (field 6) naming `services`. Returns a grpc_malloc()'d buffer and its
 * length in *out_resp_len, or nullptr on allocation failure. */
static uint8_t *build_list_services_response(const char services[][MAX_SYMBOL_LEN],
                                             size_t nb_services, const uint8_t *orig_req,
                                             size_t orig_req_len, size_t *out_resp_len) {
  // Calculate size of ListServiceResponse
  size_t list_resp_inner = 0;
  for (size_t i = 0; i < nb_services; i++) {
    size_t nlen = strlen(services[i]);
    // ServiceResponse: field 1 (string name), tag 0x0A, varint(nlen), nlen
    size_t s_size = 1 + varint_size(nlen) + nlen;
    // ListServiceResponse.service: tag 0x0A, varint(s_size), s_size
    list_resp_inner += 1 + varint_size(s_size) + s_size;
  }

  // ServerReflectionResponse:
  // Field 2: original_request (tag 0x12, varint orig_req_len, orig_req)
  size_t orig_req_field_size = 0;
  if (orig_req_len > 0) {
    orig_req_field_size = 1 + varint_size(orig_req_len) + orig_req_len;
  }
  // Field 6: list_services_response (tag (6 << 3) | 2 = 0x32, varint list_resp_inner, inner)
  size_t list_field_size = 1 + varint_size(list_resp_inner) + list_resp_inner;

  size_t total = orig_req_field_size + list_field_size;
  uint8_t *buf = grpc_malloc(total);
  if (buf == nullptr)
    return nullptr;

  size_t off = 0;
  if (orig_req_len > 0) {
    buf[off++] = (2 << 3) | 2; // 0x12
    off += write_varint(buf + off, orig_req_len);
    memcpy(buf + off, orig_req, orig_req_len);
    off += orig_req_len;
  }

  buf[off++] = (6 << 3) | 2; // 0x32
  off += write_varint(buf + off, list_resp_inner);

  for (size_t i = 0; i < nb_services; i++) {
    size_t nlen = strlen(services[i]);
    size_t s_size = 1 + varint_size(nlen) + nlen;

    buf[off++] = (1 << 3) | 2; // 0x0A
    off += write_varint(buf + off, s_size);

    buf[off++] = (1 << 3) | 2; // 0x0A
    off += write_varint(buf + off, nlen);
    memcpy(buf + off, services[i], nlen);
    off += nlen;
  }

  *out_resp_len = off;
  return buf;
}

/* True when a registered key answers a query: the query equals the key, or names something
 * inside it ("pkg.Service" answers "pkg.Service.Method"). An empty query matches nothing. */
static bool key_matches(const char *key, const char *query) {
  size_t klen = strlen(key);
  if (query[0] == '\0' || klen == 0 || strncmp(key, query, klen) != 0)
    return false;
  return query[klen] == '\0' || query[klen] == '.';
}

/* Builds a ServerReflectionResponse with original_request (field 2) echoing the request and
 * error_response (field 7) {error_code, error_message}. Returns a grpc_malloc buffer and its
 * length, or nullptr when out of memory. */
static uint8_t *build_error_response(uint32_t code, const char *msg, const uint8_t *req_data,
                                     size_t req_len, size_t *out_len) {
  size_t elen = strlen(msg);
  size_t err_inner = 1 + varint_size(code) + 1 + varint_size(elen) + elen;
  size_t orig_req_size = req_len > 0 ? 1 + varint_size(req_len) + req_len : 0;
  size_t total = orig_req_size + 1 + varint_size(err_inner) + err_inner;
  uint8_t *out = grpc_malloc(total);
  if (out == nullptr)
    return nullptr;
  size_t off = 0;
  if (req_len > 0) {
    out[off++] = (2 << 3) | 2; /* original_request */
    off += write_varint(out + off, req_len);
    memcpy(out + off, req_data, req_len);
    off += req_len;
  }
  out[off++] = (7 << 3) | 2; /* error_response */
  off += write_varint(out + off, err_inner);
  out[off++] = (1 << 3) | 0; /* error_code */
  off += write_varint(out + off, code);
  out[off++] = (2 << 3) | 2; /* error_message */
  off += write_varint(out + off, elen);
  memcpy(out + off, msg, elen);
  off += elen;
  *out_len = off;
  return out;
}

/* on_message handler: answers one ServerReflectionRequest. Scans the request for
 * list_services (field 7) and file_by_filename / file_containing_symbol (fields 3 and 4); a
 * request of at most two bytes with no file query is treated as list_services. A file query is
 * matched against the registered keys by substring in either direction, so the first key that
 * contains the query, or is contained in it, wins. Requests matching nothing get no response.
 * Every response echoes the request as original_request. `userdata` is the server. */
static void reflection_on_message(grpc_call *call, const grpc_message *in_msg, void *userdata) {
  grpc_server *server = userdata;
  struct reflection_state *state = server->reflection;

  // Parse request
  const uint8_t *req_data = in_msg->data;
  size_t req_len = in_msg->len;

  bool is_list_services = false;
  char queried_symbol[MAX_SYMBOL_LEN] = {};
  bool is_file_query = false;
  /* file_containing_extension (5) / all_extension_numbers_of_type (6): valid, not supported */
  bool is_unsupported = false;

  size_t seek = 0;
  while (seek < req_len) {
    /* the tag is a varint: field numbers from 16 up take more than one byte */
    uint64_t tag = read_varint(req_data, req_len, &seek);
    uint64_t field_num = tag >> 3;
    unsigned wire_type = (unsigned)(tag & 0x07);

    if (field_num == 7 && wire_type == 2) {
      // list_services
      uint64_t slen = read_varint(req_data, req_len, &seek);
      seek = skip_bytes(seek, slen, req_len);
      is_list_services = true;
    } else if ((field_num == 3 || field_num == 4) && wire_type == 2) {
      // file_by_filename (3) or file_containing_symbol (4)
      uint64_t slen = read_varint(req_data, req_len, &seek);
      if (slen <= req_len - seek) {
        size_t cpy = slen < MAX_SYMBOL_LEN - 1 ? (size_t)slen : MAX_SYMBOL_LEN - 1;
        memcpy(queried_symbol, req_data + seek, cpy);
        queried_symbol[cpy] = '\0';
      }
      seek = skip_bytes(seek, slen, req_len);
      is_file_query = true;
    } else if (field_num == 5 || field_num == 6) {
      is_unsupported = true;
      if (wire_type == 2)
        seek = skip_bytes(seek, read_varint(req_data, req_len, &seek), req_len);
      else
        break;
    } else if (wire_type == 0) {
      read_varint(req_data, req_len, &seek);
    } else if (wire_type == 1) { /* fixed64 */
      seek = skip_bytes(seek, 8, req_len);
    } else if (wire_type == 5) { /* fixed32 */
      seek = skip_bytes(seek, 4, req_len);
    } else if (wire_type == 2) {
      uint64_t skip = read_varint(req_data, req_len, &seek);
      seek = skip_bytes(seek, skip, req_len);
    } else {
      break;
    }
  }

  uint8_t *resp_bytes = nullptr;
  size_t resp_len = 0;

  if (is_list_services || (!is_file_query && req_len <= 2)) {
    // List services request
    char services[64][MAX_SYMBOL_LEN];
    size_t nb_services = collect_services(server, services, 64);
    resp_bytes = build_list_services_response(services, nb_services, req_data, req_len, &resp_len);
  } else if (is_file_query) {
    const descriptor_entry *found = nullptr;
    for (size_t i = 0; state != nullptr && i < state->nb_entries; i++) {
      if (key_matches(state->entries[i].symbol, queried_symbol)) {
        found = &state->entries[i];
        break;
      }
    }

    if (found != nullptr) {
      // FileDescriptorResponse: field 1 (repeated bytes file_descriptor_proto = 1)
      size_t f_size = 1 + varint_size(found->len) + found->len;
      // ServerReflectionResponse.file_descriptor_response: tag 0x22, varint(f_size)
      size_t orig_req_size = req_len > 0 ? 1 + varint_size(req_len) + req_len : 0;
      size_t total = orig_req_size + 1 + varint_size(f_size) + f_size;
      resp_bytes = grpc_malloc(total);
      if (resp_bytes != nullptr) {
        size_t off = 0;
        if (req_len > 0) {
          resp_bytes[off++] = 0x12;
          off += write_varint(resp_bytes + off, req_len);
          memcpy(resp_bytes + off, req_data, req_len);
          off += req_len;
        }
        resp_bytes[off++] = (4 << 3) | 2; // 0x22
        off += write_varint(resp_bytes + off, f_size);

        resp_bytes[off++] = (1 << 3) | 2; // 0x0A
        off += write_varint(resp_bytes + off, found->len);
        memcpy(resp_bytes + off, found->data, found->len);
        off += found->len;
        resp_len = off;
      }
    } else {
      resp_bytes = build_error_response(5 /* NOT_FOUND */, "Symbol or file not found", req_data,
                                        req_len, &resp_len);
    }
  } else {
    /* a request without a supported message_request still gets an answer */
    resp_bytes = build_error_response(12 /* UNIMPLEMENTED */,
                                      is_unsupported ? "Extension queries are not supported"
                                                     : "Unsupported reflection request",
                                      req_data, req_len, &resp_len);
  }

  if (resp_bytes != nullptr) {
    (void)grpc_call_send_data(call, resp_bytes, resp_len);
    grpc_free(resp_bytes);
  }
}

/* on_end handler: the client closed its side of the stream, so end the call with OK. */
static void reflection_on_end(grpc_call *call, [[maybe_unused]] void *userdata) {
  (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

/* Creates the reflection state and registers the stream handler on the v1alpha and v1 paths,
 * with the server as userdata. If the v1 registration fails, the v1alpha one stays. */
GRPC_API grpc_err grpc_reflection_enable(grpc_server *server) {
  if (server == nullptr)
    return GRPC_ERR_INVALID;
  if (reflection_state_get(server) == nullptr)
    return GRPC_ERR_NOMEM;
  const grpc_stream_handler handler = {
      .on_message = reflection_on_message,
      .on_end = reflection_on_end,
  };
  grpc_err err = grpc_server_handle_stream(server, GRPC_REFLECTION_V1ALPHA_PATH, &handler, server);
  if (err != GRPC_OK)
    return err;
  err = grpc_server_handle_stream(server, GRPC_REFLECTION_V1_PATH, &handler, server);
  if (err != GRPC_OK) {
    /* all or nothing, as for health and channelz: a retry must not find v1alpha half-registered */
    (void)grpc_server_unregister(server, GRPC_REFLECTION_V1ALPHA_PATH);
  }
  return err;
}
