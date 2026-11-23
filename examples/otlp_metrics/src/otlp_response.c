#include "otlp_response.h"

#include <string.h>

#include "otlp_proto.h"

grpc_message *otlp_response_new(uint64_t rejected, const char *error_message) {
  bool has_message = error_message != nullptr && error_message[0] != '\0';
  if (rejected == 0 && !has_message)
    return grpc_message_new(nullptr, 0); /* full success: partial_success stays unset */

  char text[OTLP_ERROR_MESSAGE_MAX + 1] = "";
  if (has_message) {
    size_t len = strnlen(error_message, OTLP_ERROR_MESSAGE_MAX);
    memcpy(text, error_message, len);
    text[len] = '\0';
  }
  /* the generated init functions, not the __INIT macros: those cast away const in this file */
  otlp_pb_partial_success partial;
  opentelemetry__proto__collector__metrics__v1__export_metrics_partial_success__init(&partial);
  partial.rejected_data_points = rejected > INT64_MAX ? INT64_MAX : (int64_t)rejected;
  partial.error_message = text;
  otlp_pb_response response;
  opentelemetry__proto__collector__metrics__v1__export_metrics_service_response__init(&response);
  response.partial_success = &partial;

  size_t len = protobuf_c_message_get_packed_size(&response.base);
  grpc_message *msg = grpc_message_alloc(len);
  if (msg == nullptr)
    return nullptr;
  [[maybe_unused]] size_t packed = protobuf_c_message_pack(&response.base, msg->data);
  return msg;
}

static size_t put_varint(uint8_t *out, uint64_t value) {
  size_t n = 0;
  do {
    uint8_t byte = value & 0x7f;
    value >>= 7;
    out[n++] = value != 0 ? (uint8_t)(byte | 0x80) : byte;
  } while (value != 0);
  return n;
}

/* google.rpc.RetryInfo { google.protobuf.Duration retry_delay = 1; }
 * google.protobuf.Duration { int64 seconds = 1; int32 nanos = 2; }
 * Two fixed, tiny messages from googleapis, so they are encoded by hand rather than generating
 * bindings for all of google/rpc/error_details.proto. */
size_t otlp_retry_info_encode(uint8_t *out, uint32_t delay_ms) {
  uint8_t duration[24];
  size_t dlen = 0;
  uint64_t seconds = delay_ms / 1000u;
  uint64_t nanos = (uint64_t)(delay_ms % 1000u) * 1000000u;
  if (seconds != 0) {
    duration[dlen++] = 0x08; /* field 1, varint */
    dlen += put_varint(duration + dlen, seconds);
  }
  if (nanos != 0) {
    duration[dlen++] = 0x10; /* field 2, varint */
    dlen += put_varint(duration + dlen, nanos);
  }
  size_t n = 0;
  out[n++] = 0x0a; /* field 1, length-delimited */
  n += put_varint(out + n, dlen);
  memcpy(out + n, duration, dlen);
  return n + dlen;
}
