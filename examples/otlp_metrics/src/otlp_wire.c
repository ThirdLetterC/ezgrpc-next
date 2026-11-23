#include "otlp_wire.h"

#include <stdbool.h>

enum {
  WIRE_VARINT = 0,
  WIRE_FIXED64 = 1,
  WIRE_LEN = 2,
  WIRE_FIXED32 = 5,
};

/* Reads a varint of at most 10 bytes at data[*pos] (before end). */
static bool read_varint(const uint8_t *data, size_t end, size_t *pos, uint64_t *out) {
  uint64_t value = 0;
  for (unsigned i = 0; i < 10; i++) {
    if (*pos >= end)
      return false;
    uint8_t byte = data[(*pos)++];
    if (i == 9 && byte > 1)
      return false; /* more than 64 bits */
    value |= (uint64_t)(byte & 0x7f) << (7 * i);
    if ((byte & 0x80) == 0) {
      *out = value;
      return true;
    }
  }
  return false;
}

typedef struct frame {
  const ProtobufCMessageDescriptor *desc;
  size_t end;
} frame;

otlp_wire_status otlp_wire_check(const ProtobufCMessageDescriptor *root, const uint8_t *data,
                                 size_t len, unsigned max_depth) {
  if (max_depth == 0 || max_depth > OTLP_WIRE_MAX_DEPTH_LIMIT)
    max_depth = OTLP_WIRE_MAX_DEPTH_LIMIT;
  frame stack[OTLP_WIRE_MAX_DEPTH_LIMIT];
  unsigned depth = 1;
  stack[0] = (frame){root, len};
  size_t pos = 0;

  while (depth > 0) {
    frame *top = &stack[depth - 1];
    if (pos == top->end) {
      depth--;
      continue;
    }
    uint64_t tag;
    if (!read_varint(data, top->end, &pos, &tag))
      return OTLP_WIRE_MALFORMED;
    uint64_t number = tag >> 3;
    unsigned wire_type = (unsigned)(tag & 7);
    if (number == 0 || number > 536870911u) /* 2^29 - 1 */
      return OTLP_WIRE_MALFORMED;
    uint64_t skip;
    switch (wire_type) {
    case WIRE_VARINT:
      if (!read_varint(data, top->end, &pos, &skip))
        return OTLP_WIRE_MALFORMED;
      continue;
    case WIRE_FIXED64:
      skip = 8;
      break;
    case WIRE_FIXED32:
      skip = 4;
      break;
    case WIRE_LEN: {
      if (!read_varint(data, top->end, &pos, &skip))
        return OTLP_WIRE_MALFORMED;
      if (skip > top->end - pos)
        return OTLP_WIRE_MALFORMED;
      const ProtobufCFieldDescriptor *field =
          top->desc != nullptr
              ? protobuf_c_message_descriptor_get_field(top->desc, (unsigned)number)
              : nullptr;
      if (field != nullptr && field->type == PROTOBUF_C_TYPE_MESSAGE) {
        if (depth >= max_depth)
          return OTLP_WIRE_TOO_DEEP;
        stack[depth++] = (frame){field->descriptor, pos + (size_t)skip};
        continue;
      }
      break;
    }
    default: /* groups (3, 4) are not used by proto3; 6 and 7 do not exist */
      return OTLP_WIRE_MALFORMED;
    }
    if (skip > top->end - pos)
      return OTLP_WIRE_MALFORMED;
    pos += (size_t)skip;
  }
  return pos == len ? OTLP_WIRE_OK : OTLP_WIRE_MALFORMED;
}
