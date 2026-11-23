#pragma once

/* otlp_wire.h - a non-recursive pre-scan of protobuf wire data
 *
 * protobuf-c decodes nested messages recursively and has no depth limit. The OTLP schema is
 * recursive (AnyValue -> ArrayValue/KeyValueList -> KeyValue -> AnyValue ...), so a few megabytes
 * of crafted input could nest deep enough to overflow the stack of the thread decoding it. This
 * scan walks the encoding with an explicit, bounded stack, using the message descriptors to know
 * which length-delimited fields are nested messages, and rejects input nested deeper than a limit
 * before protobuf-c sees it. It also rejects framing errors (truncated varints or lengths, fields
 * overrunning their parent, field number 0, group wire types), which keeps the decoder on input
 * that is at least well-framed. Field contents are otherwise left to protobuf-c.
 */

#include <protobuf-c/protobuf-c.h>
#include <stddef.h>
#include <stdint.h>

/** Deepest nesting otlp_wire_check() can be asked to allow. */
#define OTLP_WIRE_MAX_DEPTH_LIMIT 256u

typedef enum otlp_wire_status {
  OTLP_WIRE_OK = 0,
  /** Not a valid protobuf encoding. */
  OTLP_WIRE_MALFORMED,
  /** Messages nested deeper than allowed. */
  OTLP_WIRE_TOO_DEEP,
} otlp_wire_status;

/**
 * Scans `len` bytes of `data` encoding a `root` message. The root message is depth 1; `max_depth`
 * (at most OTLP_WIRE_MAX_DEPTH_LIMIT) bounds the nesting of message fields.
 */
otlp_wire_status otlp_wire_check(const ProtobufCMessageDescriptor *root, const uint8_t *data,
                                 size_t len, unsigned max_depth);
