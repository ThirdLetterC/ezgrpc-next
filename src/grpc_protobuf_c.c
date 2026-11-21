/* grpc_protobuf_c.c - protobuf-c integration: (de)serialize messages and send them on calls.
 *
 * Packs protobuf-c messages into grpc_message buffers and unpacks received messages. Every
 * allocation protobuf-c makes for unpacked messages goes through the grpc allocator, via a
 * ProtobufCAllocator, rather than protobuf-c's libc default. This keeps one allocator family for
 * the whole library: on MSVC grpc_malloc() is _aligned_malloc(), so its memory must be released
 * with grpc_free() and never with libc free(), and vice versa.
 */
#include "grpc_protobuf_c.h"
#include "grpc_mem.h"

/* ProtobufCAllocator.alloc: allocates `size` bytes with grpc_malloc(). */
static void *pb_alloc([[maybe_unused]] void *allocator_data, size_t size) {
  return grpc_malloc(size);
}

/* ProtobufCAllocator.free: releases memory from pb_alloc() with grpc_free(). */
static void pb_free([[maybe_unused]] void *allocator_data, void *pointer) {
  grpc_free(pointer);
}

/* The allocator handed to protobuf-c; stateless, so one shared instance is enough. */
static ProtobufCAllocator g_grpc_pb_allocator = {
    .alloc = pb_alloc,
    .free = pb_free,
    .allocator_data = nullptr,
};

/* Returns the shared grpc-backed allocator, for callers that use protobuf-c directly. */
GRPC_API ProtobufCAllocator *grpc_pb_get_allocator(void) {
  return &g_grpc_pb_allocator;
}

/* Deserializes `message` with the grpc allocator. Returns nullptr on nullptr arguments or a parse
 * error; the result must be freed with grpc_pb_free_unpacked(). */
GRPC_API ProtobufCMessage *grpc_pb_unpack(const ProtobufCMessageDescriptor *descriptor,
                                          const grpc_message *message) {
  if (descriptor == nullptr || message == nullptr) {
    return nullptr;
  }
  return protobuf_c_message_unpack(descriptor, &g_grpc_pb_allocator, message->len, message->data);
}

/* Serializes `message` into a new grpc_message of exactly its packed size. Returns nullptr on a
 * nullptr argument, allocation failure, or if protobuf-c writes a different size than it
 * computed. */
GRPC_API grpc_message *grpc_pb_pack(const ProtobufCMessage *message) {
  if (message == nullptr) {
    return nullptr;
  }

  size_t packed_size = protobuf_c_message_get_packed_size(message);
  grpc_message *msg = grpc_message_alloc(packed_size);
  if (msg == nullptr) {
    return nullptr;
  }

  size_t written = protobuf_c_message_pack(message, msg->data);
  if (written != packed_size) {
    grpc_message_free(msg);
    return nullptr;
  }

  return msg;
}

/* Frees a message from grpc_pb_unpack() with the same allocator; nullptr is ignored. */
GRPC_API void grpc_pb_free_unpacked(ProtobufCMessage *message) {
  if (message != nullptr) {
    protobuf_c_message_free_unpacked(message, &g_grpc_pb_allocator);
  }
}

/* Packs `message` and sends it with grpc_call_send(), which takes ownership of the packed
 * buffer. Returns GRPC_ERR_INVALID on nullptr arguments and GRPC_ERR_NOMEM if packing fails. */
GRPC_API grpc_err grpc_call_send_pb(grpc_call *call, const ProtobufCMessage *message) {
  if (call == nullptr || message == nullptr) {
    return GRPC_ERR_INVALID;
  }
  grpc_message *msg = grpc_pb_pack(message);
  if (msg == nullptr) {
    return GRPC_ERR_NOMEM;
  }
  return grpc_call_send(call, msg);
}

/* Sends `message`, then finishes the call with GRPC_STATUS_OK. If sending fails, the call is not
 * finished and the error is returned. */
GRPC_API grpc_err grpc_call_reply_pb(grpc_call *call, const ProtobufCMessage *message) {
  grpc_err err = grpc_call_send_pb(call, message);
  if (err != GRPC_OK) {
    return err;
  }
  return grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}
