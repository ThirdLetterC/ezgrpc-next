/* grpc_message.c - allocation of grpc_message objects
 *
 * A grpc_message is a single allocation: the header struct followed by its payload bytes in a
 * flexible array member. Messages are allocated with grpc_aligned_alloc so they start on a cache
 * line, and must be released with grpc_message_free (which calls grpc_free); never pass one to
 * libc free, since on MSVC the default allocator uses _aligned_malloc/_aligned_free.
 *
 * message_create is the internal constructor; besides the wire values 0/1 for the compressed
 * flag, other modules use it with internal cflag markers (e.g. 0xFE for pre-framed gRPC-Web text
 * bytes written without a length prefix, 0x80 for gRPC-Web trailer frames).
 */

#include <string.h>
#include "core.h"
#include "grpc_message.h"

/* messages start on a cache line */
enum { MESSAGE_ALIGN = 64 };
static_assert((MESSAGE_ALIGN & (MESSAGE_ALIGN - 1)) == 0, "MESSAGE_ALIGN must be a power of two");

/* Allocates a message of len payload bytes with the given compressed-flag byte, copying data
 * into it when data is non-null (otherwise the payload is left uninitialized). The compression
 * fields start as identity / default level. The size is rounded up to MESSAGE_ALIGN because
 * aligned_alloc requires a multiple of the alignment. Returns nullptr if len does not fit the
 * 32-bit wire length or on allocation failure; the caller frees with grpc_message_free. */
grpc_message *message_create(u8 cflag, const void *data, size_t len) {
  if (len > UINT32_MAX)
    return nullptr;
  /* header + payload, rounded up to the alignment: can exceed a 32-bit size_t */
  size_t size;
  if (grpc_size_add(sizeof(grpc_message), len, &size) ||
      grpc_size_add(size, MESSAGE_ALIGN - 1, &size))
    return nullptr;
  grpc_message *message = grpc_aligned_alloc(MESSAGE_ALIGN, grpc_align_up(size, MESSAGE_ALIGN));
  if (message == nullptr)
    return nullptr;
  message->len = (uint32_t)len;
  message->cflag = cflag;
  message->compression = GRPC_COMPRESS_IDENTITY;
  message->compression_level = -1;
  if (data != nullptr && len > 0)
    memcpy(message->data, data, len);
  return message;
}

/* Copies data into a new uncompressed (cflag 0) message; rejects a null data with nonzero len. */
GRPC_API grpc_message *grpc_message_new(const void *data, size_t len) {
  if (data == nullptr && len > 0)
    return nullptr;
  return message_create(0, data, len);
}

/* Allocates an uncompressed message with len uninitialized payload bytes. */
GRPC_API grpc_message *grpc_message_alloc(size_t len) {
  return message_create(0, nullptr, len);
}

/* Frees a message from message_create; nullptr is passed straight to grpc_free. */
GRPC_API void grpc_message_free(grpc_message *message) {
  grpc_free(message);
}
