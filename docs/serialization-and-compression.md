# Serialization & Compression

## Protobuf-C (`include/grpc_protobuf_c.h`)

Enabled automatically when `protobuf-c` and `protoc-c` are installed (`-DGRPC_PROTOBUF_C=AUTO|ON|OFF`). All unpacked structs use the active `ezGRPC-next` allocator.

```c
#include "grpc.h"
#include "helloworld.pb-c.h"

static void say_hello(grpc_call *call, const grpc_message *req_msg, void *userdata) {
  (void)userdata;
  Helloworld__HelloRequest *req = GRPC_PB_UNPACK(
      Helloworld__HelloRequest, &helloworld__hello_request__descriptor, req_msg);
  if (req == nullptr) {
    (void)grpc_call_finish(call, GRPC_STATUS_INVALID_ARGUMENT, "invalid protobuf");
    return;
  }

  Helloworld__HelloReply reply = HELLOWORLD__HELLO_REPLY__INIT;
  reply.message = "Hello, world!";
  /* Packs, sends, and finishes with GRPC_STATUS_OK (use grpc_call_send_pb for streaming) */
  (void)grpc_call_reply_pb(call, (const ProtobufCMessage *)&reply);
  GRPC_PB_FREE_UNPACKED(req);
}
```

---

## FlatBuffers (`include/grpc_flatcc.h`)

Enabled when [flatcc](https://github.com/dvidelabs/flatcc) is installed (build with `scripts/build-flatcc.sh` and pass `-DGRPC_FLATCC_PREFIX=<dir>`).

Message payloads (`grpc_message.data`) are aligned like `malloc()`, so verified buffers are read zero-copy in place:

```c
#include "grpc.h"
#include "greeter_builder.h"
#include "greeter_verifier.h"

static void say_hello_fb(grpc_call *call, const grpc_message *req_msg, void *userdata) {
  flatcc_builder_t *builder = userdata; /* initialized once via grpc_fb_builder_init() */

  greeter_HelloRequest_table_t req = GRPC_FB_UNPACK(greeter_HelloRequest, req_msg);
  if (req == nullptr) {
    (void)grpc_call_finish(call, GRPC_STATUS_INVALID_ARGUMENT, "invalid flatbuffer");
    return;
  }

  const char *name = greeter_HelloRequest_name(req);
  greeter_HelloReply_start_as_root(builder);
  greeter_HelloReply_message_create_str(builder, name != nullptr ? name : "World");
  greeter_HelloReply_end_as_root(builder);

  /* Sends, resets builder, and finishes with GRPC_STATUS_OK (use grpc_call_send_fb for streaming) */
  (void)grpc_call_reply_fb(call, builder);
}
```

---

## Compression (`include/grpc_compression.h`)

Supported codecs:
- `GRPC_COMPRESS_GZIP` and `GRPC_COMPRESS_DEFLATE` (via `zlib`, levels `0`–`9` or `GRPC_COMPRESSION_LEVEL_DEFAULT`)
- `GRPC_COMPRESS_ZSTD` (via `libzstd`, any supported Zstandard level or `GRPC_COMPRESSION_LEVEL_DEFAULT`)
- `GRPC_COMPRESS_SNAPPY` (via `libsnappy`, requires `GRPC_COMPRESSION_LEVEL_DEFAULT`)

### Automatic Response Compression

Configure a server-wide default in `grpc_config.default_compression` or override per call with `grpc_call_set_compression()`. Both automatically negotiate against the client's `grpc-accept-encoding` header and fall back to uncompressed if the client does not advertise the codec:

```c
/* Server-wide default */
grpc_server *server = grpc_server_new(&(grpc_config){
    .port = 50051,
    .default_compression = GRPC_COMPRESS_GZIP,
});

/* Per-call override (must be called before the first send) */
(void)grpc_call_set_compression(call, GRPC_COMPRESS_ZSTD, 5);
(void)grpc_call_reply_data(call, payload, payload_len);
```

### Explicit Per-Message & Bounded Decompression

```c
grpc_message *raw = grpc_message_new(payload, payload_len);
grpc_message *comp = grpc_message_compress(raw, GRPC_COMPRESS_ZSTD, 5);
grpc_message_free(raw);
if (comp != nullptr)
  (void)grpc_call_send(call, comp); /* takes ownership */
```

Inbound compressed requests are automatically decompressed with output bounded by `grpc_config.max_message_size` (excessive expansion ends the call with `RESOURCE_EXHAUSTED`). For raw buffers, `grpc_decompress_limited()` returns `grpc_err` (`GRPC_ERR_LIMIT` when output exceeds `max_output`).
