/* grpc_call_async.c - the *_async call functions, posted from other threads to the server thread
 *
 * Part of the call layer described in grpc_call.c.
 */

#include <stdarg.h>
#include <stdio.h>
#include "internal/grpc_platform.h"

#include "core.h"
#include "internal_call.h"
#include "internal_helpers.h"
#include "grpc_timeout.h"

/* ---- from other threads -------------------------------------------------------- */

typedef enum async_op_kind {
  ASYNC_OP_SEND,
  ASYNC_OP_REPLY,
  ASYNC_OP_FINISH,
  ASYNC_OP_METADATA,
  ASYNC_OP_TRAILERS,
} async_op_kind;

typedef struct async_op async_op;
struct async_op {
  async_op_kind kind;
  grpc_call *call;
  grpc_message *message;
  grpc_status status;
  char *text;
  grpc_metadata *metadata;
  size_t metadata_count;
};

/* Frees an op and the copies it owns (metadata strings, finish message). The message is not
 * freed here: run_async_op() hands it to the call, post_async_op() frees it on failure. */
static void async_op_free(async_op *op) {
  if (op == nullptr)
    return;
  for (size_t i = 0; i < op->metadata_count; i++) {
    grpc_free(grpc_unconst(op->metadata[i].key));
    grpc_free(grpc_unconst(op->metadata[i].value));
  }
  grpc_free(op->metadata);
  grpc_free(op->text);
  grpc_free(op);
}

/* Server-thread side of the *_async functions: replays the synchronous call function, which
 * takes ownership of op->message, then drops the reference taken by post_async_op(). Errors
 * are ignored; there is no one to report them to. */
static void run_async_op(void *arg) {
  async_op *op = arg;
  switch (op->kind) {
  case ASYNC_OP_SEND:
    (void)grpc_call_send(op->call, op->message);
    break;
  case ASYNC_OP_REPLY:
    (void)grpc_call_reply(op->call, op->message);
    break;
  case ASYNC_OP_FINISH:
    (void)grpc_call_finish(op->call, op->status, op->text);
    break;
  case ASYNC_OP_METADATA:
    (void)grpc_call_send_metadata(op->call, op->metadata, op->metadata_count);
    break;
  case ASYNC_OP_TRAILERS:
    (void)grpc_call_add_trailers(op->call, op->metadata, op->metadata_count);
    break;
  }
  grpc_call_unref(op->call);
  async_op_free(op);
}

/* Takes a reference on the call for the op and posts it to the server thread. On failure
 * releases the reference, the message and the op, so the caller owns nothing afterwards. */
static grpc_err post_async_op(async_op *op) {
  /* the caller holds a reference, so the call is alive to take another */
  grpc_call_ref(op->call);
  grpc_err err = grpc_server_post(op->call->server, run_async_op, op);
  if (err != GRPC_OK) {
    grpc_call_unref(op->call);
    grpc_message_free(op->message);
    async_op_free(op);
  }
  return err;
}

/* Validates and deep-copies `metadata` (the caller's strings may go away once this returns),
 * then posts a send-metadata or add-trailers op. Validation matches nv_list_add_metadata() so
 * that bad input is reported to the caller instead of being dropped on the server thread. */
static grpc_err post_async_metadata(grpc_call *call, async_op_kind kind,
                                    const grpc_metadata *metadata, size_t count) {
  if (call == nullptr || (metadata == nullptr && count > 0))
    return GRPC_ERR_INVALID;
  async_op *op = grpc_calloc(1, sizeof(*op));
  if (op == nullptr)
    return GRPC_ERR_NOMEM;
  op->kind = kind;
  op->call = call;
  if (count > 0) {
    op->metadata = grpc_calloc(count, sizeof(*op->metadata));
    if (op->metadata == nullptr) {
      grpc_free(op);
      return GRPC_ERR_NOMEM;
    }
    for (size_t i = 0; i < count; i++) {
      if (metadata[i].key == nullptr || metadata[i].value == nullptr) {
        op->metadata_count = i;
        async_op_free(op);
        return GRPC_ERR_INVALID;
      }
      size_t klen = metadata[i].key_len ? metadata[i].key_len : strlen(metadata[i].key);
      size_t vlen = metadata[i].value_len ? metadata[i].value_len : strlen(metadata[i].value);
      if (!metadata_entry_valid(metadata[i].key, klen, metadata[i].value, vlen, true)) {
        op->metadata_count = i;
        async_op_free(op);
        return GRPC_ERR_INVALID;
      }
      char *k = grpc_strndup(metadata[i].key, klen);
      char *v = grpc_strndup(metadata[i].value, vlen);
      if (k == nullptr || v == nullptr) {
        grpc_free(k);
        grpc_free(v);
        op->metadata_count = i;
        async_op_free(op);
        return GRPC_ERR_NOMEM;
      }
      op->metadata[i] = (grpc_metadata){.key = k, .key_len = klen, .value = v, .value_len = vlen};
    }
    op->metadata_count = count;
  }
  return post_async_op(op);
}

/* Posts grpc_call_send_metadata() with a copy of `metadata`. */
GRPC_API grpc_err grpc_call_send_metadata_async(grpc_call *call, const grpc_metadata *metadata,
                                                size_t count) {
  return post_async_metadata(call, ASYNC_OP_METADATA, metadata, count);
}

/* Posts grpc_call_add_trailers() with a copy of `metadata`. */
GRPC_API grpc_err grpc_call_add_trailers_async(grpc_call *call, const grpc_metadata *metadata,
                                               size_t count) {
  return post_async_metadata(call, ASYNC_OP_TRAILERS, metadata, count);
}

/* Posts grpc_call_send(). Takes ownership of `message`, freeing it on every error. */
GRPC_API grpc_err grpc_call_send_async(grpc_call *call, grpc_message *message) {
  if (call == nullptr || message == nullptr) {
    grpc_message_free(message);
    return GRPC_ERR_INVALID;
  }
  async_op *op = grpc_calloc(1, sizeof(*op));
  if (op == nullptr) {
    grpc_message_free(message);
    return GRPC_ERR_NOMEM;
  }
  op->kind = ASYNC_OP_SEND;
  op->call = call;
  op->message = message;
  return post_async_op(op);
}

/* Copies `data` into a message and posts it with grpc_call_send_async(). */
GRPC_API grpc_err grpc_call_send_data_async(grpc_call *call, const void *data, size_t len) {
  if (call == nullptr || (data == nullptr && len > 0))
    return GRPC_ERR_INVALID;
  grpc_message *message = grpc_message_new(data, len);
  if (message == nullptr)
    return GRPC_ERR_NOMEM;
  return grpc_call_send_async(call, message);
}

/* Posts grpc_call_reply(). Takes ownership of `message`, freeing it on every error. */
GRPC_API grpc_err grpc_call_reply_async(grpc_call *call, grpc_message *message) {
  if (call == nullptr || message == nullptr) {
    grpc_message_free(message);
    return GRPC_ERR_INVALID;
  }
  async_op *op = grpc_calloc(1, sizeof(*op));
  if (op == nullptr) {
    grpc_message_free(message);
    return GRPC_ERR_NOMEM;
  }
  op->kind = ASYNC_OP_REPLY;
  op->call = call;
  op->message = message;
  return post_async_op(op);
}

/* Posts grpc_call_finish() with a copy of `message`. */
GRPC_API grpc_err grpc_call_finish_async(grpc_call *call, grpc_status status, const char *message) {
  if (call == nullptr)
    return GRPC_ERR_INVALID;
  async_op *op = grpc_calloc(1, sizeof(*op));
  if (op == nullptr)
    return GRPC_ERR_NOMEM;
  op->kind = ASYNC_OP_FINISH;
  op->call = call;
  op->status = status;
  if (message != nullptr && (op->text = grpc_strdup(message)) == nullptr) {
    grpc_free(op);
    return GRPC_ERR_NOMEM;
  }
  return post_async_op(op);
}
