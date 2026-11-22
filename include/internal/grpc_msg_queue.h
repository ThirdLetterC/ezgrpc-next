#pragma once

/* grpc_msg_queue.h - FIFO queue of grpc_message pointers
 *
 * A growable ring buffer embedded by value: a call's received messages waiting for the
 * application, a stream's replies waiting for the transport, and a client stream's received
 * responses. A zeroed grpc_msg_queue is a valid empty queue, and only growing it allocates. The
 * queue owns the messages it holds: msg_queue_pop() hands one to the caller and
 * msg_queue_clear() frees the rest along with the buffer. Not thread-safe.
 */
#include <assert.h>
#include <stdint.h>
#include "grpc_defs.h"
#include "grpc_error.h"
#include "grpc_mem.h"
#include "grpc_message.h"

typedef struct {
  grpc_message **items; /* cap slots from grpc_malloc, or nullptr while cap is 0 */
  size_t head;          /* slot of the oldest message */
  size_t count;         /* queued messages */
  size_t cap;
} grpc_msg_queue;

/* Returns the number of queued messages. */
static inline size_t msg_queue_count(const grpc_msg_queue *q) {
  return q->count;
}

/* True if no message is queued. */
static inline bool msg_queue_is_empty(const grpc_msg_queue *q) {
  return q->count == 0;
}

/* Returns the oldest message without removing it, or nullptr if the queue is empty. */
static inline grpc_message *msg_queue_peek(const grpc_msg_queue *q) {
  return q->count > 0 ? q->items[q->head] : nullptr;
}

/* Removes and returns the oldest message (the caller owns it), or nullptr if the queue is
 * empty. Keeps the buffer for later pushes. */
static inline grpc_message *msg_queue_pop(grpc_msg_queue *q) {
  if (q->count == 0)
    return nullptr;
  grpc_message *msg = q->items[q->head];
  q->head = q->head + 1 == q->cap ? 0 : q->head + 1;
  if (--q->count == 0)
    q->head = 0;
  return msg;
}

/* Grows the buffer to at least need slots (doubling, from 4), keeping the order. Returns
 * GRPC_OK, or GRPC_ERR_NOMEM with the queue unchanged. */
static inline grpc_err msg_queue_reserve(grpc_msg_queue *q, size_t need) {
  if (need <= q->cap)
    return GRPC_OK;
  size_t cap = q->cap > 0 ? q->cap : 4;
  while (cap < need) {
    if (cap > SIZE_MAX / 2 / sizeof(*q->items))
      return GRPC_ERR_NOMEM;
    cap *= 2;
  }
  grpc_message **items = grpc_malloc(cap * sizeof(*items));
  if (items == nullptr)
    return GRPC_ERR_NOMEM;
  for (size_t i = 0; i < q->count; i++) {
    size_t slot = q->head + i;
    items[i] = q->items[slot < q->cap ? slot : slot - q->cap];
  }
  grpc_free(q->items);
  q->items = items;
  q->cap = cap;
  q->head = 0;
  return GRPC_OK;
}

/* Appends msg (not nullptr); the queue owns it on success. Returns GRPC_OK, or GRPC_ERR_NOMEM
 * when the buffer cannot grow, in which case the caller still owns msg. */
static inline grpc_err msg_queue_push(grpc_msg_queue *q, grpc_message *msg) {
  assert(msg != nullptr);
  if (q->count == q->cap) {
    grpc_err err = msg_queue_reserve(q, q->count + 1);
    if (err != GRPC_OK)
      return err;
  }
  size_t tail = q->head + q->count;
  q->items[tail < q->cap ? tail : tail - q->cap] = msg;
  q->count++;
  return GRPC_OK;
}

/* Frees every queued message and the buffer, leaving an empty queue that may be reused. */
static inline void msg_queue_clear(grpc_msg_queue *q) {
  grpc_message *msg;
  while ((msg = msg_queue_pop(q)) != nullptr)
    grpc_message_free(msg);
  grpc_free(q->items);
  q->items = nullptr;
  q->cap = 0;
  q->head = 0;
}
