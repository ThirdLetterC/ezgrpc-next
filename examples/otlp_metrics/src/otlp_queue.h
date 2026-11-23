#pragma once

/* otlp_queue.h - a bounded multi-producer, multi-consumer queue of pointers
 *
 * Producers never block: otlp_queue_push() fails when the queue is full, which the receiver turns
 * into a retryable UNAVAILABLE. Consumers block in otlp_queue_pop() until an item arrives or the
 * queue is closed and empty. Thread-safe.
 */

#include "otlp_threads.h"
#include <stdbool.h>
#include <stddef.h>

typedef struct otlp_queue {
  pthread_mutex_t lock;
  pthread_cond_t nonempty;
  void **items; /* ring of `capacity` slots */
  size_t capacity;
  size_t head;
  size_t count;
  size_t peak;
  bool closed;
} otlp_queue;

/** Allocates the ring. Returns false when out of memory or `capacity` is 0. */
bool otlp_queue_init(otlp_queue *queue, size_t capacity);

/** Appends `item` (non-null). Returns false when the queue is full or closed. */
bool otlp_queue_push(otlp_queue *queue, void *item);

/** Removes the oldest item, waiting for one. Returns nullptr once closed and empty. */
void *otlp_queue_pop(otlp_queue *queue);

/** Wakes every consumer; pushes fail from now on, pops drain what is left. */
void otlp_queue_close(otlp_queue *queue);

/** Current and highest number of queued items. */
void otlp_queue_depth(otlp_queue *queue, size_t *count, size_t *peak);

/** Frees the ring. The queue must be empty and have no waiting consumers. */
void otlp_queue_destroy(otlp_queue *queue);
