#include "otlp_queue.h"

#include "grpc_mem.h"

bool otlp_queue_init(otlp_queue *queue, size_t capacity) {
  *queue = (otlp_queue){.capacity = capacity};
  if (capacity == 0)
    return false;
  queue->items = grpc_calloc(capacity, sizeof(void *));
  if (queue->items == nullptr)
    return false;
  pthread_mutex_init(&queue->lock, nullptr);
  otlp_cond_init(&queue->nonempty);
  return true;
}

bool otlp_queue_push(otlp_queue *queue, void *item) {
  pthread_mutex_lock(&queue->lock);
  if (queue->closed || queue->count == queue->capacity) {
    pthread_mutex_unlock(&queue->lock);
    return false;
  }
  queue->items[(queue->head + queue->count) % queue->capacity] = item;
  queue->count++;
  if (queue->count > queue->peak)
    queue->peak = queue->count;
  pthread_cond_signal(&queue->nonempty);
  pthread_mutex_unlock(&queue->lock);
  return true;
}

void *otlp_queue_pop(otlp_queue *queue) {
  pthread_mutex_lock(&queue->lock);
  while (queue->count == 0 && !queue->closed)
    pthread_cond_wait(&queue->nonempty, &queue->lock);
  void *item = nullptr;
  if (queue->count > 0) {
    item = queue->items[queue->head];
    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;
  }
  pthread_mutex_unlock(&queue->lock);
  return item;
}

void otlp_queue_close(otlp_queue *queue) {
  pthread_mutex_lock(&queue->lock);
  queue->closed = true;
  pthread_cond_broadcast(&queue->nonempty);
  pthread_mutex_unlock(&queue->lock);
}

void otlp_queue_depth(otlp_queue *queue, size_t *count, size_t *peak) {
  pthread_mutex_lock(&queue->lock);
  *count = queue->count;
  *peak = queue->peak;
  pthread_mutex_unlock(&queue->lock);
}

void otlp_queue_destroy(otlp_queue *queue) {
  if (queue->items == nullptr)
    return;
  otlp_cond_destroy(&queue->nonempty);
  pthread_mutex_destroy(&queue->lock);
  grpc_free(queue->items);
  queue->items = nullptr;
}
