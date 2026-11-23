/* otlp_sink_basic.c - the null and memory sinks */
#include "otlp_threads.h"
#include <inttypes.h>
#include <string.h>

#include "grpc_mem.h"
#include "otlp_sink.h"

/* ---- null ---------------------------------------------------------------------------------- */

typedef struct null_sink {
  otlp_sink base;
  atomic_uint_least64_t batches;
  atomic_uint_least64_t points;
} null_sink;

static otlp_sink_result null_submit(otlp_sink *sink, const otlp_batch *batch,
                                    const otlp_sink_completion *done, const char **detail) {
  (void)done;
  (void)detail;
  null_sink *s = (null_sink *)sink;
  atomic_fetch_add_explicit(&s->batches, 1, memory_order_relaxed);
  atomic_fetch_add_explicit(&s->points, batch->points, memory_order_relaxed);
  return OTLP_SINK_ACCEPTED;
}

static void null_stats(otlp_sink *sink, otlp_strbuf *out) {
  null_sink *s = (null_sink *)sink;
  otlp_strbuf_printf(out, "\"batches\":%" PRIu64 ",\"points\":%" PRIu64,
                     (uint64_t)atomic_load_explicit(&s->batches, memory_order_relaxed),
                     (uint64_t)atomic_load_explicit(&s->points, memory_order_relaxed));
}

static void null_destroy(otlp_sink *sink) {
  grpc_free(sink);
}

static const otlp_sink_ops null_ops = {
    .name = "null",
    .submit = null_submit,
    .stats_json = null_stats,
    .destroy = null_destroy,
};

otlp_sink *otlp_null_sink_new(void) {
  null_sink *s = grpc_calloc(1, sizeof(*s));
  if (s == nullptr)
    return nullptr;
  s->base = (otlp_sink){.ops = &null_ops, .flags = 0};
  atomic_init(&s->batches, 0);
  atomic_init(&s->points, 0);
  return &s->base;
}

/* ---- memory -------------------------------------------------------------------------------- */

typedef struct mem_entry mem_entry;
struct mem_entry {
  mem_entry *next;
  size_t len;
  uint64_t points;
  uint8_t data[];
};

typedef struct memory_sink {
  otlp_sink base;
  size_t max_bytes;
  size_t max_batches;
  otlp_memory_full_policy policy;
  pthread_mutex_t lock;
  /* guarded by lock: */
  mem_entry *head, *tail;
  size_t bytes;
  size_t batches;
  uint64_t points;
  uint64_t stored_batches; /* ever stored */
  uint64_t evicted_batches;
  uint64_t evicted_points;
  uint64_t refused_batches;
  bool closed;
} memory_sink;

static void drop_head(memory_sink *s) {
  mem_entry *e = s->head;
  s->head = e->next;
  if (s->head == nullptr)
    s->tail = nullptr;
  s->bytes -= e->len;
  s->batches--;
  s->points -= e->points;
  grpc_free(e);
}

static bool fits(const memory_sink *s, size_t len) {
  return len <= s->max_bytes - s->bytes && (s->max_batches == 0 || s->batches < s->max_batches);
}

static otlp_sink_result memory_submit(otlp_sink *sink, const otlp_batch *batch,
                                      const otlp_sink_completion *done, const char **detail) {
  (void)done;
  memory_sink *s = (memory_sink *)sink;
  if (batch->payload_len > s->max_bytes) {
    *detail = "batch larger than the memory sink";
    return OTLP_SINK_FAILED;
  }
  /* copy outside the lock */
  mem_entry *e = grpc_malloc(sizeof(*e) + batch->payload_len);
  if (e == nullptr) {
    *detail = "memory sink out of memory";
    return OTLP_SINK_RETRYABLE;
  }
  *e = (mem_entry){.len = batch->payload_len, .points = batch->points};
  if (batch->payload_len > 0)
    memcpy(e->data, batch->payload, batch->payload_len);

  pthread_mutex_lock(&s->lock);
  if (s->closed) {
    pthread_mutex_unlock(&s->lock);
    grpc_free(e);
    *detail = "shutting down";
    return OTLP_SINK_RETRYABLE;
  }
  if (s->policy == OTLP_MEMORY_EVICT) {
    while (!fits(s, e->len) && s->head != nullptr) {
      s->evicted_batches++;
      s->evicted_points += s->head->points;
      drop_head(s);
    }
  }
  if (!fits(s, e->len)) {
    s->refused_batches++;
    pthread_mutex_unlock(&s->lock);
    grpc_free(e);
    *detail = "memory sink is full";
    return OTLP_SINK_RETRYABLE;
  }
  if (s->tail != nullptr)
    s->tail->next = e;
  else
    s->head = e;
  s->tail = e;
  s->bytes += e->len;
  s->batches++;
  s->points += e->points;
  s->stored_batches++;
  pthread_mutex_unlock(&s->lock);
  return OTLP_SINK_ACCEPTED;
}

static void memory_stats(otlp_sink *sink, otlp_strbuf *out) {
  memory_sink *s = (memory_sink *)sink;
  pthread_mutex_lock(&s->lock);
  otlp_strbuf_printf(out,
                     "\"batches\":%zu,\"bytes\":%zu,\"points\":%" PRIu64
                     ",\"stored_batches\":%" PRIu64 ",\"evicted_batches\":%" PRIu64
                     ",\"evicted_points\":%" PRIu64 ",\"refused_batches\":%" PRIu64
                     ",\"max_bytes\":%zu",
                     s->batches, s->bytes, s->points, s->stored_batches, s->evicted_batches,
                     s->evicted_points, s->refused_batches, s->max_bytes);
  pthread_mutex_unlock(&s->lock);
}

static void memory_shutdown(otlp_sink *sink) {
  memory_sink *s = (memory_sink *)sink;
  pthread_mutex_lock(&s->lock);
  s->closed = true;
  pthread_mutex_unlock(&s->lock);
}

static void memory_destroy(otlp_sink *sink) {
  memory_sink *s = (memory_sink *)sink;
  while (s->head != nullptr)
    drop_head(s);
  pthread_mutex_destroy(&s->lock);
  grpc_free(s);
}

static const otlp_sink_ops memory_ops = {
    .name = "memory",
    .submit = memory_submit,
    .stats_json = memory_stats,
    .shutdown = memory_shutdown,
    .destroy = memory_destroy,
};

otlp_sink *otlp_memory_sink_new(size_t max_bytes, size_t max_batches,
                                otlp_memory_full_policy policy) {
  memory_sink *s = grpc_calloc(1, sizeof(*s));
  if (s == nullptr)
    return nullptr;
  s->base = (otlp_sink){.ops = &memory_ops, .flags = OTLP_SINK_NEEDS_PAYLOAD};
  s->max_bytes = max_bytes;
  s->max_batches = max_batches;
  s->policy = policy;
  pthread_mutex_init(&s->lock, nullptr);
  return &s->base;
}

bool otlp_memory_sink_dump(otlp_sink *sink, size_t max_len, uint8_t **out, size_t *out_len,
                           size_t *batches) {
  sink = otlp_sink_unwrap(sink);
  if (sink->ops != &memory_ops)
    return false;
  memory_sink *s = (memory_sink *)sink;
  pthread_mutex_lock(&s->lock);
  bool ok = false;
  if (s->bytes <= max_len) {
    uint8_t *buf = grpc_malloc(s->bytes > 0 ? s->bytes : 1);
    if (buf != nullptr) {
      size_t off = 0;
      for (mem_entry *e = s->head; e != nullptr; e = e->next) {
        memcpy(buf + off, e->data, e->len);
        off += e->len;
      }
      *out = buf;
      *out_len = off;
      if (batches != nullptr)
        *batches = s->batches;
      ok = true;
    }
  }
  pthread_mutex_unlock(&s->lock);
  return ok;
}

void otlp_memory_sink_clear(otlp_sink *sink) {
  sink = otlp_sink_unwrap(sink);
  if (sink->ops != &memory_ops)
    return;
  memory_sink *s = (memory_sink *)sink;
  pthread_mutex_lock(&s->lock);
  while (s->head != nullptr)
    drop_head(s);
  pthread_mutex_unlock(&s->lock);
}
