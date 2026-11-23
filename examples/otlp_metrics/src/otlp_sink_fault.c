/* otlp_sink_fault.c - fault injection around another sink
 *
 * Deterministic failures (every Nth batch RETRYABLE or FAILED) and a gate. A closed gate holds
 * batches, completing them only when it opens: tests use it to keep requests in flight on purpose,
 * filling queues and memory reservations to an exact, known level without sleeping.
 */
#include "otlp_threads.h"
#include <inttypes.h>
#include <string.h>

#include "grpc_mem.h"
#include "otlp_sink.h"

typedef struct held held;
struct held {
  held *next;
  otlp_sink_completion done;
  uint64_t points;
  size_t len;
  size_t reserved;
  uint8_t data[];
};

typedef struct fault_sink {
  otlp_sink base;
  otlp_sink *inner;
  otlp_fault_config config;
  otlp_budget *budget;
  pthread_mutex_t lock;
  pthread_cond_t changed; /* gate opened/closed, sink closed, held count changed */
  /* guarded by lock: */
  uint64_t submits;
  size_t blocked; /* threads waiting in a blocking gate */
  bool gate_closed;
  bool quiesced; /* refuse batches that would be held */
  bool closed;
  held *head, *tail;
  size_t held_count;
  uint64_t injected_retryable;
  uint64_t injected_failed;
} fault_sink;

static const otlp_sink_ops fault_ops;

otlp_sink *otlp_sink_unwrap(otlp_sink *sink) {
  return sink != nullptr && sink->ops == &fault_ops ? ((fault_sink *)sink)->inner : sink;
}

/* Hands a held batch to the inner sink and completes it (unless the inner sink defers). */
static void forward(fault_sink *s, held *h) {
  otlp_sink_result result;
  const char *detail = nullptr;
  if (h->done.cancelled != nullptr && atomic_load(h->done.cancelled)) {
    result = OTLP_SINK_RETRYABLE;
    detail = "cancelled";
  } else {
    otlp_batch batch = {
        .payload = h->len > 0 ? h->data : nullptr, .payload_len = h->len, .points = h->points};
    result = otlp_sink_submit(s->inner, &batch, &h->done, &detail);
  }
  if (h->reserved != 0 && s->budget != nullptr)
    otlp_budget_release(s->budget, h->reserved);
  otlp_sink_completion done = h->done;
  grpc_free(h);
  if (result != OTLP_SINK_PENDING)
    done.fn(done.ctx, result, detail);
}

static otlp_sink_result fault_submit(otlp_sink *sink, const otlp_batch *batch,
                                     const otlp_sink_completion *done, const char **detail) {
  fault_sink *s = (fault_sink *)sink;
  pthread_mutex_lock(&s->lock);
  uint64_t n = ++s->submits;
  bool gate_closed = s->gate_closed;
  bool closed = s->closed;
  if (!closed && s->config.retryable_every != 0 && n % s->config.retryable_every == 0) {
    s->injected_retryable++;
    pthread_mutex_unlock(&s->lock);
    *detail = "injected retryable failure";
    return OTLP_SINK_RETRYABLE;
  }
  if (!closed && s->config.fail_every != 0 && n % s->config.fail_every == 0) {
    s->injected_failed++;
    pthread_mutex_unlock(&s->lock);
    *detail = "injected permanent failure";
    return OTLP_SINK_FAILED;
  }
  pthread_mutex_unlock(&s->lock);
  if (closed) {
    *detail = "shutting down";
    return OTLP_SINK_RETRYABLE;
  }
  if (!gate_closed)
    return otlp_sink_submit(s->inner, batch, done, detail);

  if (s->config.gate_blocks) {
    /* a slow sink: keep the caller (a worker) here until the gate opens */
    pthread_mutex_lock(&s->lock);
    s->blocked++;
    pthread_cond_broadcast(&s->changed);
    while (s->gate_closed && !s->closed)
      pthread_cond_wait(&s->changed, &s->lock);
    s->blocked--;
    bool now_closed = s->closed;
    pthread_mutex_unlock(&s->lock);
    if (now_closed) {
      *detail = "shutting down";
      return OTLP_SINK_RETRYABLE;
    }
    return otlp_sink_submit(s->inner, batch, done, detail);
  }

  pthread_mutex_lock(&s->lock);
  bool quiesced = s->quiesced;
  pthread_mutex_unlock(&s->lock);
  if (quiesced) {
    *detail = "shutting down";
    return OTLP_SINK_RETRYABLE;
  }

  /* hold a copy until the gate opens */
  size_t len = batch->payload != nullptr ? batch->payload_len : 0;
  size_t reserved = len;
  if (reserved != 0 && s->budget != nullptr && !otlp_budget_reserve(s->budget, reserved)) {
    *detail = "memory limit reached";
    return OTLP_SINK_RETRYABLE;
  }
  held *h = grpc_malloc(sizeof(*h) + len);
  if (h == nullptr) {
    if (reserved != 0 && s->budget != nullptr)
      otlp_budget_release(s->budget, reserved);
    *detail = "out of memory";
    return OTLP_SINK_RETRYABLE;
  }
  *h = (held){.done = *done, .points = batch->points, .len = len, .reserved = reserved};
  if (len > 0)
    memcpy(h->data, batch->payload, len);

  pthread_mutex_lock(&s->lock);
  if (!s->gate_closed || s->closed || s->quiesced || s->held_count >= s->config.gate_capacity) {
    /* the gate opened (or the sink quiesced, or filled up) meanwhile */
    bool now_closed = s->closed || s->quiesced;
    bool full = s->gate_closed && !now_closed;
    pthread_mutex_unlock(&s->lock);
    if (reserved != 0 && s->budget != nullptr)
      otlp_budget_release(s->budget, reserved);
    grpc_free(h);
    if (full) {
      *detail = "fault gate is full";
      return OTLP_SINK_RETRYABLE;
    }
    if (now_closed) {
      *detail = "shutting down";
      return OTLP_SINK_RETRYABLE;
    }
    return otlp_sink_submit(s->inner, batch, done, detail);
  }
  if (s->tail != nullptr)
    s->tail->next = h;
  else
    s->head = h;
  s->tail = h;
  s->held_count++;
  pthread_cond_broadcast(&s->changed);
  pthread_mutex_unlock(&s->lock);
  return OTLP_SINK_PENDING;
}

void otlp_fault_sink_set_gate(otlp_sink *sink, bool open) {
  if (sink == nullptr || sink->ops != &fault_ops)
    return;
  fault_sink *s = (fault_sink *)sink;
  pthread_mutex_lock(&s->lock);
  s->gate_closed = !open;
  pthread_cond_broadcast(&s->changed);
  held *list = nullptr;
  if (open) {
    list = s->head;
    s->head = s->tail = nullptr;
    s->held_count = 0;
  }
  pthread_mutex_unlock(&s->lock);
  while (list != nullptr) {
    held *next = list->next;
    forward(s, list);
    list = next;
  }
}

size_t otlp_fault_sink_held(otlp_sink *sink) {
  if (sink == nullptr || sink->ops != &fault_ops)
    return 0;
  fault_sink *s = (fault_sink *)sink;
  pthread_mutex_lock(&s->lock);
  size_t n = s->held_count + s->blocked;
  pthread_mutex_unlock(&s->lock);
  return n;
}

void otlp_fault_sink_wait_held(otlp_sink *sink, size_t count) {
  if (sink == nullptr || sink->ops != &fault_ops)
    return;
  fault_sink *s = (fault_sink *)sink;
  pthread_mutex_lock(&s->lock);
  while (s->held_count + s->blocked < count && !s->closed)
    pthread_cond_wait(&s->changed, &s->lock);
  pthread_mutex_unlock(&s->lock);
}

static void fault_stats(otlp_sink *sink, otlp_strbuf *out) {
  fault_sink *s = (fault_sink *)sink;
  pthread_mutex_lock(&s->lock);
  otlp_strbuf_printf(out,
                     "\"fault\":{\"submits\":%" PRIu64
                     ",\"gate_closed\":%s,\"held\":%zu,\"blocked\":%zu"
                     ",\"injected_retryable\":%" PRIu64 ",\"injected_failed\":%" PRIu64 "},",
                     s->submits, s->gate_closed ? "true" : "false", s->held_count, s->blocked,
                     s->injected_retryable, s->injected_failed);
  pthread_mutex_unlock(&s->lock);
  otlp_strbuf_printf(out, "\"inner\":\"%s\"", s->inner->ops->name);
  if (s->inner->ops->stats_json != nullptr) {
    otlp_strbuf_printf(out, ",");
    s->inner->ops->stats_json(s->inner, out);
  }
}

/* Fails the held batches (never stored) and refuses new ones that a closed gate would hold, so
 * their calls get answered while the server still drains. Threads blocked in a blocking gate
 * model a slow sink that is still working: they keep waiting for the gate (or shutdown()). */
static void fault_quiesce(otlp_sink *sink) {
  fault_sink *s = (fault_sink *)sink;
  pthread_mutex_lock(&s->lock);
  s->quiesced = true;
  held *list = s->head;
  s->head = s->tail = nullptr;
  s->held_count = 0;
  pthread_cond_broadcast(&s->changed);
  pthread_mutex_unlock(&s->lock);
  while (list != nullptr) {
    held *next = list->next;
    if (list->reserved != 0 && s->budget != nullptr)
      otlp_budget_release(s->budget, list->reserved);
    otlp_sink_completion done = list->done;
    grpc_free(list);
    done.fn(done.ctx, OTLP_SINK_RETRYABLE, "shutting down");
    list = next;
  }
  otlp_sink_quiesce(s->inner);
}

static void fault_shutdown(otlp_sink *sink) {
  fault_sink *s = (fault_sink *)sink;
  fault_quiesce(sink);
  pthread_mutex_lock(&s->lock);
  s->closed = true; /* wakes blocked submitters, which refuse their batches */
  pthread_cond_broadcast(&s->changed);
  pthread_mutex_unlock(&s->lock);
  otlp_sink_shutdown(s->inner);
}

static void fault_destroy(otlp_sink *sink) {
  fault_sink *s = (fault_sink *)sink;
  otlp_sink_destroy(s->inner);
  otlp_cond_destroy(&s->changed);
  pthread_mutex_destroy(&s->lock);
  grpc_free(s);
}

static const otlp_sink_ops fault_ops = {
    .name = "fault",
    .submit = fault_submit,
    .stats_json = fault_stats,
    .quiesce = fault_quiesce,
    .shutdown = fault_shutdown,
    .destroy = fault_destroy,
};

otlp_sink *otlp_fault_sink_new(otlp_sink *inner, const otlp_fault_config *config,
                               otlp_budget *budget) {
  fault_sink *s = grpc_calloc(1, sizeof(*s));
  if (s == nullptr)
    return nullptr;
  s->base = (otlp_sink){.ops = &fault_ops, .flags = inner->flags | OTLP_SINK_MAY_DEFER};
  s->inner = inner;
  s->config = *config;
  if (s->config.gate_capacity == 0)
    s->config.gate_capacity = 1024;
  s->budget = budget;
  s->gate_closed = config->gate_closed;
  pthread_mutex_init(&s->lock, nullptr);
  otlp_cond_init(&s->changed);
  return &s->base;
}
