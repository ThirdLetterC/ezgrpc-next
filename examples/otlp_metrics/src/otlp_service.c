#include "otlp_service.h"

#include "otlp_threads.h"
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <uv.h>

#include "otlp_arena.h"
#include "otlp_proto.h"
#include "otlp_queue.h"
#include "otlp_response.h"
#include "otlp_validate.h"
#include "otlp_wire.h"

#define ADMIN_PREFIX  "/otlp_metrics.Admin/"
#define EXPORT_PREFIX "/opentelemetry.proto.collector.metrics.v1.MetricsService/"

/* Dumps of the memory sink are refused above this size (the response must stay bounded). */
enum { MAX_DUMP_BYTES = 64 * 1024 * 1024 };

/* Per event loop state: the decode arena of that loop's thread. */
typedef struct otlp_loop otlp_loop;
struct otlp_loop {
  otlp_service *svc;
  grpc_server *server;
  otlp_arena arena;
  otlp_loop *next;
};

typedef struct otlp_worker {
  otlp_service *svc;
  pthread_t thread;
  otlp_arena arena;
} otlp_worker;

/* An /otlp_metrics.Admin/Wait call parked until a counter reaches a threshold. Freed with its call
 * (call userdata), so callbacks of the call can always use it. */
typedef struct waiter waiter;
struct waiter {
  waiter *prev, *next;
  otlp_service *svc;
  grpc_call *call; /* our reference while listed */
  bool listed;
  uint64_t threshold;
  char counter[48];
};

struct otlp_service {
  otlp_service_config config;
  char *auth_token;
  size_t auth_token_len;
  otlp_sink *sink;
  otlp_budget *budget;
  otlp_stats stats;
  atomic_bool shutting_down;
  atomic_size_t decoded_peak;
  atomic_uint_least64_t in_flight;

  otlp_queue queue;
  otlp_worker *workers;
  unsigned nb_workers; /* started */

  otlp_loop *loops; /* attached before serving; read-only afterwards */

  pthread_mutex_t waiters_lock;
  waiter *waiters; /* guarded by waiters_lock */
  atomic_size_t nb_waiters;
  bool stopped;
};

/* A queued or deferred Export call. Freed with its call (call userdata). */
typedef struct otlp_job {
  otlp_service *svc;
  grpc_call *call;             /* our reference until the job is finished */
  const grpc_message *request; /* owned by the call */
  uint64_t arrival_ns;
  size_t reserved; /* bytes reserved from the budget for this job */
  atomic_bool cancelled;
  /* kept for a deferred sink completion: */
  grpc_message *response;
  uint64_t points, accepted, rejected;
} otlp_job;

/* How a call ends. */
typedef struct outcome {
  grpc_status status;
  bool retry;             /* attach RetryInfo (retryable error) */
  bool pending;           /* the sink answers later */
  const char *message;    /* grpc-message of an error */
  grpc_message *response; /* status OK: the encoded response (owned) */
  uint64_t points, accepted, rejected;
  char text[OTLP_REASON_MAX + 64];
} outcome;

static void fail(outcome *out, grpc_status status, bool retry, const char *message) {
  out->status = status;
  out->retry = retry;
  out->message = message;
  if (out->response != nullptr) {
    grpc_message_free(out->response);
    out->response = nullptr;
  }
}

static uint64_t now_ns(void) {
  return uv_hrtime();
}

static bool timed_out(const otlp_service *svc, uint64_t arrival_ns) {
  return svc->config.request_timeout_ms != 0 &&
         now_ns() - arrival_ns > (uint64_t)svc->config.request_timeout_ms * 1000000u;
}

static void raise_max(atomic_size_t *max, size_t value) {
  size_t cur = atomic_load_explicit(max, memory_order_relaxed);
  while (value > cur && !atomic_compare_exchange_weak_explicit(
                            max, &cur, value, memory_order_relaxed, memory_order_relaxed)) {
  }
}

/* ---- accounting and waiters ------------------------------------------------------------- */

static void notify_waiters(otlp_service *svc);

static void account(otlp_service *svc, const outcome *out) {
  otlp_stats *st = &svc->stats;
  if ((unsigned)out->status < 17)
    OTLP_STAT_ADD(st, status[out->status], 1);
  if (out->status == GRPC_STATUS_OK) {
    if (out->rejected > 0)
      OTLP_STAT_ADD(st, requests_partial, 1);
    else
      OTLP_STAT_ADD(st, requests_ok, 1);
    OTLP_STAT_ADD(st, points_accepted, out->accepted);
    OTLP_STAT_ADD(st, points_rejected, out->rejected);
  } else {
    OTLP_STAT_ADD(st, requests_failed, 1);
    OTLP_STAT_ADD(st, points_failed, out->points);
  }
  notify_waiters(svc);
}

/* Answers `call` according to `out` and accounts for it. `async` when not on the call's own
 * server thread. */
static void answer(otlp_service *svc, grpc_call *call, outcome *out, bool async) {
  if (out->status == GRPC_STATUS_OK) {
    grpc_message *msg = out->response;
    out->response = nullptr;
    (void)(async ? grpc_call_reply_async(call, msg) : grpc_call_reply(call, msg));
  } else if (out->retry) {
    uint8_t retry_info[32];
    size_t len = otlp_retry_info_encode(retry_info, svc->config.retry_delay_ms);
    grpc_status_detail_entry detail = {
        .type_url = OTLP_RETRY_INFO_TYPE_URL, .value = retry_info, .value_len = len};
    /* The OTLP spec carries RetryInfo in the status details (grpc-status-details-bin), which is
     * what the Go exporter reads. The Python exporter reads a "google.rpc.retryinfo-bin" trailer
     * instead, so the same RetryInfo goes there too. */
    char *b64 = grpc_metadata_encode_bin(retry_info, len, nullptr);
    if (b64 != nullptr) {
      grpc_metadata trailer = {.key = OTLP_RETRY_INFO_TRAILER, .value = b64};
      (void)(async ? grpc_call_add_trailers_async(call, &trailer, 1)
                   : grpc_call_add_trailers(call, &trailer, 1));
      grpc_free(b64);
    }
    (void)(async ? grpc_call_finish_with_details_async(call, out->status, out->message, &detail, 1)
                 : grpc_call_finish_with_details(call, out->status, out->message, &detail, 1));
  } else {
    (void)(async ? grpc_call_finish_async(call, out->status, out->message)
                 : grpc_call_finish(call, out->status, out->message));
  }
  account(svc, out);
}

static void unlink_waiter(otlp_service *svc, waiter *w) {
  if (w->prev != nullptr)
    w->prev->next = w->next;
  else
    svc->waiters = w->next;
  if (w->next != nullptr)
    w->next->prev = w->prev;
  w->prev = w->next = nullptr;
  w->listed = false;
  atomic_fetch_sub(&svc->nb_waiters, 1);
}

static grpc_message *stats_message(otlp_service *svc) {
  otlp_strbuf buf = {0};
  otlp_service_stats_json(svc, &buf);
  grpc_message *msg = buf.failed ? nullptr : grpc_message_new(buf.data, buf.len);
  otlp_strbuf_free(&buf);
  return msg;
}

static void notify_waiters(otlp_service *svc) {
  if (atomic_load(&svc->nb_waiters) == 0)
    return;
  otlp_stats_snapshot snap;
  otlp_stats_snapshot_get(&svc->stats, &snap);
  pthread_mutex_lock(&svc->waiters_lock);
  waiter *w = svc->waiters;
  while (w != nullptr) {
    waiter *next = w->next;
    uint64_t value = 0;
    if (otlp_stats_lookup(&snap, w->counter, &value) && value >= w->threshold) {
      grpc_call *call = w->call;
      unlink_waiter(svc, w);
      grpc_message *msg = stats_message(svc);
      if (msg != nullptr)
        (void)grpc_call_reply_async(call, msg);
      else
        (void)grpc_call_finish_async(call, GRPC_STATUS_RESOURCE_EXHAUSTED, "out of memory");
      grpc_call_unref(call); /* may free w */
    }
    w = next;
  }
  pthread_mutex_unlock(&svc->waiters_lock);
}

/* ---- processing ------------------------------------------------------------------------- */

static void on_sink_done(void *ctx, otlp_sink_result result, const char *detail);

/* Decodes, validates and submits one request. On return the arena may be ended: the sink copied
 * what it keeps. `job` is nullptr for calls processed synchronously on their loop. */
static void process(otlp_service *svc, otlp_arena *arena, otlp_job *job,
                    const grpc_message *request, uint64_t arrival_ns, outcome *out) {
  *out = (outcome){.status = GRPC_STATUS_OK};
  const otlp_service_config *cfg = &svc->config;
  otlp_stats *st = &svc->stats;

  if (job != nullptr && atomic_load(&job->cancelled)) {
    OTLP_STAT_ADD(st, cancelled, 1);
    fail(out, GRPC_STATUS_CANCELLED, false, "cancelled");
    return;
  }
  if (timed_out(svc, arrival_ns)) {
    OTLP_STAT_ADD(st, deadline_exceeded, 1);
    fail(out, GRPC_STATUS_DEADLINE_EXCEEDED, false, "request timed out before processing");
    return;
  }

  switch (
      otlp_wire_check(OTLP_PB_REQUEST_DESCRIPTOR, request->data, request->len, cfg->max_depth)) {
  case OTLP_WIRE_OK:
    break;
  case OTLP_WIRE_TOO_DEEP:
    OTLP_STAT_ADD(st, too_deep, 1);
    (void)snprintf(out->text, sizeof(out->text), "request nests messages deeper than %u levels",
                   cfg->max_depth);
    fail(out, GRPC_STATUS_INVALID_ARGUMENT, false, out->text);
    return;
  default:
    OTLP_STAT_ADD(st, malformed, 1);
    fail(out, GRPC_STATUS_INVALID_ARGUMENT, false, "malformed ExportMetricsServiceRequest");
    return;
  }

  otlp_arena_begin(arena, cfg->max_decoded_bytes, request->len);
  otlp_pb_request *pb = (otlp_pb_request *)protobuf_c_message_unpack(
      OTLP_PB_REQUEST_DESCRIPTOR, &arena->allocator, request->len, request->data);
  raise_max(&svc->decoded_peak, arena->used);
  if (pb == nullptr) {
    switch (otlp_arena_status_get(arena)) {
    case OTLP_ARENA_LIMIT:
      OTLP_STAT_ADD(st, too_large, 1);
      (void)snprintf(out->text, sizeof(out->text),
                     "request decodes to more than %zu bytes (max decoded size)",
                     cfg->max_decoded_bytes);
      fail(out, GRPC_STATUS_RESOURCE_EXHAUSTED, false, out->text);
      return;
    case OTLP_ARENA_NOMEM:
      OTLP_STAT_ADD(st, throttled_memory, 1);
      fail(out, GRPC_STATUS_UNAVAILABLE, true, "memory limit reached");
      return;
    default:
      OTLP_STAT_ADD(st, malformed, 1);
      fail(out, GRPC_STATUS_INVALID_ARGUMENT, false, "malformed ExportMetricsServiceRequest");
      return;
    }
  }

  otlp_counts counts;
  otlp_validate_request(pb, &counts);
  out->points = counts.points;
  out->rejected = counts.rejected;
  out->accepted = counts.points - counts.rejected;
  OTLP_STAT_ADD(st, points_received, counts.points);

  /* the response is built first: once the sink has accepted the batch, answering must not fail */
  if (counts.rejected > 0) {
    char message[OTLP_REASON_MAX + 96];
    (void)snprintf(message, sizeof(message),
                   "rejected %" PRIu64 " of %" PRIu64 " data points; first: %s", counts.rejected,
                   counts.points, counts.reason);
    out->response = otlp_response_new(counts.rejected, message);
  } else {
    out->response = otlp_response_new(0, nullptr);
  }
  if (out->response == nullptr) {
    OTLP_STAT_ADD(st, throttled_memory, 1);
    fail(out, GRPC_STATUS_UNAVAILABLE, true, "out of memory");
    return;
  }
  if (out->accepted == 0)
    return; /* an empty request, or every point rejected: nothing to store */

  otlp_batch batch = {.points = out->accepted};
  if ((svc->sink->flags & OTLP_SINK_NEEDS_PAYLOAD) != 0) {
    if (counts.rejected == 0) {
      /* everything was accepted: the request bytes are the batch, unchanged */
      batch.payload = request->data;
      batch.payload_len = request->len;
    } else {
      otlp_filter_request(pb);
      size_t len = protobuf_c_message_get_packed_size(&pb->base);
      uint8_t *buf = otlp_arena_alloc(arena, len);
      if (buf == nullptr) {
        bool limit = otlp_arena_status_get(arena) == OTLP_ARENA_LIMIT;
        OTLP_STAT_ADD(st, too_large, limit ? 1 : 0);
        OTLP_STAT_ADD(st, throttled_memory, limit ? 0 : 1);
        fail(out, limit ? GRPC_STATUS_RESOURCE_EXHAUSTED : GRPC_STATUS_UNAVAILABLE, !limit,
             limit ? "accepted data exceeds the max decoded size" : "memory limit reached");
        return;
      }
      batch.payload = buf;
      batch.payload_len = protobuf_c_message_pack(&pb->base, buf);
    }
  }

  if (job != nullptr && atomic_load(&job->cancelled)) {
    OTLP_STAT_ADD(st, cancelled, 1);
    fail(out, GRPC_STATUS_CANCELLED, false, "cancelled");
    return;
  }
  if (timed_out(svc, arrival_ns)) {
    OTLP_STAT_ADD(st, deadline_exceeded, 1);
    fail(out, GRPC_STATUS_DEADLINE_EXCEEDED, false, "request timed out before reaching the sink");
    return;
  }

  otlp_sink_completion done = {
      .fn = on_sink_done, .ctx = job, .cancelled = job != nullptr ? &job->cancelled : nullptr};
  const char *detail = nullptr;
  otlp_sink_result result = otlp_sink_submit(svc->sink, &batch, &done, &detail);
  switch (result) {
  case OTLP_SINK_ACCEPTED:
    OTLP_STAT_ADD(st, payload_bytes_stored, batch.payload_len);
    return;
  case OTLP_SINK_PENDING:
    /* only sinks with OTLP_SINK_MAY_DEFER defer, and their requests always have a job */
    job->response = out->response;
    job->points = out->points;
    job->accepted = out->accepted;
    job->rejected = out->rejected;
    out->response = nullptr;
    out->pending = true;
    OTLP_STAT_ADD(st, payload_bytes_stored, batch.payload_len);
    return;
  case OTLP_SINK_RETRYABLE:
    OTLP_STAT_ADD(st, sink_retryable, 1);
    (void)snprintf(out->text, sizeof(out->text), "sink unavailable: %s",
                   detail != nullptr ? detail : "temporary failure");
    fail(out, GRPC_STATUS_UNAVAILABLE, true, out->text);
    return;
  case OTLP_SINK_FAILED:
  default:
    OTLP_STAT_ADD(st, sink_failed, 1);
    (void)snprintf(out->text, sizeof(out->text), "sink failed: %s",
                   detail != nullptr ? detail : "permanent failure");
    fail(out, GRPC_STATUS_INTERNAL, false, out->text);
    return;
  }
}

/* Ends a job: answers its call, releases its reservation and its call reference. The job (freed
 * with the call) must not be touched afterwards. */
static void finish_job(otlp_job *job, outcome *out, bool async) {
  otlp_service *svc = job->svc;
  grpc_call *call = job->call;
  answer(svc, call, out, async);
  if (job->response != nullptr) {
    grpc_message_free(job->response);
    job->response = nullptr;
  }
  otlp_budget_release(svc->budget, job->reserved);
  atomic_fetch_sub(&svc->in_flight, 1);
  grpc_call_unref(call);
}

static void run_job(otlp_service *svc, otlp_arena *arena, otlp_job *job, bool async) {
  outcome out;
  process(svc, arena, job, job->request, job->arrival_ns, &out);
  otlp_arena_end(arena);
  if (!out.pending)
    finish_job(job, &out, async);
}

/* Sink completion of a deferred batch: any thread. */
static void on_sink_done(void *ctx, otlp_sink_result result, const char *detail) {
  otlp_job *job = ctx;
  otlp_service *svc = job->svc;
  outcome out = {
      .status = GRPC_STATUS_OK,
      .points = job->points,
      .accepted = job->accepted,
      .rejected = job->rejected,
  };
  if (result != OTLP_SINK_ACCEPTED && atomic_load(&job->cancelled)) {
    /* the client is gone; the sink dropped the batch because of that */
    OTLP_STAT_ADD(&svc->stats, cancelled, 1);
    fail(&out, GRPC_STATUS_CANCELLED, false, "cancelled");
    finish_job(job, &out, true);
    return;
  }
  switch (result) {
  case OTLP_SINK_ACCEPTED:
    out.response = job->response;
    job->response = nullptr;
    break;
  case OTLP_SINK_RETRYABLE:
    OTLP_STAT_ADD(&svc->stats, sink_retryable, 1);
    (void)snprintf(out.text, sizeof(out.text), "sink unavailable: %s",
                   detail != nullptr ? detail : "temporary failure");
    fail(&out, GRPC_STATUS_UNAVAILABLE, true, out.text);
    break;
  default:
    OTLP_STAT_ADD(&svc->stats, sink_failed, 1);
    (void)snprintf(out.text, sizeof(out.text), "sink failed: %s",
                   detail != nullptr ? detail : "permanent failure");
    fail(&out, GRPC_STATUS_INTERNAL, false, out.text);
    break;
  }
  finish_job(job, &out, true);
}

static void *worker_main(void *arg) {
  otlp_worker *w = arg;
  otlp_job *job;
  while ((job = otlp_queue_pop(&w->svc->queue)) != nullptr)
    run_job(w->svc, &w->arena, job, true);
  return nullptr;
}

/* ---- handlers ----------------------------------------------------------------------------- */

static void on_job_cancel(grpc_call *call, void *userdata) {
  (void)call;
  otlp_job *job = userdata;
  atomic_store(&job->cancelled, true);
}

static void count_encoding(otlp_service *svc, grpc_call *call) {
  const char *encoding = grpc_call_metadata(call, "grpc-encoding", nullptr);
  if (encoding == nullptr || strcmp(encoding, "identity") == 0)
    OTLP_STAT_ADD(&svc->stats, encoding_identity, 1);
  else if (strcmp(encoding, "gzip") == 0)
    OTLP_STAT_ADD(&svc->stats, encoding_gzip, 1);
  else
    OTLP_STAT_ADD(&svc->stats, encoding_other, 1);
}

static void refuse(otlp_service *svc, grpc_call *call, grpc_status status, bool retry,
                   const char *message) {
  outcome out = {.status = GRPC_STATUS_OK};
  fail(&out, status, retry, message);
  answer(svc, call, &out, false);
}

static void export_call(otlp_loop *loop, grpc_call *call, const grpc_message *request);

static void on_export(grpc_call *call, const grpc_message *request, void *userdata) {
  otlp_loop *loop = userdata;
  export_call(loop, call, request);
  /* Wait calls may watch the request counters, which move before a call is answered */
  notify_waiters(loop->svc);
}

static void export_call(otlp_loop *loop, grpc_call *call, const grpc_message *request) {
  otlp_service *svc = loop->svc;
  uint64_t arrival = now_ns();
  OTLP_STAT_ADD(&svc->stats, requests, 1);
  OTLP_STAT_ADD(&svc->stats, bytes_received, request->len);
  count_encoding(svc, call);

  if (atomic_load(&svc->shutting_down)) {
    OTLP_STAT_ADD(&svc->stats, shutting_down, 1);
    refuse(svc, call, GRPC_STATUS_UNAVAILABLE, true, "shutting down");
    return;
  }

  bool deferred = svc->nb_workers > 0 || (svc->sink->flags & OTLP_SINK_MAY_DEFER) != 0;
  if (!deferred) {
    /* the common case: decode and store right here, nothing outlives this callback */
    outcome out;
    process(svc, &loop->arena, nullptr, request, arrival, &out);
    otlp_arena_end(&loop->arena);
    answer(svc, call, &out, false);
    return;
  }

  /* the call outlives this callback: its request stays in memory, so it is reserved */
  size_t reserve = request->len + sizeof(otlp_job);
  if (!otlp_budget_reserve(svc->budget, reserve)) {
    OTLP_STAT_ADD(&svc->stats, throttled_memory, 1);
    refuse(svc, call, GRPC_STATUS_UNAVAILABLE, true, "memory limit reached");
    return;
  }
  otlp_job *job = grpc_calloc(1, sizeof(*job));
  if (job == nullptr) {
    otlp_budget_release(svc->budget, reserve);
    OTLP_STAT_ADD(&svc->stats, throttled_memory, 1);
    refuse(svc, call, GRPC_STATUS_UNAVAILABLE, true, "out of memory");
    return;
  }
  job->svc = svc;
  job->request = request;
  job->arrival_ns = arrival;
  job->reserved = reserve;
  atomic_init(&job->cancelled, false);
  job->call = grpc_call_ref(call);
  grpc_call_set_userdata(call, job, grpc_free);
  grpc_call_on_cancel(call, on_job_cancel, job);
  atomic_fetch_add(&svc->in_flight, 1);

  if (svc->nb_workers > 0) {
    if (!otlp_queue_push(&svc->queue, job)) {
      OTLP_STAT_ADD(&svc->stats, throttled_queue, 1);
      outcome out = {.status = GRPC_STATUS_OK};
      fail(&out, GRPC_STATUS_UNAVAILABLE, true, "receiver queue is full");
      finish_job(job, &out, false);
    }
    return;
  }
  run_job(svc, &loop->arena, job, false);
}

/* ---- admin -------------------------------------------------------------------------------- */

static void on_admin_stats(grpc_call *call, const grpc_message *request, void *userdata) {
  (void)request;
  otlp_loop *loop = userdata;
  grpc_message *msg = stats_message(loop->svc);
  if (msg == nullptr) {
    (void)grpc_call_finish(call, GRPC_STATUS_RESOURCE_EXHAUSTED, "out of memory");
    return;
  }
  (void)grpc_call_reply(call, msg);
}

/* The Wait call ended (deadline, client gone): drop it unless a notification already did. */
static void on_waiter_cancel(grpc_call *call, void *userdata) {
  waiter *w = userdata; /* alive: it is freed with the call */
  otlp_service *svc = w->svc;
  pthread_mutex_lock(&svc->waiters_lock);
  bool listed = w->listed;
  if (listed)
    unlink_waiter(svc, w);
  pthread_mutex_unlock(&svc->waiters_lock);
  if (listed)
    grpc_call_unref(call);
}

/* Body: "<counter> <threshold>", e.g. "points_accepted 120". Answers with the stats document once
 * the counter reaches the threshold; the client's deadline bounds the wait. */
static void on_admin_wait(grpc_call *call, const grpc_message *request, void *userdata) {
  otlp_loop *loop = userdata;
  otlp_service *svc = loop->svc;
  char text[96];
  size_t len = request->len < sizeof(text) - 1 ? request->len : sizeof(text) - 1;
  memcpy(text, request->data, len);
  text[len] = '\0';
  char counter[48];
  unsigned long long threshold;
  otlp_stats_snapshot snap;
  otlp_stats_snapshot_get(&svc->stats, &snap);
  uint64_t value;
  if (sscanf(text, "%47s %llu", counter, &threshold) != 2 ||
      !otlp_stats_lookup(&snap, counter, &value)) {
    (void)grpc_call_finish(call, GRPC_STATUS_INVALID_ARGUMENT,
                           "expected \"<counter> <threshold>\" with a known counter");
    return;
  }
  if (value >= threshold) {
    grpc_message *msg = stats_message(svc);
    if (msg != nullptr)
      (void)grpc_call_reply(call, msg);
    else
      (void)grpc_call_finish(call, GRPC_STATUS_RESOURCE_EXHAUSTED, "out of memory");
    return;
  }
  waiter *w = grpc_calloc(1, sizeof(*w));
  if (w == nullptr) {
    (void)grpc_call_finish(call, GRPC_STATUS_RESOURCE_EXHAUSTED, "out of memory");
    return;
  }
  w->svc = svc;
  w->threshold = threshold;
  memcpy(w->counter, counter, sizeof(w->counter));
  w->call = grpc_call_ref(call);
  grpc_call_set_userdata(call, w, grpc_free);
  grpc_call_on_cancel(call, on_waiter_cancel, w);
  pthread_mutex_lock(&svc->waiters_lock);
  w->listed = true;
  w->next = svc->waiters;
  if (svc->waiters != nullptr)
    svc->waiters->prev = w;
  svc->waiters = w;
  atomic_fetch_add(&svc->nb_waiters, 1);
  pthread_mutex_unlock(&svc->waiters_lock);
  /* the counter may have moved between the check and the registration */
  notify_waiters(svc);
}

static void on_admin_dump(grpc_call *call, const grpc_message *request, void *userdata) {
  (void)request;
  otlp_loop *loop = userdata;
  uint8_t *data = nullptr;
  size_t len = 0;
  if (!otlp_memory_sink_dump(loop->svc->sink, MAX_DUMP_BYTES, &data, &len, nullptr)) {
    (void)grpc_call_finish(call, GRPC_STATUS_FAILED_PRECONDITION,
                           "dump needs the memory sink and at most 64 MiB stored");
    return;
  }
  (void)grpc_call_reply_data(call, data, len);
  grpc_free(data);
}

static void on_admin_clear(grpc_call *call, const grpc_message *request, void *userdata) {
  (void)request;
  otlp_loop *loop = userdata;
  otlp_memory_sink_clear(loop->svc->sink);
  (void)grpc_call_reply_data(call, nullptr, 0);
}

/* Body: "open" or "close". */
static void on_admin_gate(grpc_call *call, const grpc_message *request, void *userdata) {
  otlp_loop *loop = userdata;
  otlp_sink *sink = loop->svc->sink;
  if (otlp_sink_unwrap(sink) == sink) {
    (void)grpc_call_finish(call, GRPC_STATUS_FAILED_PRECONDITION, "fault injection is off");
    return;
  }
  bool open = request->len == 4 && memcmp(request->data, "open", 4) == 0;
  bool close = request->len == 5 && memcmp(request->data, "close", 5) == 0;
  if (!open && !close) {
    (void)grpc_call_finish(call, GRPC_STATUS_INVALID_ARGUMENT, "expected \"open\" or \"close\"");
    return;
  }
  otlp_fault_sink_set_gate(sink, open);
  (void)grpc_call_reply_data(call, nullptr, 0);
}

/* ---- lifecycle ---------------------------------------------------------------------------- */

static bool token_valid(const char *token, void *userdata) {
  const otlp_service *svc = userdata;
  size_t len = strlen(token);
  /* constant time in the token's content */
  unsigned char diff = len != svc->auth_token_len;
  for (size_t i = 0; i < svc->auth_token_len; i++)
    diff |= (unsigned char)(svc->auth_token[i] ^ (i < len ? token[i] : 0));
  return diff == 0;
}

otlp_service *otlp_service_new(const otlp_service_config *config, otlp_sink *sink,
                               otlp_budget *budget) {
  otlp_service *svc = grpc_calloc(1, sizeof(*svc));
  if (svc == nullptr)
    return nullptr;
  svc->config = *config;
  svc->config.auth_token = nullptr;
  svc->sink = sink;
  svc->budget = budget;
  otlp_stats_init(&svc->stats);
  atomic_init(&svc->shutting_down, false);
  atomic_init(&svc->decoded_peak, 0);
  atomic_init(&svc->in_flight, 0);
  atomic_init(&svc->nb_waiters, 0);
  pthread_mutex_init(&svc->waiters_lock, nullptr);
  if (config->auth_token != nullptr && config->auth_token[0] != '\0') {
    svc->auth_token = grpc_strdup(config->auth_token);
    if (svc->auth_token == nullptr)
      goto fail;
    svc->auth_token_len = strlen(svc->auth_token);
  }
  if (config->workers > 0) {
    if (!otlp_queue_init(&svc->queue, config->queue_capacity))
      goto fail;
    svc->workers = grpc_calloc(config->workers, sizeof(otlp_worker));
    if (svc->workers == nullptr)
      goto fail;
    for (unsigned i = 0; i < config->workers; i++) {
      otlp_worker *w = &svc->workers[i];
      w->svc = svc;
      otlp_arena_init(&w->arena, budget, config->arena_keep_bytes);
      if (pthread_create(&w->thread, nullptr, worker_main, w) != 0)
        goto fail;
      svc->nb_workers++;
    }
  }
  return svc;
fail:
  if (svc->nb_workers > 0 || svc->queue.items != nullptr) {
    otlp_queue_close(&svc->queue);
    for (unsigned i = 0; i < svc->nb_workers; i++)
      pthread_join(svc->workers[i].thread, nullptr);
    otlp_queue_destroy(&svc->queue);
  }
  grpc_free(svc->workers);
  grpc_free(svc->auth_token);
  pthread_mutex_destroy(&svc->waiters_lock);
  grpc_free(svc);
  return nullptr;
}

static grpc_err add_auth(otlp_service *svc, grpc_server *server, const char *prefix) {
  grpc_interceptor auth = grpc_interceptor_bearer_auth(token_valid, svc);
  auth.path_prefix = prefix;
  grpc_err err = grpc_server_add_interceptor(server, &auth);
  if (err != GRPC_OK && auth.userdata_free != nullptr)
    auth.userdata_free(auth.userdata);
  return err;
}

grpc_err otlp_service_attach(otlp_service *svc, grpc_server *server) {
  otlp_loop *loop = grpc_calloc(1, sizeof(*loop));
  if (loop == nullptr)
    return GRPC_ERR_NOMEM;
  loop->svc = svc;
  loop->server = server;
  otlp_arena_init(&loop->arena, svc->budget, svc->config.arena_keep_bytes);
  loop->next = svc->loops;
  svc->loops = loop;

  grpc_err err = grpc_server_handle(server, OTLP_EXPORT_PATH, on_export, loop);
  if (err == GRPC_OK && svc->auth_token != nullptr) {
    err = add_auth(svc, server, EXPORT_PREFIX);
    if (err == GRPC_OK && svc->config.admin)
      err = add_auth(svc, server, ADMIN_PREFIX);
  }
  if (err == GRPC_OK && svc->config.admin) {
    static const struct {
      const char *path;
      grpc_handler_fn fn;
    } admin[] = {
        {ADMIN_PREFIX "Stats", on_admin_stats}, {ADMIN_PREFIX "Wait", on_admin_wait},
        {ADMIN_PREFIX "Dump", on_admin_dump},   {ADMIN_PREFIX "Clear", on_admin_clear},
        {ADMIN_PREFIX "Gate", on_admin_gate},
    };
    for (size_t i = 0; i < sizeof(admin) / sizeof(admin[0]) && err == GRPC_OK; i++)
      err = grpc_server_handle(server, admin[i].path, admin[i].fn, loop);
  }
  return err;
}

void otlp_service_begin_shutdown(otlp_service *svc) {
  atomic_store(&svc->shutting_down, true);
}

void otlp_service_stop(otlp_service *svc) {
  if (svc->stopped)
    return;
  svc->stopped = true;
  otlp_service_begin_shutdown(svc);
  if (svc->nb_workers > 0) {
    otlp_queue_close(&svc->queue); /* workers finish what is queued, then exit */
    for (unsigned i = 0; i < svc->nb_workers; i++)
      pthread_join(svc->workers[i].thread, nullptr);
  }
  otlp_sink_shutdown(svc->sink); /* completes the batches it still holds */

  pthread_mutex_lock(&svc->waiters_lock);
  while (svc->waiters != nullptr) {
    waiter *w = svc->waiters;
    grpc_call *call = w->call;
    unlink_waiter(svc, w);
    (void)grpc_call_finish_async(call, GRPC_STATUS_UNAVAILABLE, "shutting down");
    grpc_call_unref(call);
  }
  pthread_mutex_unlock(&svc->waiters_lock);
}

void otlp_service_free(otlp_service *svc) {
  if (svc == nullptr)
    return;
  otlp_service_stop(svc);
  for (unsigned i = 0; i < svc->nb_workers; i++)
    otlp_arena_destroy(&svc->workers[i].arena);
  if (svc->queue.items != nullptr)
    otlp_queue_destroy(&svc->queue);
  grpc_free(svc->workers);
  while (svc->loops != nullptr) {
    otlp_loop *next = svc->loops->next;
    otlp_arena_destroy(&svc->loops->arena);
    grpc_free(svc->loops);
    svc->loops = next;
  }
  otlp_sink_destroy(svc->sink);
  grpc_free(svc->auth_token);
  pthread_mutex_destroy(&svc->waiters_lock);
  grpc_free(svc);
}

otlp_stats *otlp_service_stats(otlp_service *svc) {
  return &svc->stats;
}

otlp_sink *otlp_service_sink(otlp_service *svc) {
  return svc->sink;
}

otlp_budget *otlp_service_budget(otlp_service *svc) {
  return svc->budget;
}

void otlp_service_stats_json(otlp_service *svc, otlp_strbuf *out) {
  otlp_stats_snapshot snap;
  otlp_stats_snapshot_get(&svc->stats, &snap);
  otlp_strbuf_printf(out, "{\"stats\":{");
  otlp_stats_json(&snap, out);
  size_t depth = 0, peak = 0;
  if (svc->nb_workers > 0)
    otlp_queue_depth(&svc->queue, &depth, &peak);
  otlp_strbuf_printf(out,
                     "},\"in_flight\":%" PRIu64 ",\"decoded_bytes_peak\":%zu"
                     ",\"memory\":{\"limit\":%zu,\"used\":%zu,\"peak\":%zu,\"denied\":%" PRIu64 "}"
                     ",\"queue\":{\"workers\":%u,\"capacity\":%zu,\"depth\":%zu,\"peak\":%zu}"
                     ",\"sink\":{\"name\":\"%s\"",
                     (uint64_t)atomic_load(&svc->in_flight), atomic_load(&svc->decoded_peak),
                     svc->budget->limit, otlp_budget_used(svc->budget),
                     atomic_load(&svc->budget->peak), (uint64_t)atomic_load(&svc->budget->denied),
                     svc->nb_workers, svc->nb_workers > 0 ? svc->config.queue_capacity : 0, depth,
                     peak, svc->sink->ops->name);
  if (svc->sink->ops->stats_json != nullptr) {
    otlp_strbuf_printf(out, ",");
    svc->sink->ops->stats_json(svc->sink, out);
  }
  otlp_strbuf_printf(out, "}}");
}
