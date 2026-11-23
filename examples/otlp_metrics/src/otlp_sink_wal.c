/* otlp_sink_wal.c - the write-ahead-log sink; see otlp_wal.h */
#include "otlp_threads.h"
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <uv.h>

#include "grpc_mem.h"
#include "otlp_wal.h"
#include "otlp_wal_format.h"

typedef enum entry_state {
  ENTRY_QUEUED = 0,
  ENTRY_WRITTEN,
  ENTRY_CANCELLED,
  ENTRY_FULL,
  ENTRY_FAILED,
} entry_state;

typedef struct wal_entry wal_entry;
struct wal_entry {
  wal_entry *next;
  otlp_sink_completion done;
  uint64_t points;
  size_t len;
  size_t reserved;
  entry_state state;
  uint8_t data[];
};

typedef struct wal_sink {
  otlp_sink base;
  otlp_wal_config cfg;
  char *dir;
  otlp_budget *budget;

  pthread_mutex_t lock;
  pthread_cond_t wake;
  /* guarded by lock: */
  wal_entry *head, *tail;
  size_t queued_bytes;
  bool quiesced; /* no new batches */
  bool closing;  /* the writer exits once the queue is empty */
  pthread_t thread;
  bool thread_started;

  /* the writer thread's (and, before it starts, the constructor's): */
  uv_loop_t loop;
  uv_file fd;
  uint64_t segment_written; /* bytes in the current segment */
  uint64_t next_sequence;
  uint64_t writes; /* for fail_writes_every */
  char path[1024];

  /* readable from any thread: */
  atomic_uint_least64_t total_bytes; /* all segments, recovered ones included */
  atomic_uint_least64_t records;
  atomic_uint_least64_t points;
  atomic_uint_least64_t segments;
  atomic_uint_least64_t groups;
  atomic_uint_least64_t max_group;
  atomic_uint_least64_t syncs;
  atomic_uint_least64_t sync_ns;
  atomic_uint_least64_t write_errors;
  atomic_uint_least64_t refused_full;
  atomic_uint_least64_t refused_queue;
  atomic_uint_least64_t skipped_cancelled;
  uint64_t recovered_records; /* set before the writer starts */
  uint64_t recovered_segments;
  uint64_t truncated_bytes;
  uint64_t damaged_segments;
} wal_sink;

/* ---- file helpers (writer thread, or constructor) -------------------------------------------- */

static int fs_result(uv_fs_t *req, int r) {
  uv_fs_req_cleanup(req);
  return r;
}

static int write_all(wal_sink *w, uv_buf_t *bufs, unsigned nbufs, uint64_t offset) {
  while (nbufs > 0) {
    uv_fs_t req;
    int n =
        fs_result(&req, uv_fs_write(&w->loop, &req, w->fd, bufs, nbufs, (int64_t)offset, nullptr));
    if (n < 0)
      return n;
    if (n == 0)
      return UV_EIO;
    offset += (uint64_t)n;
    size_t left = (size_t)n;
    while (nbufs > 0 && left >= bufs->len) {
      left -= bufs->len;
      bufs++;
      nbufs--;
    }
    if (nbufs > 0) {
      bufs->base += left;
      bufs->len -= (unsigned)left;
    }
  }
  return 0;
}

static int sync_file(wal_sink *w) {
  uint64_t t0 = uv_hrtime();
  uv_fs_t req;
  int r = fs_result(&req, uv_fs_fdatasync(&w->loop, &req, w->fd, nullptr));
  atomic_fetch_add(&w->syncs, 1);
  atomic_fetch_add(&w->sync_ns, uv_hrtime() - t0);
  return r;
}

/* Makes a new directory entry durable (POSIX; Windows has no directory handles to sync). */
static void sync_dir(wal_sink *w) {
#ifndef _WIN32
  uv_fs_t req;
  int fd = fs_result(&req, uv_fs_open(&w->loop, &req, w->dir, UV_FS_O_RDONLY, 0, nullptr));
  if (fd >= 0) {
    (void)fs_result(&req, uv_fs_fsync(&w->loop, &req, fd, nullptr));
    (void)fs_result(&req, uv_fs_close(&w->loop, &req, fd, nullptr));
  }
#else
  (void)w;
#endif
}

static void close_segment(wal_sink *w) {
  if (w->fd < 0)
    return;
  if (w->cfg.sync == OTLP_WAL_SYNC_FSYNC)
    (void)sync_file(w);
  uv_fs_t req;
  (void)fs_result(&req, uv_fs_close(&w->loop, &req, w->fd, nullptr));
  w->fd = -1;
}

/* Starts segment "<next_sequence>.wal". Returns 0 or a libuv error. */
static int open_segment(wal_sink *w) {
  char name[32];
  otlp_wal_segment_name(name, w->next_sequence);
  int len = snprintf(w->path, sizeof(w->path), "%s/%s", w->dir, name);
  if (len < 0 || (size_t)len >= sizeof(w->path))
    return UV_ENAMETOOLONG;
  uv_fs_t req;
  int fd =
      fs_result(&req, uv_fs_open(&w->loop, &req, w->path,
                                 UV_FS_O_WRONLY | UV_FS_O_CREAT | UV_FS_O_EXCL, 0644, nullptr));
  if (fd < 0)
    return fd;
  w->fd = fd;
  uint8_t header[OTLP_WAL_SEGMENT_HEADER];
  otlp_wal_segment_header(header);
  uv_buf_t buf = uv_buf_init((char *)header, sizeof(header));
  int r = write_all(w, &buf, 1, 0);
  if (r == 0 && w->cfg.sync == OTLP_WAL_SYNC_FSYNC) {
    r = sync_file(w);
    sync_dir(w);
  }
  if (r != 0) {
    close_segment(w);
    (void)fs_result(&req, uv_fs_unlink(&w->loop, &req, w->path, nullptr));
    return r;
  }
  w->segment_written = OTLP_WAL_SEGMENT_HEADER;
  atomic_fetch_add(&w->total_bytes, OTLP_WAL_SEGMENT_HEADER);
  atomic_fetch_add(&w->segments, 1);
  return 0;
}

/* ---- the writer ------------------------------------------------------------------------------- */

/* Truncates the current segment back to `offset` after a failed write or sync. Returns false
 * when that failed: the writer then continues in a fresh segment, leaving the torn tail for
 * readers to stop at (as after a crash). */
static bool truncate_to(wal_sink *w, uint64_t offset) {
  uv_fs_t req;
  if (fs_result(&req, uv_fs_ftruncate(&w->loop, &req, w->fd, (int64_t)offset, nullptr)) == 0)
    return true;
  close_segment(w);
  (void)open_segment(w);
  return false;
}

/* Writes one group of entries, syncs, and marks each entry's state. */
static void write_group(wal_sink *w, wal_entry *list) {
  if (w->fd >= 0 && w->segment_written >= w->cfg.segment_bytes)
    close_segment(w); /* rotate between groups */
  if (w->fd < 0 && open_segment(w) != 0)
    atomic_fetch_add(&w->write_errors, 1); /* retried with the next group */
  uint64_t group_start = w->segment_written;
  uint64_t group_bytes = 0;
  uint64_t written = 0;
  bool failed = w->fd < 0;
  for (wal_entry *e = list; e != nullptr; e = e->next) {
    if (e->done.cancelled != nullptr && atomic_load(e->done.cancelled)) {
      e->state = ENTRY_CANCELLED;
      continue;
    }
    if (failed) {
      e->state = ENTRY_FAILED;
      continue;
    }
    uint64_t rec = OTLP_WAL_RECORD_HEADER + e->len;
    if (atomic_load(&w->total_bytes) + rec > w->cfg.max_bytes) {
      e->state = ENTRY_FULL;
      continue;
    }
    uint8_t header[OTLP_WAL_RECORD_HEADER];
    otlp_wal_record_header(header, w->next_sequence, e->points, e->data, (uint32_t)e->len);
    uv_buf_t bufs[2] = {uv_buf_init((char *)header, sizeof(header)),
                        uv_buf_init((char *)e->data, (unsigned)e->len)};
    int r;
    w->writes++;
    if (w->cfg.fail_writes_every != 0 && w->writes % w->cfg.fail_writes_every == 0) {
      /* injected fault: part of the record reaches the file, then the write "fails" */
      uv_buf_t half = uv_buf_init((char *)header, sizeof(header) / 2);
      (void)write_all(w, &half, 1, w->segment_written);
      r = UV_EIO;
    } else {
      r = write_all(w, bufs, 2, w->segment_written);
    }
    if (r != 0) {
      atomic_fetch_add(&w->write_errors, 1);
      truncate_to(w, w->segment_written);
      e->state = ENTRY_FAILED;
      failed = true;
      continue;
    }
    w->next_sequence++;
    w->segment_written += rec;
    group_bytes += rec;
    atomic_fetch_add(&w->total_bytes, rec);
    e->state = ENTRY_WRITTEN;
    written++;
  }
  if (written > 0 && w->cfg.sync == OTLP_WAL_SYNC_FSYNC && sync_file(w) != 0) {
    /* durability of the whole group is unknown: drop it from the log and fail it, so a retried
     * batch does not appear twice */
    atomic_fetch_add(&w->write_errors, 1);
    if (truncate_to(w, group_start)) {
      w->segment_written = group_start;
      atomic_fetch_sub(&w->total_bytes, group_bytes);
    }
    for (wal_entry *e = list; e != nullptr; e = e->next) {
      if (e->state == ENTRY_WRITTEN)
        e->state = ENTRY_FAILED;
    }
    written = 0;
  }
  if (written > 0) {
    atomic_fetch_add(&w->records, written);
    atomic_fetch_add(&w->groups, 1);
    uint64_t max = atomic_load(&w->max_group);
    while (written > max && !atomic_compare_exchange_weak(&w->max_group, &max, written)) {
    }
  }
}

/* Completes and frees a written group. */
static void complete_group(wal_sink *w, wal_entry *list) {
  while (list != nullptr) {
    wal_entry *next = list->next;
    otlp_sink_result result = OTLP_SINK_RETRYABLE;
    const char *detail = nullptr;
    switch (list->state) {
    case ENTRY_WRITTEN:
      result = OTLP_SINK_ACCEPTED;
      atomic_fetch_add(&w->points, list->points);
      break;
    case ENTRY_CANCELLED:
      atomic_fetch_add(&w->skipped_cancelled, 1);
      detail = "cancelled";
      break;
    case ENTRY_FULL:
      atomic_fetch_add(&w->refused_full, 1);
      detail = "write-ahead log is full";
      break;
    default:
      detail = "write-ahead log write failed";
      break;
    }
    otlp_sink_completion done = list->done;
    if (w->budget != nullptr && list->reserved != 0)
      otlp_budget_release(w->budget, list->reserved);
    grpc_free(list);
    done.fn(done.ctx, result, detail);
    list = next;
  }
}

static void *writer_main(void *arg) {
  wal_sink *w = arg;
  pthread_mutex_lock(&w->lock);
  for (;;) {
    while (w->head == nullptr && !w->closing)
      pthread_cond_wait(&w->wake, &w->lock);
    if (w->head == nullptr)
      break; /* closing and drained */
    wal_entry *list = w->head;
    w->head = w->tail = nullptr;
    for (wal_entry *e = list; e != nullptr; e = e->next)
      w->queued_bytes -= e->len;
    pthread_mutex_unlock(&w->lock);
    write_group(w, list);
    complete_group(w, list);
    pthread_mutex_lock(&w->lock);
  }
  pthread_mutex_unlock(&w->lock);
  return nullptr;
}

/* ---- sink operations ---------------------------------------------------------------------------- */

static otlp_sink_result wal_submit(otlp_sink *sink, const otlp_batch *batch,
                                   const otlp_sink_completion *done, const char **detail) {
  wal_sink *w = (wal_sink *)sink;
  size_t len = batch->payload_len;
  if (len > OTLP_WAL_MAX_PAYLOAD) {
    *detail = "batch larger than a write-ahead log record";
    return OTLP_SINK_FAILED;
  }
  uint64_t rec = OTLP_WAL_RECORD_HEADER + len;
  pthread_mutex_lock(&w->lock);
  bool closed = w->quiesced || w->closing;
  bool queue_full = len > w->cfg.queue_bytes - w->queued_bytes;
  bool log_full = atomic_load(&w->total_bytes) + w->queued_bytes + rec > w->cfg.max_bytes;
  pthread_mutex_unlock(&w->lock);
  if (closed) {
    *detail = "shutting down";
    return OTLP_SINK_RETRYABLE;
  }
  if (log_full) {
    atomic_fetch_add(&w->refused_full, 1);
    *detail = "write-ahead log is full";
    return OTLP_SINK_RETRYABLE;
  }
  if (queue_full) {
    atomic_fetch_add(&w->refused_queue, 1);
    *detail = "write-ahead log queue is full";
    return OTLP_SINK_RETRYABLE;
  }
  size_t reserved = sizeof(wal_entry) + len;
  if (w->budget != nullptr && !otlp_budget_reserve(w->budget, reserved)) {
    *detail = "memory limit reached";
    return OTLP_SINK_RETRYABLE;
  }
  wal_entry *e = grpc_malloc(sizeof(*e) + len);
  if (e == nullptr) {
    if (w->budget != nullptr)
      otlp_budget_release(w->budget, reserved);
    *detail = "out of memory";
    return OTLP_SINK_RETRYABLE;
  }
  *e = (wal_entry){.done = *done, .points = batch->points, .len = len, .reserved = reserved};
  if (len > 0)
    memcpy(e->data, batch->payload, len);

  pthread_mutex_lock(&w->lock);
  if (w->quiesced || w->closing || len > w->cfg.queue_bytes - w->queued_bytes) {
    bool now_closed = w->quiesced || w->closing;
    pthread_mutex_unlock(&w->lock);
    if (w->budget != nullptr)
      otlp_budget_release(w->budget, reserved);
    grpc_free(e);
    if (!now_closed)
      atomic_fetch_add(&w->refused_queue, 1);
    *detail = now_closed ? "shutting down" : "write-ahead log queue is full";
    return OTLP_SINK_RETRYABLE;
  }
  if (w->tail != nullptr)
    w->tail->next = e;
  else
    w->head = e;
  w->tail = e;
  w->queued_bytes += len;
  pthread_cond_signal(&w->wake);
  pthread_mutex_unlock(&w->lock);
  return OTLP_SINK_PENDING;
}

static void wal_stats(otlp_sink *sink, otlp_strbuf *out) {
  wal_sink *w = (wal_sink *)sink;
  pthread_mutex_lock(&w->lock);
  size_t queued = w->queued_bytes;
  pthread_mutex_unlock(&w->lock);
  otlp_strbuf_printf(out, "\"dir\":");
  otlp_strbuf_json_string(out, w->dir);
  otlp_strbuf_printf(
      out,
      ",\"sync\":\"%s\",\"records\":%" PRIu64 ",\"points\":%" PRIu64 ",\"bytes\":%" PRIu64
      ",\"segments\":%" PRIu64 ",\"groups\":%" PRIu64 ",\"max_group\":%" PRIu64
      ",\"syncs\":%" PRIu64 ",\"sync_ns\":%" PRIu64 ",\"write_errors\":%" PRIu64
      ",\"refused_full\":%" PRIu64 ",\"refused_queue\":%" PRIu64 ",\"skipped_cancelled\":%" PRIu64
      ",\"queued_bytes\":%zu,\"recovered_records\":%" PRIu64 ",\"recovered_segments\":%" PRIu64
      ",\"truncated_bytes\":%" PRIu64 ",\"damaged_segments\":%" PRIu64,
      w->cfg.sync == OTLP_WAL_SYNC_FSYNC ? "fsync" : "none", (uint64_t)atomic_load(&w->records),
      (uint64_t)atomic_load(&w->points), (uint64_t)atomic_load(&w->total_bytes),
      (uint64_t)atomic_load(&w->segments), (uint64_t)atomic_load(&w->groups),
      (uint64_t)atomic_load(&w->max_group), (uint64_t)atomic_load(&w->syncs),
      (uint64_t)atomic_load(&w->sync_ns), (uint64_t)atomic_load(&w->write_errors),
      (uint64_t)atomic_load(&w->refused_full), (uint64_t)atomic_load(&w->refused_queue),
      (uint64_t)atomic_load(&w->skipped_cancelled), queued, w->recovered_records,
      w->recovered_segments, w->truncated_bytes, w->damaged_segments);
}

static void wal_quiesce(otlp_sink *sink) {
  wal_sink *w = (wal_sink *)sink;
  pthread_mutex_lock(&w->lock);
  w->quiesced = true; /* queued batches are still written */
  pthread_mutex_unlock(&w->lock);
}

static void wal_shutdown(otlp_sink *sink) {
  wal_sink *w = (wal_sink *)sink;
  pthread_mutex_lock(&w->lock);
  w->quiesced = true;
  w->closing = true;
  pthread_cond_signal(&w->wake);
  bool started = w->thread_started;
  w->thread_started = false;
  pthread_mutex_unlock(&w->lock);
  if (started)
    pthread_join(w->thread, nullptr);
  close_segment(w);
}

static void wal_destroy(otlp_sink *sink) {
  wal_sink *w = (wal_sink *)sink;
  wal_shutdown(sink);
  uv_loop_close(&w->loop);
  otlp_cond_destroy(&w->wake);
  pthread_mutex_destroy(&w->lock);
  grpc_free(w->dir);
  grpc_free(w);
}

static const otlp_sink_ops wal_ops = {
    .name = "wal",
    .submit = wal_submit,
    .stats_json = wal_stats,
    .quiesce = wal_quiesce,
    .shutdown = wal_shutdown,
    .destroy = wal_destroy,
};

/* ---- recovery ------------------------------------------------------------------------------------ */

static bool recover(wal_sink *w) {
  char **names;
  long count = otlp_wal_list_segments(w->dir, &names);
  if (count < 0) {
    fprintf(stderr, "otlp_metrics: cannot list the write-ahead log %s\n", w->dir);
    return false;
  }
  uint64_t next = 1;
  for (long i = 0; i < count; i++) {
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s", w->dir, names[i]);
    uint64_t first;
    (void)otlp_wal_segment_name_parse(names[i], &first);
    otlp_wal_scan scan;
    otlp_wal_scan_segment(path, nullptr, nullptr, &scan);
    bool last = i == count - 1;
    if (scan.end == OTLP_WAL_SCAN_IO_ERROR) {
      fprintf(stderr, "otlp_metrics: cannot read %s\n", path);
      otlp_wal_list_free(names, count);
      return false;
    }
    if (scan.end == OTLP_WAL_SCAN_BAD_HEADER && last &&
        scan.file_bytes <= OTLP_WAL_SEGMENT_HEADER) {
      /* created right before a crash, header incomplete: nothing in it */
      uv_fs_t req;
      (void)fs_result(&req, uv_fs_unlink(&w->loop, &req, path, nullptr));
      w->truncated_bytes += scan.file_bytes;
      continue;
    }
    if (scan.end == OTLP_WAL_SCAN_TORN && last) {
      uv_fs_t req;
      int fd = fs_result(&req, uv_fs_open(&w->loop, &req, path, UV_FS_O_WRONLY, 0, nullptr));
      int r = fd < 0 ? fd
                     : fs_result(&req, uv_fs_ftruncate(&w->loop, &req, fd,
                                                       (int64_t)scan.valid_bytes, nullptr));
      if (fd >= 0) {
        if (r == 0 && w->cfg.sync == OTLP_WAL_SYNC_FSYNC)
          (void)fs_result(&req, uv_fs_fsync(&w->loop, &req, fd, nullptr));
        (void)fs_result(&req, uv_fs_close(&w->loop, &req, fd, nullptr));
      }
      if (r != 0) {
        fprintf(stderr, "otlp_metrics: cannot truncate the torn tail of %s\n", path);
        otlp_wal_list_free(names, count);
        return false;
      }
      w->truncated_bytes += scan.file_bytes - scan.valid_bytes;
      scan.end = OTLP_WAL_SCAN_CLEAN;
    }
    if (scan.end == OTLP_WAL_SCAN_CLEAN && scan.records == 0) {
      /* a segment that never got a record (the process stopped right after starting it) */
      uv_fs_t req;
      (void)fs_result(&req, uv_fs_unlink(&w->loop, &req, path, nullptr));
      if (first > next)
        next = first;
      continue;
    }
    if (scan.end != OTLP_WAL_SCAN_CLEAN) {
      /* damage inside the log (not at its end) is left for the operator; the valid prefix of
       * the segment still counts */
      fprintf(stderr, "otlp_metrics: %s is damaged after %" PRIu64 " valid bytes\n", path,
              scan.valid_bytes);
      w->damaged_segments++;
    }
    w->recovered_segments++;
    w->recovered_records += scan.records;
    atomic_fetch_add(&w->total_bytes,
                     scan.end == OTLP_WAL_SCAN_CLEAN ? scan.valid_bytes : scan.file_bytes);
    uint64_t after = scan.records > 0 ? scan.last_sequence + 1 : first;
    if (after > next)
      next = after;
  }
  otlp_wal_list_free(names, count);
  w->next_sequence = next;
  return true;
}

otlp_sink *otlp_wal_sink_new(const otlp_wal_config *config, otlp_budget *budget) {
  wal_sink *w = grpc_calloc(1, sizeof(*w));
  if (w == nullptr)
    return nullptr;
  w->base = (otlp_sink){.ops = &wal_ops, .flags = OTLP_SINK_NEEDS_PAYLOAD | OTLP_SINK_MAY_DEFER};
  w->cfg = *config;
  w->budget = budget;
  w->fd = -1;
  if (w->cfg.segment_bytes == 0)
    w->cfg.segment_bytes = (size_t)64 << 20;
  if (w->cfg.max_bytes == 0)
    w->cfg.max_bytes = SIZE_MAX;
  if (w->cfg.queue_bytes == 0)
    w->cfg.queue_bytes = (size_t)64 << 20;
  w->dir = grpc_strdup(config->dir != nullptr ? config->dir : ".");
  pthread_mutex_init(&w->lock, nullptr);
  otlp_cond_init(&w->wake);
  if (w->dir == nullptr || uv_loop_init(&w->loop) != 0) {
    otlp_cond_destroy(&w->wake);
    pthread_mutex_destroy(&w->lock);
    grpc_free(w->dir);
    grpc_free(w);
    return nullptr;
  }
  uv_fs_t req;
  int r = fs_result(&req, uv_fs_mkdir(&w->loop, &req, w->dir, 0755, nullptr));
  if (r != 0 && r != UV_EEXIST) {
    fprintf(stderr, "otlp_metrics: cannot create %s: %s\n", w->dir, uv_strerror(r));
    wal_destroy(&w->base);
    return nullptr;
  }
  if (!recover(w)) {
    wal_destroy(&w->base);
    return nullptr;
  }
  r = open_segment(w);
  if (r != 0) {
    fprintf(stderr, "otlp_metrics: cannot start a segment in %s: %s\n", w->dir, uv_strerror(r));
    wal_destroy(&w->base);
    return nullptr;
  }
  if (pthread_create(&w->thread, nullptr, writer_main, w) != 0) {
    wal_destroy(&w->base);
    return nullptr;
  }
  w->thread_started = true;
  return &w->base;
}
