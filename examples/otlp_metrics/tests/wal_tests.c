/* wal_tests.c - the write-ahead-log format and sink (built with -DOTLP_METRICS_WAL=ON) */
#include "otlp_threads.h"
#include <stdio.h>
#include <string.h>
#include <uv.h>

#include "test.h"

#include "grpc_mem.h"
#include "otlp_wal.h"
#include "otlp_wal_format.h"

static char g_dir[512];
static uv_loop_t g_loop;

/* Completion tracking: the writer thread completes batches; the test thread waits. */
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_cond = PTHREAD_COND_INITIALIZER;
static int g_done;
static int g_accepted;
static int g_retryable;
static char g_last_detail[64];

static void on_done(void *ctx, otlp_sink_result result, const char *detail) {
  (void)ctx;
  pthread_mutex_lock(&g_lock);
  g_done++;
  if (result == OTLP_SINK_ACCEPTED)
    g_accepted++;
  else if (result == OTLP_SINK_RETRYABLE)
    g_retryable++;
  snprintf(g_last_detail, sizeof(g_last_detail), "%s", detail != nullptr ? detail : "");
  pthread_cond_broadcast(&g_cond);
  pthread_mutex_unlock(&g_lock);
}

static void wait_done(int n) {
  pthread_mutex_lock(&g_lock);
  while (g_done < n)
    pthread_cond_wait(&g_cond, &g_lock);
  pthread_mutex_unlock(&g_lock);
}

static void remove_dir(const char *dir) {
  uv_fs_t req;
  if (uv_fs_scandir(&g_loop, &req, dir, 0, nullptr) >= 0) {
    uv_dirent_t ent;
    while (uv_fs_scandir_next(&req, &ent) != UV_EOF) {
      char path[1024];
      snprintf(path, sizeof(path), "%s/%s", dir, ent.name);
      uv_fs_t r2;
      uv_fs_unlink(&g_loop, &r2, path, nullptr);
      uv_fs_req_cleanup(&r2);
    }
  }
  uv_fs_req_cleanup(&req);
  uv_fs_rmdir(&g_loop, &req, dir, nullptr);
  uv_fs_req_cleanup(&req);
}

void setUp(void) {
  uv_loop_init(&g_loop);
  uv_fs_t req;
  TEST_ASSERT_EQUAL_INT(0, uv_fs_mkdtemp(&g_loop, &req, "otlp-wal-test-XXXXXX", nullptr));
  snprintf(g_dir, sizeof(g_dir), "%s", req.path);
  uv_fs_req_cleanup(&req);
  g_done = g_accepted = g_retryable = 0;
}

void tearDown(void) {
  remove_dir(g_dir);
  uv_loop_close(&g_loop);
}

static otlp_wal_config config(void) {
  return (otlp_wal_config){
      .dir = g_dir,
      .sync = OTLP_WAL_SYNC_FSYNC,
      .segment_bytes = 1 << 20,
      .max_bytes = (size_t)1 << 30,
      .queue_bytes = 1 << 20,
  };
}

static otlp_sink_result submit(otlp_sink *s, const char *text, uint64_t points,
                               const atomic_bool *cancelled) {
  otlp_batch b = {.payload = (const uint8_t *)text, .payload_len = strlen(text), .points = points};
  otlp_sink_completion done = {.fn = on_done, .cancelled = cancelled};
  const char *detail = nullptr;
  otlp_sink_result r = otlp_sink_submit(s, &b, &done, &detail);
  if (r != OTLP_SINK_PENDING) {
    pthread_mutex_lock(&g_lock); /* the writer thread updates it too */
    snprintf(g_last_detail, sizeof(g_last_detail), "%s", detail != nullptr ? detail : "");
    pthread_mutex_unlock(&g_lock);
  }
  return r;
}

/* Every record of the log, concatenated, and the totals. */
typedef struct collected {
  char text[4096];
  size_t len;
  uint64_t records, points, last_sequence;
  int segments;
  bool clean;
} collected;

static bool collect_record(void *ctx, const otlp_wal_record *rec, const uint8_t *payload) {
  collected *c = ctx;
  if (c->len + rec->payload_len < sizeof(c->text)) {
    memcpy(c->text + c->len, payload, rec->payload_len);
    c->len += rec->payload_len;
  }
  return true;
}

static collected read_log(void) {
  collected c = {.clean = true};
  char **names;
  long n = otlp_wal_list_segments(g_dir, &names);
  for (long i = 0; i < n; i++) {
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s", g_dir, names[i]);
    otlp_wal_scan scan;
    otlp_wal_scan_segment(path, collect_record, &c, &scan);
    c.records += scan.records;
    c.points += scan.points;
    if (scan.records > 0)
      c.last_sequence = scan.last_sequence;
    c.clean = c.clean && scan.end == OTLP_WAL_SCAN_CLEAN;
  }
  c.segments = (int)n;
  c.text[c.len] = '\0';
  otlp_wal_list_free(names, n);
  return c;
}

static void stop(otlp_sink *s) {
  otlp_sink_shutdown(s);
  otlp_sink_destroy(s);
}

/* ---- format ------------------------------------------------------------------------------------- */

static void test_crc32c_known_values(void) {
  TEST_ASSERT_EQUAL_UINT32(0xE3069283u, otlp_crc32c(0, "123456789", 9));
  TEST_ASSERT_EQUAL_UINT32(0, otlp_crc32c(0, "", 0));
  /* incremental == one shot, across the 8-byte stride */
  const char *text = "The quick brown fox jumps over the lazy dog";
  uint32_t whole = otlp_crc32c(0, text, strlen(text));
  uint32_t parts = otlp_crc32c(otlp_crc32c(0, text, 13), text + 13, strlen(text) - 13);
  TEST_ASSERT_EQUAL_UINT32(whole, parts);
}

static void test_record_roundtrip_and_corruption(void) {
  uint8_t payload[] = "payload bytes";
  uint8_t header[OTLP_WAL_RECORD_HEADER];
  otlp_wal_record_header(header, 42, 7, payload, sizeof(payload));
  otlp_wal_record rec;
  TEST_ASSERT_TRUE(otlp_wal_record_parse(header, &rec));
  TEST_ASSERT_EQUAL_UINT64(42, rec.sequence);
  TEST_ASSERT_EQUAL_UINT64(7, rec.points);
  TEST_ASSERT_EQUAL_UINT32(sizeof(payload), rec.payload_len);
  TEST_ASSERT_TRUE(otlp_wal_record_check(header, payload, sizeof(payload)));
  payload[3] ^= 1;
  TEST_ASSERT_FALSE(otlp_wal_record_check(header, payload, sizeof(payload)));
  payload[3] ^= 1;
  header[16] ^= 1; /* points field */
  TEST_ASSERT_FALSE(otlp_wal_record_check(header, payload, sizeof(payload)));
  header[0] = 'X';
  TEST_ASSERT_FALSE(otlp_wal_record_parse(header, &rec));
  char name[32];
  otlp_wal_segment_name(name, 123);
  TEST_ASSERT_EQUAL_STRING("00000000000000000123.wal", name);
  uint64_t seq;
  TEST_ASSERT_TRUE(otlp_wal_segment_name_parse(name, &seq));
  TEST_ASSERT_EQUAL_UINT64(123, seq);
  TEST_ASSERT_FALSE(otlp_wal_segment_name_parse("123.wal", &seq));
}

/* ---- sink ----------------------------------------------------------------------------------------- */

static void test_writes_and_acknowledges_after_sync(void) {
  otlp_budget budget;
  otlp_budget_init(&budget, 0);
  otlp_wal_config cfg = config();
  otlp_sink *s = otlp_wal_sink_new(&cfg, &budget);
  TEST_ASSERT_NOT_NULL(s);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "alpha", 1, nullptr));
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "beta", 2, nullptr));
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "gamma", 3, nullptr));
  wait_done(3);
  TEST_ASSERT_EQUAL_INT(3, g_accepted);
  TEST_ASSERT_EQUAL_size_t(0, otlp_budget_used(&budget)); /* copies released after writing */
  otlp_strbuf json = {0};
  s->ops->stats_json(s, &json);
  TEST_ASSERT_NOT_NULL(strstr(json.data, "\"records\":3"));
  TEST_ASSERT_NULL(strstr(json.data, "\"syncs\":0,"));
  otlp_strbuf_free(&json);
  stop(s);
  collected c = read_log();
  TEST_ASSERT_TRUE(c.clean);
  TEST_ASSERT_EQUAL_UINT64(3, c.records);
  TEST_ASSERT_EQUAL_UINT64(6, c.points);
  TEST_ASSERT_EQUAL_STRING("alphabetagamma", c.text);
}

static void test_rotation_and_restart_continue_numbering(void) {
  otlp_wal_config cfg = config();
  cfg.segment_bytes = 64; /* rotate after (almost) every group */
  cfg.sync = OTLP_WAL_SYNC_NONE;
  otlp_sink *s = otlp_wal_sink_new(&cfg, nullptr);
  for (int i = 0; i < 5; i++) {
    TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "0123456789abcdef0123456789", 1, nullptr));
    wait_done(i + 1); /* one group each */
  }
  stop(s);
  collected c = read_log();
  TEST_ASSERT_EQUAL_UINT64(5, c.records);
  TEST_ASSERT_TRUE(c.segments >= 3);
  TEST_ASSERT_EQUAL_UINT64(5, c.last_sequence);
  /* restart: numbering continues */
  s = otlp_wal_sink_new(&cfg, nullptr);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "after-restart", 1, nullptr));
  wait_done(6);
  stop(s);
  c = read_log();
  TEST_ASSERT_TRUE(c.clean);
  TEST_ASSERT_EQUAL_UINT64(6, c.records);
  TEST_ASSERT_EQUAL_UINT64(6, c.last_sequence);
}

static void append_bytes(const char *path, const void *data, size_t len) {
  FILE *f = fopen(path, "ab");
  TEST_ASSERT_NOT_NULL(f);
  TEST_ASSERT_EQUAL_size_t(len, fwrite(data, 1, len, f));
  fclose(f);
}

static void test_recovery_truncates_torn_tail(void) {
  otlp_wal_config cfg = config();
  otlp_sink *s = otlp_wal_sink_new(&cfg, nullptr);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "first", 1, nullptr));
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "second", 1, nullptr));
  wait_done(2);
  stop(s);
  /* a crash in the middle of the next record: half a header and some payload */
  char **names;
  long n = otlp_wal_list_segments(g_dir, &names);
  TEST_ASSERT_EQUAL_INT(1, n);
  char path[1024];
  snprintf(path, sizeof(path), "%s/%s", g_dir, names[0]);
  otlp_wal_list_free(names, n);
  uint8_t header[OTLP_WAL_RECORD_HEADER];
  otlp_wal_record_header(header, 3, 1, (const uint8_t *)"third", 5);
  append_bytes(path, header, 20);
  collected torn = read_log();
  TEST_ASSERT_FALSE(torn.clean);
  TEST_ASSERT_EQUAL_UINT64(2, torn.records);

  s = otlp_wal_sink_new(&cfg, nullptr);
  TEST_ASSERT_NOT_NULL(s);
  otlp_strbuf json = {0};
  s->ops->stats_json(s, &json);
  TEST_ASSERT_NOT_NULL(strstr(json.data, "\"truncated_bytes\":20"));
  TEST_ASSERT_NOT_NULL(strstr(json.data, "\"recovered_records\":2"));
  otlp_strbuf_free(&json);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "third", 1, nullptr));
  wait_done(3);
  stop(s);
  collected c = read_log();
  TEST_ASSERT_TRUE(c.clean);
  TEST_ASSERT_EQUAL_STRING("firstsecondthird", c.text);
  TEST_ASSERT_EQUAL_UINT64(3, c.last_sequence);
}

static void test_damaged_payload_detected(void) {
  otlp_wal_config cfg = config();
  otlp_sink *s = otlp_wal_sink_new(&cfg, nullptr);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "intact", 1, nullptr));
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "flipped", 1, nullptr));
  wait_done(2);
  stop(s);
  char **names;
  long n = otlp_wal_list_segments(g_dir, &names);
  char path[1024];
  snprintf(path, sizeof(path), "%s/%s", g_dir, names[0]);
  otlp_wal_list_free(names, n);
  /* flip one payload bit of the second record */
  FILE *f = fopen(path, "r+b");
  TEST_ASSERT_NOT_NULL(f);
  long offset = OTLP_WAL_SEGMENT_HEADER + OTLP_WAL_RECORD_HEADER + 6 + OTLP_WAL_RECORD_HEADER + 2;
  TEST_ASSERT_EQUAL_INT(0, fseek(f, offset, SEEK_SET));
  int ch = fgetc(f);
  fseek(f, offset, SEEK_SET);
  fputc(ch ^ 0x10, f);
  fclose(f);
  collected c = read_log();
  TEST_ASSERT_FALSE(c.clean);
  TEST_ASSERT_EQUAL_UINT64(1, c.records); /* the reader stops at the damaged record */
  TEST_ASSERT_EQUAL_STRING("intact", c.text);
}

static void test_full_log_is_retryable(void) {
  otlp_wal_config cfg = config();
  cfg.max_bytes = OTLP_WAL_SEGMENT_HEADER + 2 * (OTLP_WAL_RECORD_HEADER + 4);
  otlp_sink *s = otlp_wal_sink_new(&cfg, nullptr);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "aaaa", 1, nullptr));
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "bbbb", 1, nullptr));
  wait_done(2);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_RETRYABLE, submit(s, "cccc", 1, nullptr));
  TEST_ASSERT_EQUAL_STRING("write-ahead log is full", g_last_detail);
  stop(s);
  TEST_ASSERT_EQUAL_UINT64(2, read_log().records);
}

static void test_queue_bound_is_retryable(void) {
  otlp_wal_config cfg = config();
  cfg.queue_bytes = 3;
  otlp_sink *s = otlp_wal_sink_new(&cfg, nullptr);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_RETRYABLE, submit(s, "four", 1, nullptr));
  TEST_ASSERT_EQUAL_STRING("write-ahead log queue is full", g_last_detail);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "abc", 1, nullptr));
  wait_done(1);
  stop(s);
}

static void test_failed_write_is_retryable_and_truncated(void) {
  otlp_wal_config cfg = config();
  cfg.fail_writes_every = 2;
  otlp_sink *s = otlp_wal_sink_new(&cfg, nullptr);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "one", 1, nullptr));
  wait_done(1);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "two", 1, nullptr)); /* injected failure */
  wait_done(2);
  TEST_ASSERT_EQUAL_INT(1, g_retryable);
  TEST_ASSERT_EQUAL_STRING("write-ahead log write failed", g_last_detail);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "three", 1, nullptr));
  wait_done(3);
  stop(s);
  collected c = read_log();
  TEST_ASSERT_TRUE(c.clean); /* the partial record was truncated away */
  TEST_ASSERT_EQUAL_STRING("onethree", c.text);
  TEST_ASSERT_EQUAL_INT(2, g_accepted);
}

static void test_cancelled_batch_not_written(void) {
  otlp_wal_config cfg = config();
  otlp_sink *s = otlp_wal_sink_new(&cfg, nullptr);
  atomic_bool cancelled;
  atomic_init(&cancelled, true);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "gone", 1, &cancelled));
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "kept", 1, nullptr));
  wait_done(2);
  TEST_ASSERT_EQUAL_INT(1, g_retryable);
  stop(s);
  TEST_ASSERT_EQUAL_STRING("kept", read_log().text);
}

static void test_shutdown_writes_queued_batches(void) {
  otlp_wal_config cfg = config();
  otlp_sink *s = otlp_wal_sink_new(&cfg, nullptr);
  for (int i = 0; i < 50; i++)
    TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, submit(s, "x", 1, nullptr));
  otlp_sink_quiesce(s);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_RETRYABLE, submit(s, "late", 1, nullptr));
  stop(s); /* joins the writer after it drained the queue */
  TEST_ASSERT_EQUAL_INT(50, g_done);
  TEST_ASSERT_EQUAL_INT(50, g_accepted);
  TEST_ASSERT_EQUAL_UINT64(50, read_log().records);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_crc32c_known_values);
  RUN_TEST(test_record_roundtrip_and_corruption);
  RUN_TEST(test_writes_and_acknowledges_after_sync);
  RUN_TEST(test_rotation_and_restart_continue_numbering);
  RUN_TEST(test_recovery_truncates_torn_tail);
  RUN_TEST(test_damaged_payload_detected);
  RUN_TEST(test_full_log_is_retryable);
  RUN_TEST(test_queue_bound_is_retryable);
  RUN_TEST(test_failed_write_is_retryable_and_truncated);
  RUN_TEST(test_cancelled_batch_not_written);
  RUN_TEST(test_shutdown_writes_queued_batches);
  return UNITY_END();
}
