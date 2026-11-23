/* fuzz_export.c - fuzz target for the receiver's request path
 *
 * Feeds arbitrary bytes through what the service does with a request body: the nesting pre-scan,
 * decoding into a bounded arena, validation, the partial-success response, and filtering plus
 * re-encoding of the accepted subset. Invariants are checked on every input (abort on violation):
 *   - inputs the pre-scan accepts never nest deeper than the limit (protobuf-c recursion bound);
 *   - decoding never exceeds the arena's per-request budget;
 *   - the response decodes back with the same rejected count;
 *   - the filtered, re-encoded request decodes again, has no rejected points and exactly the
 *     accepted ones;
 *   - every reservation is returned to the budget.
 *
 * With -DOTLP_LIBFUZZER (clang -fsanitize=fuzzer) it is a libFuzzer target; otherwise main() runs
 * each file (or each file of each directory) named on the command line, for corpus replay in the
 * regular test suite.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#include "grpc.h"
#include "otlp_arena.h"
#include "otlp_budget.h"
#include "otlp_proto.h"
#include "otlp_response.h"
#include "otlp_validate.h"
#include "otlp_wire.h"

enum {
  MAX_DEPTH = 32,
  DECODED_LIMIT = 8 << 20,
  KEEP = 64 << 10,
};

#define CHECK(cond)                                                                      \
  do {                                                                                   \
    if (!(cond)) {                                                                       \
      fprintf(stderr, "fuzz_export: invariant failed: %s (line %d)\n", #cond, __LINE__); \
      abort();                                                                           \
    }                                                                                    \
  } while (0)

static otlp_budget g_budget;
static otlp_arena g_arena;
static otlp_arena g_check; /* second arena for re-decoding */

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  static bool ready;
  if (!ready) {
    otlp_budget_init(&g_budget, 64u << 20);
    otlp_arena_init(&g_arena, &g_budget, KEEP);
    otlp_arena_init(&g_check, &g_budget, KEEP);
    ready = true;
  }
  if (otlp_wire_check(OTLP_PB_REQUEST_DESCRIPTOR, data, size, MAX_DEPTH) != OTLP_WIRE_OK)
    return 0; /* refused before decoding, as the service does */

  otlp_arena_begin(&g_arena, DECODED_LIMIT, size);
  otlp_pb_request *req = (otlp_pb_request *)protobuf_c_message_unpack(
      OTLP_PB_REQUEST_DESCRIPTOR, &g_arena.allocator, size, data);
  CHECK(g_arena.reserved <= DECODED_LIMIT);
  if (req != nullptr) {
    otlp_counts counts;
    otlp_validate_request(req, &counts);
    CHECK(counts.rejected <= counts.points);
    uint64_t sum = 0;
    for (int k = 0; k < OTLP_KIND_COUNT; k++)
      sum += counts.points_by_kind[k];
    CHECK(sum == counts.points);

    grpc_message *resp = otlp_response_new(counts.rejected, counts.reason);
    CHECK(resp != nullptr);
    otlp_pb_response *decoded = (otlp_pb_response *)protobuf_c_message_unpack(
        &opentelemetry__proto__collector__metrics__v1__export_metrics_service_response__descriptor,
        nullptr, resp->len, resp->data);
    CHECK(decoded != nullptr);
    CHECK(counts.rejected == 0
              ? decoded->partial_success == nullptr
              : (uint64_t)decoded->partial_success->rejected_data_points == counts.rejected);
    protobuf_c_message_free_unpacked(&decoded->base, nullptr);
    grpc_message_free(resp);

    otlp_filter_request(req);
    size_t len = protobuf_c_message_get_packed_size(&req->base);
    uint8_t *buf = malloc(len + 1);
    CHECK(buf != nullptr);
    CHECK(protobuf_c_message_pack(&req->base, buf) == len);
    otlp_arena_begin(&g_check, SIZE_MAX, len);
    otlp_pb_request *again = (otlp_pb_request *)protobuf_c_message_unpack(
        OTLP_PB_REQUEST_DESCRIPTOR, &g_check.allocator, len, buf);
    CHECK(again != nullptr);
    otlp_counts after;
    otlp_validate_request(again, &after);
    CHECK(after.rejected == 0);
    CHECK(after.points == counts.points - counts.rejected);
    otlp_arena_end(&g_check);
    free(buf);
  } else {
    /* refused by protobuf-c (malformed) or by the budget: the arena says which */
    CHECK(otlp_arena_status_get(&g_arena) != OTLP_ARENA_NOMEM);
  }
  otlp_arena_end(&g_arena);
  CHECK(otlp_budget_used(&g_budget) <= 2 * (size_t)KEEP);
  return 0;
}

#ifndef OTLP_LIBFUZZER
static int run_file(const char *path) {
  FILE *f = fopen(path, "rb");
  if (f == nullptr) {
    perror(path);
    return 1;
  }
  uint8_t *data = nullptr;
  size_t len = 0, cap = 0;
  for (;;) {
    if (len == cap) {
      cap = cap ? cap * 2 : 65536;
      uint8_t *grown = realloc(data, cap);
      if (grown == nullptr) {
        free(data);
        fclose(f);
        return 1;
      }
      data = grown;
    }
    size_t n = fread(data + len, 1, cap - len, f);
    if (n == 0)
      break;
    len += n;
  }
  fclose(f);
  LLVMFuzzerTestOneInput(data, len);
  free(data);
  return 0;
}

int main(int argc, char **argv) {
  uv_loop_t loop;
  uv_loop_init(&loop);
  int files = 0, failures = 0;
  for (int i = 1; i < argc; i++) {
    uv_fs_t req;
    if (uv_fs_scandir(&loop, &req, argv[i], 0, nullptr) >= 0) {
      uv_dirent_t ent;
      while (uv_fs_scandir_next(&req, &ent) != UV_EOF) {
        if (ent.type == UV_DIRENT_DIR || ent.name[0] == '.')
          continue;
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", argv[i], ent.name);
        failures += run_file(path);
        files++;
      }
    } else {
      failures += run_file(argv[i]);
      files++;
    }
    uv_fs_req_cleanup(&req);
  }
  uv_loop_close(&loop);
  printf("fuzz_export: replayed %d inputs, %d unreadable\n", files, failures);
  return failures == 0 && files > 0 ? 0 : 1;
}
#endif
