#include "otlp_stats.h"

#include <inttypes.h>
#include <string.h>

void otlp_stats_init(otlp_stats *stats) {
#define INIT(name) atomic_init(&stats->name, 0);
  OTLP_STATS_COUNTERS(INIT)
#undef INIT
  for (size_t i = 0; i < 17; i++)
    atomic_init(&stats->status[i], 0);
}

void otlp_stats_snapshot_get(const otlp_stats *stats, otlp_stats_snapshot *out) {
#define LOAD(name) out->name = atomic_load_explicit(&stats->name, memory_order_relaxed);
  OTLP_STATS_COUNTERS(LOAD)
#undef LOAD
  for (size_t i = 0; i < 17; i++)
    out->status[i] = atomic_load_explicit(&stats->status[i], memory_order_relaxed);
}

bool otlp_stats_lookup(const otlp_stats_snapshot *snapshot, const char *name, uint64_t *value) {
#define LOOKUP(field)              \
  if (strcmp(name, #field) == 0) { \
    *value = snapshot->field;      \
    return true;                   \
  }
  OTLP_STATS_COUNTERS(LOOKUP)
#undef LOOKUP
  return false;
}

void otlp_stats_json(const otlp_stats_snapshot *snapshot, otlp_strbuf *out) {
  const char *sep = "";
#define EMIT(name)                                                          \
  otlp_strbuf_printf(out, "%s\"%s\":%" PRIu64, sep, #name, snapshot->name); \
  sep = ",";
  OTLP_STATS_COUNTERS(EMIT)
#undef EMIT
  otlp_strbuf_printf(out, ",\"status\":{");
  sep = "";
  for (size_t i = 0; i < 17; i++) {
    if (snapshot->status[i] == 0)
      continue;
    otlp_strbuf_printf(out, "%s\"%zu\":%" PRIu64, sep, i, snapshot->status[i]);
    sep = ",";
  }
  otlp_strbuf_printf(out, "}");
}
