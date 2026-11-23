#pragma once

/* otlp_strbuf.h - a growable text buffer for JSON and messages (grpc_malloc memory) */

#include <stdbool.h>
#include <stddef.h>

typedef struct otlp_strbuf {
  char *data; /* NUL-terminated while `failed` is false */
  size_t len;
  size_t cap;
  bool failed; /* an allocation failed: the content is incomplete */
} otlp_strbuf;

#if defined(__GNUC__) || defined(__clang__)
#define OTLP_PRINTF(fmt, args) [[gnu::format(printf, fmt, args)]]
#else
#define OTLP_PRINTF(fmt, args)
#endif

/** Appends printf-style text. On allocation failure sets `failed` and appends nothing more. */
OTLP_PRINTF(2, 3) void otlp_strbuf_printf(otlp_strbuf *buf, const char *fmt, ...);

/** Appends `s` as a JSON string literal, quotes included. */
void otlp_strbuf_json_string(otlp_strbuf *buf, const char *s);

/** Releases the buffer and zeroes it. */
void otlp_strbuf_free(otlp_strbuf *buf);
