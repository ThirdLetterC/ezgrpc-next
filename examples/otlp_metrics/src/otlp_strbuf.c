#include "otlp_strbuf.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include "grpc_mem.h"

static bool reserve(otlp_strbuf *buf, size_t extra) {
  if (buf->failed)
    return false;
  if (buf->len + extra + 1 <= buf->cap)
    return true;
  size_t cap = buf->cap != 0 ? buf->cap : 256;
  while (cap < buf->len + extra + 1) {
    if (cap > SIZE_MAX / 2) {
      buf->failed = true;
      return false;
    }
    cap *= 2;
  }
  char *data = grpc_realloc(buf->data, cap);
  if (data == nullptr) {
    buf->failed = true;
    return false;
  }
  buf->data = data;
  buf->cap = cap;
  return true;
}

void otlp_strbuf_printf(otlp_strbuf *buf, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  va_list ap2;
  va_copy(ap2, ap);
  int n = vsnprintf(nullptr, 0, fmt, ap);
  va_end(ap);
  if (n < 0 || !reserve(buf, (size_t)n)) {
    buf->failed = true;
    va_end(ap2);
    return;
  }
  (void)vsnprintf(buf->data + buf->len, buf->cap - buf->len, fmt, ap2);
  va_end(ap2);
  buf->len += (size_t)n;
}

void otlp_strbuf_json_string(otlp_strbuf *buf, const char *s) {
  otlp_strbuf_printf(buf, "\"");
  for (const unsigned char *p = (const unsigned char *)s; p != nullptr && *p != '\0'; p++) {
    if (*p == '"' || *p == '\\')
      otlp_strbuf_printf(buf, "\\%c", *p);
    else if (*p < 0x20)
      otlp_strbuf_printf(buf, "\\u%04x", *p);
    else
      otlp_strbuf_printf(buf, "%c", *p);
  }
  otlp_strbuf_printf(buf, "\"");
}

void otlp_strbuf_free(otlp_strbuf *buf) {
  grpc_free(buf->data);
  *buf = (otlp_strbuf){0};
}
