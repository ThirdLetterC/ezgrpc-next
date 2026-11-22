#pragma once

/* common.h - private library helpers
 *
 * Private library helpers; public headers must not include this file. Provides the short integer
 * typedefs (u8, u32, i64, ...) used throughout src/, grpc_align_up(), and compile-time limits
 * such as CONFIG_STREAM_MAX_HEADERS. Included by core.h.
 */

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "grpc_defs.h"
#include "grpc_mem.h"

typedef char i8;
typedef uint8_t u8;
typedef int8_t s8;
typedef int16_t i16;
typedef uint16_t u16;
typedef int32_t i32;
typedef uint32_t u32;
typedef int64_t i64;
typedef uint64_t u64;

#ifndef alignof
#if (!defined(__STDC_VERSION__) || __STDC_VERSION__ < 202311L)
#define alignof _Alignof
#endif
#endif

/* Marks a printf-style function (format string at fmt_index, arguments from args_index) so that
 * the compiler checks every call's format string against its arguments. */
#if defined(__GNUC__) || defined(__clang__)
#define GRPC_PRINTF_FORMAT(fmt_index, args_index) \
  __attribute__((format(printf, fmt_index, args_index)))
#else
#define GRPC_PRINTF_FORMAT(fmt_index, args_index)
#endif

/* Checked size arithmetic: each stores the result in *out and returns false, or returns true
 * (leaving *out unchanged) when the result does not fit size_t. Allocation sizes computed from
 * lengths or counts go through these, so they fail instead of wrapping to a small buffer. */
static inline bool grpc_size_add(size_t a, size_t b, size_t *out) {
  if (a > SIZE_MAX - b)
    return true;
  *out = a + b;
  return false;
}
static inline bool grpc_size_mul(size_t a, size_t b, size_t *out) {
  if (b != 0 && a > SIZE_MAX / b)
    return true;
  *out = a * b;
  return false;
}

/* grpc_malloc() / grpc_realloc() of an array of n elements of `size` bytes: nullptr when the
 * total size overflows (realloc then leaves `p` untouched, as on allocation failure). */
static inline void *grpc_malloc_array(size_t n, size_t size) {
  size_t bytes;
  return grpc_size_mul(n, size, &bytes) ? nullptr : grpc_malloc(bytes);
}
static inline void *grpc_realloc_array(void *p, size_t n, size_t size) {
  size_t bytes;
  return grpc_size_mul(n, size, &bytes) ? nullptr : grpc_realloc(p, bytes);
}

/* Round size up to the next multiple of a power-of-two alignment. */
static inline size_t grpc_align_up(size_t size, size_t alignment) {
  return (size + alignment - 1) & ~(alignment - 1);
}

/* Maximum request headers kept per stream. Overridable at build time. */
#ifndef CONFIG_STREAM_MAX_HEADERS
#define CONFIG_STREAM_MAX_HEADERS 32
#endif
