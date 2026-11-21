/* grpc_mem.c - the pluggable allocator behind every grpc allocation
 *
 * All library allocations go through grpc_malloc/grpc_calloc/grpc_realloc/grpc_aligned_alloc and
 * are released with grpc_free, which dispatch through the process-wide `current` allocator table.
 * The default table is chosen at build time: mimalloc (GRPC_USE_MIMALLOC), jemalloc
 * (GRPC_USE_JEMALLOC, which replaces the libc symbols when linked), the MSVC _aligned_* family,
 * or plain libc. Because the MSVC default uses _aligned_malloc, memory returned by grpc must be
 * freed with grpc_free and never with libc free, and vice versa.
 *
 * The table is a plain global with no locking: grpc_set_allocator must be called before any
 * allocation (in particular before creating a server or client) and not concurrently with other
 * grpc calls, since a block must be freed by the allocator that produced it.
 */

#include <stdlib.h>
#include <string.h>
#include "grpc_mem.h"

#if defined(_MSC_VER) && !defined(GRPC_USE_MIMALLOC) && !defined(GRPC_USE_JEMALLOC)
#include <malloc.h>

/* MSVC requires aligned blocks to be freed/reallocated with the matching CRT
 * functions. Use that allocation family for every default allocation, so that grpc_free can
 * release both plain and aligned blocks (MSVC has no C11 aligned_alloc). */

/* Allocates size bytes (at least 1, since _aligned_malloc rejects 0) aligned to max_align_t. */
static void *win_malloc(size_t size) {
  return _aligned_malloc(size ? size : 1, _Alignof(grpc_max_align_t));
}
/* calloc on top of win_malloc: checks count * size for overflow, then zero-fills. */
static void *win_calloc(size_t count, size_t size) {
  if (size != 0 && count > SIZE_MAX / size)
    return nullptr;
  size_t bytes = count * size;
  void *ptr = win_malloc(bytes);
  if (ptr != nullptr)
    memset(ptr, 0, bytes);
  return ptr;
}
/* Reallocates a win_malloc block, keeping the max_align_t alignment. */
static void *win_realloc(void *ptr, size_t size) {
  return _aligned_realloc(ptr, size, _Alignof(grpc_max_align_t));
}
/* Allocates size bytes (at least 1) with the requested power-of-two alignment. */
static void *win_aligned_alloc(size_t alignment, size_t size) {
  return _aligned_malloc(size ? size : 1, alignment);
}
/* Frees any block from the win_* functions; _aligned_free accepts nullptr. */
static void win_free(void *ptr) {
  _aligned_free(ptr);
}
#endif

/* default_allocator is the build-time default restored by grpc_set_allocator(nullptr); current
 * is the table actually used and starts as a copy of it. */
#if defined(GRPC_USE_MIMALLOC)
#include <mimalloc.h>
static const char default_allocator_name[] = "mimalloc";
static const grpc_allocator default_allocator = {mi_malloc, mi_calloc, mi_realloc, mi_free,
                                                 mi_aligned_alloc};
static grpc_allocator current = {mi_malloc, mi_calloc, mi_realloc, mi_free, mi_aligned_alloc};
#elif defined(GRPC_USE_JEMALLOC)
/* jemalloc replaces the libc functions when linked */
static const char default_allocator_name[] = "jemalloc";
static const grpc_allocator default_allocator = {malloc, calloc, realloc, free, aligned_alloc};
static grpc_allocator current = {malloc, calloc, realloc, free, aligned_alloc};
#elif defined(_MSC_VER)
static const char default_allocator_name[] = "libc";
static const grpc_allocator default_allocator = {win_malloc, win_calloc, win_realloc, win_free,
                                                 win_aligned_alloc};
static grpc_allocator current = {win_malloc, win_calloc, win_realloc, win_free, win_aligned_alloc};
#else
static const char default_allocator_name[] = "libc";
static const grpc_allocator default_allocator = {malloc, calloc, realloc, free, aligned_alloc};
static grpc_allocator current = {malloc, calloc, realloc, free, aligned_alloc};
#endif

/* Resets to the default table for nullptr; otherwise overrides only the non-null members, so a
 * null member keeps the function currently installed (not necessarily the default). Not
 * thread-safe. */
GRPC_API void grpc_set_allocator(const grpc_allocator *allocator) {
  if (allocator == nullptr) {
    current = default_allocator;
    return;
  }
  if (allocator->malloc)
    current.malloc = allocator->malloc;
  if (allocator->calloc)
    current.calloc = allocator->calloc;
  if (allocator->realloc)
    current.realloc = allocator->realloc;
  if (allocator->free)
    current.free = allocator->free;
  if (allocator->aligned_alloc)
    current.aligned_alloc = allocator->aligned_alloc;
}

/* Returns a pointer to the live table; it reflects later grpc_set_allocator calls. */
GRPC_API const grpc_allocator *grpc_get_allocator(void) {
  return &current;
}

/* Returns the build-time default allocator name, even if a custom allocator is installed. */
GRPC_API const char *grpc_get_allocator_name(void) {
  return default_allocator_name;
}

/* Allocates through the current allocator table. */
GRPC_API void *grpc_malloc(size_t size) {
  return current.malloc(size);
}

/* Allocates zeroed memory through the current allocator table. */
GRPC_API void *grpc_calloc(size_t nmemb, size_t size) {
  return current.calloc(nmemb, size);
}

/* Resizes a block from the current allocator table; ptr must come from grpc_*alloc. */
GRPC_API void *grpc_realloc(void *ptr, size_t size) {
  return current.realloc(ptr, size);
}

/* Frees a block from the current allocator table (the only valid way to free grpc memory). */
GRPC_API void grpc_free(void *ptr) {
  current.free(ptr);
}

/* Allocates aligned memory through the current allocator table; release it with grpc_free. With
 * the libc default (C11 aligned_alloc), size should be a multiple of alignment. */
GRPC_API void *grpc_aligned_alloc(size_t alignment, size_t size) {
  return current.aligned_alloc(alignment, size);
}

/* Copies at most n bytes of s (stopping at a NUL) into a new NUL-terminated string from the
 * current allocator. Returns nullptr if s is null or on allocation failure; free with grpc_free. */
GRPC_API char *grpc_strndup(const char *s, size_t n) {
  if (s == nullptr)
    return nullptr;
  size_t len = strnlen(s, n);
  char *r = current.malloc(len + 1);
  if (r != nullptr) {
    memcpy(r, s, len);
    r[len] = '\0';
  }
  return r;
}

/* Duplicates a NUL-terminated string with grpc_strndup; returns nullptr for nullptr. */
GRPC_API char *grpc_strdup(const char *s) {
  return s != nullptr ? grpc_strndup(s, strlen(s)) : nullptr;
}
