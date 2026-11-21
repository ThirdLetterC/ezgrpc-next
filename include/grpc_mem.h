#pragma once

/* grpc_mem.h - the library allocator
 *
 * Every allocation the library makes goes through one process-wide, replaceable allocator. Memory
 * from grpc_malloc() / grpc_calloc() / grpc_realloc() / grpc_aligned_alloc() / grpc_strdup() /
 * grpc_strndup(), and any buffer the library hands out for the application to release, must be
 * released with grpc_free(), never with libc free(): on MSVC the default allocator is built on
 * _aligned_malloc()/_aligned_free(), so mixing the two corrupts the heap.
 */

#include "grpc_defs.h"

/**
 * The allocator used for everything grpc allocates, including the
 * buffers it hands to the application (to be released with grpc_free()).
 * It defaults to the one the library was built with (libc, mimalloc or
 * jemalloc).
 */
typedef struct grpc_allocator {
  /** Like malloc(); must return memory aligned for any scalar type. */
  void *(*malloc)(size_t size);
  /** Like calloc(). */
  void *(*calloc)(size_t nmemb, size_t size);
  /** Like realloc(), for blocks from ``malloc``, ``calloc`` or ``realloc``. */
  void *(*realloc)(void *ptr, size_t size);
  /** Frees blocks from every other member, including ``aligned_alloc``. */
  void (*free)(void *ptr);
  /** Like C11 aligned_alloc(). */
  void *(*aligned_alloc)(size_t alignment, size_t size);
} grpc_allocator;

/**
 * Replaces the allocator; nullptr restores the default. Call it before
 * creating any server or client, before anything is allocated: memory must be
 * freed by the allocator that allocated it.
 * Functions left nullptr keep their current value, so replace all five
 * functions together; mixing a custom ``malloc`` with the default ``free``
 * (or the reverse) is undefined. Not thread-safe.
 */
GRPC_API void grpc_set_allocator(const grpc_allocator *allocator);

/**
 * The allocator in use. Points to the live table, so it reflects later
 * grpc_set_allocator() calls.
 */
GRPC_API const grpc_allocator *grpc_get_allocator(void);

/**
 * The name of the default allocator the library was built with: "libc",
 * "mimalloc" or "jemalloc". It does not change when a custom allocator is set.
 */
GRPC_API const char *grpc_get_allocator_name(void);

/** Allocates ``size`` bytes with the current allocator. Free with grpc_free(). */
GRPC_API GRPC_NODISCARD void *grpc_malloc(size_t size);
/** Allocates zeroed memory with the current allocator. Free with grpc_free(). */
GRPC_API GRPC_NODISCARD void *grpc_calloc(size_t nmemb, size_t size);
/**
 * Resizes a block from grpc_malloc(), grpc_calloc() or grpc_realloc(). Free
 * the result with grpc_free().
 */
GRPC_API GRPC_NODISCARD void *grpc_realloc(void *ptr, size_t size);
/**
 * Frees memory allocated by the library or by the grpc_* allocation functions.
 * Accepts nullptr (as long as the installed ``free`` does).
 */
GRPC_API void grpc_free(void *ptr);
/**
 * Allocates ``size`` bytes aligned to ``alignment`` (a power of two). Free with
 * grpc_free().
 */
GRPC_API GRPC_NODISCARD void *grpc_aligned_alloc(size_t alignment, size_t size);
/** Duplicates ``s`` with grpc_malloc(). Returns nullptr for nullptr or on failure. */
GRPC_API GRPC_NODISCARD char *grpc_strdup(const char *s);
/**
 * Copies at most ``n`` bytes of ``s`` (stopping at a NUL) into a new
 * NUL-terminated string from grpc_malloc(). Returns nullptr for nullptr or on
 * failure.
 */
GRPC_API GRPC_NODISCARD char *grpc_strndup(const char *s, size_t n);
