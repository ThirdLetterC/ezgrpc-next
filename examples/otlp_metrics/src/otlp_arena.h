#pragma once

/* otlp_arena.h - a bounded bump allocator for decoding one request
 *
 * protobuf-c allocates every decoded message, repeated-field array, string and bytes value through
 * a ProtobufCAllocator. Pointing it at an arena makes decoding fast (no per-field malloc/free) and
 * bounded: the arena hands out at most `limit` bytes per request (the decoded-data budget), and
 * every chunk it allocates is first reserved from the shared otlp_budget. When either bound is hit
 * the allocation fails, protobuf-c gives up, and otlp_arena_status() tells which bound it was.
 *
 * Lifetime: one arena per thread, reused for request after request:
 *   otlp_arena_init() once, then per request otlp_arena_begin() ... decode/use ...
 *   otlp_arena_end(), and otlp_arena_destroy() at exit. Everything allocated between begin and end
 *   is released together at end; protobuf-c's free calls are no-ops. One chunk of at most
 *   `keep_bytes` stays allocated (and reserved) between requests, so steady-state decoding of
 *   ordinary requests needs no system allocation at all.
 *
 * Not thread-safe: an arena belongs to one thread at a time.
 */

#include <protobuf-c/protobuf-c.h>
#include <stddef.h>
#include <stdint.h>

#include "otlp_budget.h"

typedef enum otlp_arena_status {
  /** No allocation has failed since otlp_arena_begin(). */
  OTLP_ARENA_OK = 0,
  /** The per-request limit was reached: the request decodes to too much data. */
  OTLP_ARENA_LIMIT,
  /** The shared budget is exhausted or the system is out of memory: a temporary condition. */
  OTLP_ARENA_NOMEM,
} otlp_arena_status;

typedef struct otlp_arena_chunk otlp_arena_chunk;

typedef struct otlp_arena {
  /** Hand this to protobuf_c_message_unpack(). */
  ProtobufCAllocator allocator;
  otlp_budget *budget;
  /** Per-request limit of chunk bytes (set by otlp_arena_begin()). */
  size_t limit;
  /** Bytes of the chunks owned now, all reserved from `budget`. */
  size_t reserved;
  /** Bytes handed out since otlp_arena_begin(). */
  size_t used;
  /** Highest `used` of any request. */
  size_t peak_used;
  /** Largest chunk kept between requests. */
  size_t keep_bytes;
  /** Size of the next chunk to allocate. */
  size_t next_chunk;
  otlp_arena_chunk *chunks;
  uint8_t *cursor;
  uint8_t *end;
  otlp_arena_status status;
} otlp_arena;

/** Prepares an arena drawing on `budget` (may be nullptr: unlimited). Allocates nothing. */
void otlp_arena_init(otlp_arena *arena, otlp_budget *budget, size_t keep_bytes);

/**
 * Starts a request that may use up to `limit` bytes. `size_hint` (e.g. the encoded size) sizes the
 * first chunk so typical requests need a single one.
 */
void otlp_arena_begin(otlp_arena *arena, size_t limit, size_t size_hint);

/** Allocates `size` bytes aligned for any scalar type, or returns nullptr (see status). */
void *otlp_arena_alloc(otlp_arena *arena, size_t size);

/** The first failure since otlp_arena_begin(), or OTLP_ARENA_OK. */
otlp_arena_status otlp_arena_status_get(const otlp_arena *arena);

/** Releases everything allocated since otlp_arena_begin(), keeping one chunk for reuse. */
void otlp_arena_end(otlp_arena *arena);

/** Releases every chunk, including the kept one. The arena can be initialized again. */
void otlp_arena_destroy(otlp_arena *arena);
