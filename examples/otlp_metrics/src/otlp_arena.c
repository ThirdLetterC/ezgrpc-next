#include "otlp_arena.h"

#include <stdalign.h>

#include "grpc_mem.h"

/* Chunk header; the usable bytes follow it, aligned like malloc() memory. */
struct otlp_arena_chunk {
  otlp_arena_chunk *next;
  size_t size; /* bytes of the whole chunk, header included (the amount reserved) */
  alignas(max_align_t) uint8_t data[];
};

enum {
  ALIGN = alignof(max_align_t),
  MIN_CHUNK = 16 * 1024,
};

static size_t align_up(size_t n) {
  return (n + (ALIGN - 1)) & ~(size_t)(ALIGN - 1);
}

static void *pb_alloc(void *data, size_t size) {
  return otlp_arena_alloc(data, size);
}

/* protobuf-c frees what it allocated when unpacking fails half way; the arena releases it all at
 * once instead */
static void pb_free(void *data, void *pointer) {
  (void)data;
  (void)pointer;
}

void otlp_arena_init(otlp_arena *arena, otlp_budget *budget, size_t keep_bytes) {
  *arena = (otlp_arena){
      .allocator = {.alloc = pb_alloc, .free = pb_free, .allocator_data = arena},
      .budget = budget,
      .keep_bytes = keep_bytes,
  };
}

static void chunk_release(otlp_arena *arena, otlp_arena_chunk *chunk) {
  arena->reserved -= chunk->size;
  if (arena->budget != nullptr)
    otlp_budget_release(arena->budget, chunk->size);
  grpc_free(chunk);
}

void otlp_arena_begin(otlp_arena *arena, size_t limit, size_t size_hint) {
  arena->limit = limit;
  arena->used = 0;
  arena->status = OTLP_ARENA_OK;
  /* decoded protobuf-c structures are typically 2-4x the encoded size */
  size_t first = size_hint > SIZE_MAX / 3 ? SIZE_MAX : size_hint * 3;
  if (first < MIN_CHUNK)
    first = MIN_CHUNK;
  arena->next_chunk = first;
  if (arena->chunks != nullptr) {
    arena->cursor = arena->chunks->data;
    arena->end = (uint8_t *)arena->chunks + arena->chunks->size;
  } else {
    arena->cursor = arena->end = nullptr;
  }
}

/* Adds a chunk with room for `size` bytes. Returns false (setting the status) when the request
 * limit, the shared budget or the system allocator refuses. */
static bool grow(otlp_arena *arena, size_t size) {
  size_t header = offsetof(otlp_arena_chunk, data);
  if (size > SIZE_MAX - header - ALIGN) {
    arena->status = OTLP_ARENA_LIMIT;
    return false;
  }
  size_t needed = header + size;
  size_t chunk_size = arena->next_chunk > needed ? arena->next_chunk : needed;
  size_t room = arena->limit > arena->reserved ? arena->limit - arena->reserved : 0;
  if (chunk_size > room)
    chunk_size = needed; /* the doubling would overshoot: take just enough */
  if (chunk_size > room) {
    arena->status = OTLP_ARENA_LIMIT;
    return false;
  }
  if (arena->budget != nullptr && !otlp_budget_reserve(arena->budget, chunk_size)) {
    arena->status = OTLP_ARENA_NOMEM;
    return false;
  }
  otlp_arena_chunk *chunk = grpc_malloc(chunk_size);
  if (chunk == nullptr) {
    if (arena->budget != nullptr)
      otlp_budget_release(arena->budget, chunk_size);
    arena->status = OTLP_ARENA_NOMEM;
    return false;
  }
  chunk->size = chunk_size;
  chunk->next = arena->chunks;
  arena->chunks = chunk;
  arena->reserved += chunk_size;
  arena->cursor = chunk->data;
  arena->end = (uint8_t *)chunk + chunk_size;
  if (arena->next_chunk <= SIZE_MAX / 2)
    arena->next_chunk *= 2;
  return true;
}

void *otlp_arena_alloc(otlp_arena *arena, size_t size) {
  if (arena->status != OTLP_ARENA_OK)
    return nullptr;
  if (size == 0)
    size = 1;
  size_t rounded = align_up(size);
  if (rounded < size) {
    arena->status = OTLP_ARENA_LIMIT;
    return nullptr;
  }
  if (arena->cursor == nullptr || (size_t)(arena->end - arena->cursor) < rounded) {
    if (!grow(arena, rounded))
      return nullptr;
  }
  void *p = arena->cursor;
  arena->cursor += rounded;
  arena->used += rounded;
  if (arena->used > arena->peak_used)
    arena->peak_used = arena->used;
  return p;
}

otlp_arena_status otlp_arena_status_get(const otlp_arena *arena) {
  return arena->status;
}

void otlp_arena_end(otlp_arena *arena) {
  /* keep the largest chunk that fits in keep_bytes, release the rest */
  otlp_arena_chunk *keep = nullptr;
  for (otlp_arena_chunk *c = arena->chunks; c != nullptr; c = c->next) {
    if (c->size <= arena->keep_bytes && (keep == nullptr || c->size > keep->size))
      keep = c;
  }
  otlp_arena_chunk *chunk = arena->chunks;
  while (chunk != nullptr) {
    otlp_arena_chunk *next = chunk->next;
    if (chunk != keep)
      chunk_release(arena, chunk);
    chunk = next;
  }
  if (keep != nullptr)
    keep->next = nullptr;
  arena->chunks = keep;
  arena->cursor = arena->end = nullptr;
  arena->used = 0;
}

void otlp_arena_destroy(otlp_arena *arena) {
  while (arena->chunks != nullptr) {
    otlp_arena_chunk *next = arena->chunks->next;
    chunk_release(arena, arena->chunks);
    arena->chunks = next;
  }
  arena->cursor = arena->end = nullptr;
  arena->used = 0;
}
