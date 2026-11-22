#pragma once

/* tls_ctx_pool.h - lifetime of server TLS contexts across hot reloads
 *
 * A server has one active TLS context, from which new connections create their sessions. A
 * reload installs a new one; the previous context must outlive the connections that still use
 * it (not every backend reference-counts its context, e.g. GnuTLS sessions borrow the
 * credentials), but no longer: each context counts its live sessions, and a replaced context is
 * freed as soon as its last session ends (or at once when it has none). Used for the TCP TLS
 * context and the HTTP/3 QUIC context. Loop thread only.
 */
#include <stddef.h>
#include "common.h"
#include "grpc_defs.h"
#include "grpc_error.h"
#include "grpc_mem.h"

typedef struct tls_ctx_entry {
  void *ctx;
  size_t live; /* sessions created from ctx and not freed yet */
} tls_ctx_entry;

typedef struct tls_ctx_pool {
  tls_ctx_entry active;   /* ctx is nullptr until the first install */
  tls_ctx_entry *retired; /* replaced contexts that still have live sessions */
  size_t nb_retired;
  size_t cap_retired;
  void (*free_ctx)(void *ctx);
} tls_ctx_pool;

/* Makes room to retire the active context, so that a following tls_ctx_pool_install() cannot
 * fail. Returns GRPC_OK or GRPC_ERR_NOMEM (the pool is unchanged). */
static inline grpc_err tls_ctx_pool_reserve(tls_ctx_pool *pool) {
  if (pool->nb_retired < pool->cap_retired)
    return GRPC_OK;
  size_t cap = pool->cap_retired > 0 ? pool->cap_retired * 2 : 4;
  tls_ctx_entry *r = grpc_realloc_array(pool->retired, cap, sizeof(*r));
  if (r == nullptr)
    return GRPC_ERR_NOMEM;
  pool->retired = r;
  pool->cap_retired = cap;
  return GRPC_OK;
}

/* Makes ctx the active context; the previous one is freed if no session uses it, otherwise
 * retired until its last session ends. Requires a successful tls_ctx_pool_reserve() when a
 * context is already active. Never fails. */
static inline void tls_ctx_pool_install(tls_ctx_pool *pool, void *ctx, void (*free_ctx)(void *)) {
  pool->free_ctx = free_ctx;
  if (pool->active.ctx != nullptr) {
    if (pool->active.live == 0)
      pool->free_ctx(pool->active.ctx);
    else
      pool->retired[pool->nb_retired++] = pool->active;
  }
  pool->active = (tls_ctx_entry){.ctx = ctx};
}

/* Returns the active context and counts a new session on it (nullptr when none is installed). */
static inline void *tls_ctx_pool_acquire(tls_ctx_pool *pool) {
  if (pool->active.ctx == nullptr)
    return nullptr;
  pool->active.live++;
  return pool->active.ctx;
}

/* Ends a session created from ctx (as returned by tls_ctx_pool_acquire()); a retired context
 * whose last session this was is freed. */
static inline void tls_ctx_pool_release(tls_ctx_pool *pool, void *ctx) {
  if (ctx == nullptr)
    return;
  if (ctx == pool->active.ctx) {
    if (pool->active.live > 0)
      pool->active.live--;
    return;
  }
  for (size_t i = 0; i < pool->nb_retired; i++) {
    if (pool->retired[i].ctx == ctx) {
      if (--pool->retired[i].live == 0) {
        pool->free_ctx(ctx);
        pool->retired[i] = pool->retired[--pool->nb_retired];
      }
      return;
    }
  }
}

/* Number of contexts the pool holds (the active one plus retired ones still in use). */
static inline size_t tls_ctx_pool_count(const tls_ctx_pool *pool) {
  return (pool->active.ctx != nullptr ? 1 : 0) + pool->nb_retired;
}

/* Frees every context (when the server is destroyed, after all sessions). */
static inline void tls_ctx_pool_free(tls_ctx_pool *pool) {
  if (pool->active.ctx != nullptr)
    pool->free_ctx(pool->active.ctx);
  for (size_t i = 0; i < pool->nb_retired; i++)
    pool->free_ctx(pool->retired[i].ctx);
  grpc_free(pool->retired);
  *pool = (tls_ctx_pool){0};
}
