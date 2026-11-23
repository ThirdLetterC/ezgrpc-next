#pragma once

/* otlp_threads.h - the library's pthreads subset plus dynamic condition variables
 *
 * internal/grpc_threads.h maps the POSIX threading subset the library uses onto Windows; it has
 * no pthread_cond_init()/pthread_cond_destroy(), which a condition variable inside a heap object
 * needs. These helpers fill that gap on both platforms.
 */

#include "internal/grpc_threads.h"

static inline void otlp_cond_init(pthread_cond_t *cond) {
#ifdef _WIN32
  InitializeConditionVariable(cond);
#else
  pthread_cond_init(cond, nullptr);
#endif
}

static inline void otlp_cond_destroy(pthread_cond_t *cond) {
#ifdef _WIN32
  (void)cond; /* Windows condition variables need no cleanup */
#else
  pthread_cond_destroy(cond);
#endif
}
