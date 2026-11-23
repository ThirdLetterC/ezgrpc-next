#include "otlp_budget.h"

#include <assert.h>

void otlp_budget_init(otlp_budget *budget, size_t limit) {
  budget->limit = limit;
  atomic_init(&budget->used, 0);
  atomic_init(&budget->peak, 0);
  atomic_init(&budget->denied, 0);
}

static void raise_peak(otlp_budget *budget, size_t value) {
  size_t peak = atomic_load_explicit(&budget->peak, memory_order_relaxed);
  while (value > peak &&
         !atomic_compare_exchange_weak_explicit(&budget->peak, &peak, value, memory_order_relaxed,
                                                memory_order_relaxed)) {
  }
}

bool otlp_budget_reserve(otlp_budget *budget, size_t bytes) {
  size_t used = atomic_load_explicit(&budget->used, memory_order_relaxed);
  for (;;) {
    if (bytes > SIZE_MAX - used || (budget->limit != 0 && used + bytes > budget->limit)) {
      atomic_fetch_add_explicit(&budget->denied, 1, memory_order_relaxed);
      return false;
    }
    if (atomic_compare_exchange_weak_explicit(&budget->used, &used, used + bytes,
                                              memory_order_relaxed, memory_order_relaxed))
      break;
  }
  raise_peak(budget, used + bytes);
  return true;
}

void otlp_budget_release(otlp_budget *budget, size_t bytes) {
  [[maybe_unused]] size_t before =
      atomic_fetch_sub_explicit(&budget->used, bytes, memory_order_relaxed);
  assert(before >= bytes);
}

size_t otlp_budget_used(const otlp_budget *budget) {
  return atomic_load_explicit(&budget->used, memory_order_relaxed);
}
