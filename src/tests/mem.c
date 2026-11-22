#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "grpc_mem.h"
#include "unity.h"

void setUp(void) {}

/* runs even when an assertion failed while the mock allocator was installed */
void tearDown(void) {
  grpc_set_allocator(nullptr);
}

void test_allocator_name(void) {
  const char *name = grpc_get_allocator_name();
  TEST_ASSERT_NOT_NULL(name);
#if defined(GRPC_USE_MIMALLOC)
  TEST_ASSERT_EQUAL_STRING("mimalloc", name);
#elif defined(GRPC_USE_JEMALLOC)
  TEST_ASSERT_EQUAL_STRING("jemalloc", name);
#else
  TEST_ASSERT_EQUAL_STRING("libc", name);
#endif
}

void test_allocator_basic_operations(void) {
  // Test grpc_malloc & grpc_free
  void *p1 = grpc_malloc(128);
  TEST_ASSERT_NOT_NULL(p1);
  memset(p1, 0xAA, 128);
  grpc_free(p1);

  // Test grpc_calloc
  int *p2 = (int *)grpc_calloc(16, sizeof(int));
  TEST_ASSERT_NOT_NULL(p2);
  for (int i = 0; i < 16; i++) {
    TEST_ASSERT_EQUAL_INT(0, p2[i]);
  }

  // Test grpc_realloc
  p2 = (int *)grpc_realloc(p2, 32 * sizeof(int));
  TEST_ASSERT_NOT_NULL(p2);
  for (int i = 0; i < 16; i++) {
    TEST_ASSERT_EQUAL_INT(0, p2[i]);
  }
  grpc_free(p2);

  // Test grpc_strdup
  char *str = grpc_strdup("hello grpc");
  TEST_ASSERT_NOT_NULL(str);
  TEST_ASSERT_EQUAL_STRING("hello grpc", str);
  grpc_free(str);

  // Test grpc_strndup
  char *nstr = grpc_strndup("hello grpc", 5);
  TEST_ASSERT_NOT_NULL(nstr);
  TEST_ASSERT_EQUAL_STRING("hello", nstr);
  grpc_free(nstr);

  // Test grpc_aligned_alloc (64 bytes aligned)
  void *aligned = grpc_aligned_alloc(64, 256);
  TEST_ASSERT_NOT_NULL(aligned);
  TEST_ASSERT_EQUAL_UINT64(0, (uintptr_t)aligned % 64);
  grpc_free(aligned);
}

/* The mock counts calls and forwards every one of them to the allocator that was installed
 * before it (a copy: grpc_get_allocator() points to the live table). Blocks are therefore always
 * allocated and freed by the same family, which matters where the default is _aligned_malloc
 * (MSVC) or mimalloc rather than libc malloc. */
static grpc_allocator g_orig;
static size_t g_mock_malloc_count;
static size_t g_mock_calloc_count;
static size_t g_mock_realloc_count;
static size_t g_mock_free_count;
static size_t g_mock_aligned_count;

static void *mock_malloc(size_t size) {
  g_mock_malloc_count++;
  return g_orig.malloc(size);
}

static void *mock_calloc(size_t nmemb, size_t size) {
  g_mock_calloc_count++;
  return g_orig.calloc(nmemb, size);
}

static void *mock_realloc(void *ptr, size_t size) {
  g_mock_realloc_count++;
  return g_orig.realloc(ptr, size);
}

static void mock_free(void *ptr) {
  g_mock_free_count++;
  g_orig.free(ptr);
}

static void *mock_aligned_alloc(size_t alignment, size_t size) {
  g_mock_aligned_count++;
  return g_orig.aligned_alloc(alignment, size);
}

void test_custom_allocator_switch(void) {
  const grpc_allocator *live = grpc_get_allocator();
  TEST_ASSERT_NOT_NULL(live);
  g_orig = *live;
  TEST_ASSERT_NOT_NULL(g_orig.malloc);
  TEST_ASSERT_NOT_NULL(g_orig.calloc);
  TEST_ASSERT_NOT_NULL(g_orig.realloc);
  TEST_ASSERT_NOT_NULL(g_orig.free);
  TEST_ASSERT_NOT_NULL(g_orig.aligned_alloc);

  grpc_allocator mock_alloc = {
      .malloc = mock_malloc,
      .calloc = mock_calloc,
      .realloc = mock_realloc,
      .free = mock_free,
      .aligned_alloc = mock_aligned_alloc,
  };
  g_mock_malloc_count = g_mock_calloc_count = g_mock_realloc_count = 0;
  g_mock_free_count = g_mock_aligned_count = 0;

  grpc_set_allocator(&mock_alloc);
  const grpc_allocator *current = grpc_get_allocator();
  TEST_ASSERT_TRUE(current == live); /* the live table, now holding the mock */
  TEST_ASSERT_TRUE(current->malloc == mock_malloc);
  TEST_ASSERT_TRUE(current->calloc == mock_calloc);
  TEST_ASSERT_TRUE(current->realloc == mock_realloc);
  TEST_ASSERT_TRUE(current->free == mock_free);
  TEST_ASSERT_TRUE(current->aligned_alloc == mock_aligned_alloc);
  /* the name still reports the build-time default */
#if defined(GRPC_USE_MIMALLOC)
  TEST_ASSERT_EQUAL_STRING("mimalloc", grpc_get_allocator_name());
#elif defined(GRPC_USE_JEMALLOC)
  TEST_ASSERT_EQUAL_STRING("jemalloc", grpc_get_allocator_name());
#else
  TEST_ASSERT_EQUAL_STRING("libc", grpc_get_allocator_name());
#endif

  /* every grpc_* allocation function routes through the installed table */
  void *ptr = grpc_malloc(64);
  TEST_ASSERT_NOT_NULL(ptr);
  TEST_ASSERT_EQUAL_UINT64(1, g_mock_malloc_count);
  grpc_free(ptr);
  TEST_ASSERT_EQUAL_UINT64(1, g_mock_free_count);

  int *arr = grpc_calloc(4, sizeof(int));
  TEST_ASSERT_NOT_NULL(arr);
  TEST_ASSERT_EQUAL_UINT64(1, g_mock_calloc_count);
  arr = grpc_realloc(arr, 64 * sizeof(int));
  TEST_ASSERT_NOT_NULL(arr);
  TEST_ASSERT_EQUAL_UINT64(1, g_mock_realloc_count);
  grpc_free(arr);
  TEST_ASSERT_EQUAL_UINT64(2, g_mock_free_count);

  void *aligned = grpc_aligned_alloc(64, 128);
  TEST_ASSERT_NOT_NULL(aligned);
  TEST_ASSERT_EQUAL_UINT64(0, (uintptr_t)aligned % 64);
  TEST_ASSERT_EQUAL_UINT64(1, g_mock_aligned_count);
  grpc_free(aligned);
  TEST_ASSERT_EQUAL_UINT64(3, g_mock_free_count);

  char *dup = grpc_strdup("routed");
  TEST_ASSERT_NOT_NULL(dup);
  TEST_ASSERT_EQUAL_STRING("routed", dup);
  TEST_ASSERT_EQUAL_UINT64(2, g_mock_malloc_count);
  grpc_free(dup);
  TEST_ASSERT_EQUAL_UINT64(4, g_mock_free_count);

  /* members left nullptr keep their current value */
  grpc_set_allocator(&(grpc_allocator){.malloc = g_orig.malloc});
  TEST_ASSERT_TRUE(current->malloc == g_orig.malloc);
  TEST_ASSERT_TRUE(current->free == mock_free);
  TEST_ASSERT_TRUE(current->calloc == mock_calloc);

  /* nullptr restores the default */
  grpc_set_allocator(nullptr);
  TEST_ASSERT_TRUE(current->malloc == g_orig.malloc);
  TEST_ASSERT_TRUE(current->calloc == g_orig.calloc);
  TEST_ASSERT_TRUE(current->realloc == g_orig.realloc);
  TEST_ASSERT_TRUE(current->free == g_orig.free);
  TEST_ASSERT_TRUE(current->aligned_alloc == g_orig.aligned_alloc);
  ptr = grpc_malloc(8);
  TEST_ASSERT_NOT_NULL(ptr);
  grpc_free(ptr);
  TEST_ASSERT_EQUAL_UINT64(2, g_mock_malloc_count);
  TEST_ASSERT_EQUAL_UINT64(4, g_mock_free_count);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_allocator_name);
  RUN_TEST(test_allocator_basic_operations);
  RUN_TEST(test_custom_allocator_switch);
  return UNITY_END();
}
