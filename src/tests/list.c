#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "grpc_list.h"
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

void test_list_creation(void) {
  grpc_list *list = grpc_list_new();
  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_free(list);
}

void test_list_push_and_pop_back_1(void) {
  grpc_list *list = grpc_list_new();
  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_push_back(list, (void *)"Alpha");
  TEST_ASSERT_EQUAL_UINT64(1, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_MEMORY("Alpha", grpc_list_pop_back(list), 6);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_free(list);
}

void test_list_push_and_pop_back_2(void) {
  grpc_list *list = grpc_list_new();
  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_push_back(list, (void *)"Alpha");
  TEST_ASSERT_EQUAL_UINT64(1, (uint64_t)grpc_list_count(list));
  grpc_list_push_back(list, (void *)"Beta");
  TEST_ASSERT_EQUAL_UINT64(2, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_MEMORY("Beta", grpc_list_pop_back(list), 5);
  TEST_ASSERT_EQUAL_UINT64(1, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_MEMORY("Alpha", grpc_list_pop_back(list), 6);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_free(list);
}

void test_list_push_and_pop_front_1(void) {
  grpc_list *list = grpc_list_new();
  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_push_front(list, (void *)"Alpha");
  TEST_ASSERT_EQUAL_UINT64(1, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_MEMORY("Alpha", grpc_list_pop_front(list), 6);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_free(list);
}

void test_list_push_and_pop_front_2(void) {
  grpc_list *list = grpc_list_new();
  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_push_front(list, (void *)"Alpha");
  TEST_ASSERT_EQUAL_UINT64(1, (uint64_t)grpc_list_count(list));
  grpc_list_push_front(list, (void *)"Beta");
  TEST_ASSERT_EQUAL_UINT64(2, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_MEMORY("Beta", grpc_list_pop_front(list), 5);
  TEST_ASSERT_EQUAL_UINT64(1, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_MEMORY("Alpha", grpc_list_pop_front(list), 6);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_free(list);
}

void test_list_concat(void) {
  grpc_list *dst = grpc_list_new();
  grpc_list *src = grpc_list_new();
  grpc_list_push_back(dst, (void *)"A");
  grpc_list_push_back(src, (void *)"B");
  grpc_list_concat_and_empty_src(dst, src);
  TEST_ASSERT_EQUAL_UINT64(2, (uint64_t)grpc_list_count(dst));
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(src));
  TEST_ASSERT_EQUAL_MEMORY("A", grpc_list_pop_front(dst), 2);
  TEST_ASSERT_EQUAL_MEMORY("B", grpc_list_pop_front(dst), 2);
  grpc_list_free(dst);
  grpc_list_free(src);
}

void test_list_random(void) {
  grpc_list *list = grpc_list_new();
  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_NULL(grpc_list_pop_back(list));
  TEST_ASSERT_NULL(grpc_list_pop_front(list));

  grpc_list_push_front(list, (void *)"Alpha");
  TEST_ASSERT_EQUAL_MEMORY("Alpha", grpc_list_pop_back(list), 6);
  TEST_ASSERT_NULL(grpc_list_pop_back(list));
  TEST_ASSERT_NULL(grpc_list_pop_front(list));
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));

  grpc_list_push_back(list, (void *)"Beta");
  grpc_list_push_front(list, (void *)"Gamma");
  TEST_ASSERT_EQUAL_MEMORY("Beta", grpc_list_pop_back(list), 5);
  TEST_ASSERT_EQUAL_MEMORY("Gamma", grpc_list_pop_back(list), 6);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_NULL(grpc_list_pop_back(list));
  TEST_ASSERT_NULL(grpc_list_pop_front(list));
  grpc_list_push_back(list, (void *)"Alpha");
  grpc_list_push_back(list, (void *)"Beta");
  grpc_list_push_back(list, (void *)"Gamma");

  TEST_ASSERT_EQUAL_UINT64(3, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_MEMORY("Alpha", grpc_list_pop_front(list), 6);
  TEST_ASSERT_EQUAL_MEMORY("Beta", grpc_list_pop_front(list), 5);
  TEST_ASSERT_EQUAL_MEMORY("Gamma", grpc_list_pop_front(list), 6);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_NULL(grpc_list_pop_back(list));
  TEST_ASSERT_NULL(grpc_list_pop_front(list));

  grpc_list_free(list);
}

void foreach (const void *data, [[maybe_unused]] const void *userdata) {
  puts((const char *)data);
}

static int cmp_str(const void *a, const void *b) {
  return strcmp((const char *)a, (const char *)b);
}

static void count_cb([[maybe_unused]] const void *data, const void *userdata) {
  int *cnt = (int *)(uintptr_t)userdata;
  (*cnt)++;
}

void test_list_peek_find_remove_and_free(void) {
  grpc_list *list = grpc_list_new();
  TEST_ASSERT_NULL(grpc_list_peek_front(list));
  TEST_ASSERT_NULL(grpc_list_peek_back(list));

  grpc_list_push_back(list, (void *)"one");
  grpc_list_push_back(list, (void *)"two");
  grpc_list_push_back(list, (void *)"three");
  int seen = 0;
  grpc_list_foreach(list, count_cb, &seen);
  TEST_ASSERT_EQUAL_INT(3, seen);
  grpc_list_free(nullptr);
  TEST_ASSERT_EQUAL_STRING("one", grpc_list_peek_front(list));
  TEST_ASSERT_EQUAL_STRING("three", grpc_list_peek_back(list));
  TEST_ASSERT_EQUAL_STRING("two", grpc_list_find(list, cmp_str, "two"));
  TEST_ASSERT_NULL(grpc_list_find(list, cmp_str, "missing"));

  TEST_ASSERT_EQUAL_STRING("two", grpc_list_remove(list, cmp_str, "two"));
  TEST_ASSERT_EQUAL_UINT64(2, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_STRING("one", grpc_list_remove(list, cmp_str, "one"));
  TEST_ASSERT_EQUAL_STRING("three", grpc_list_remove(list, cmp_str, "three"));
  TEST_ASSERT_NULL(grpc_list_remove(list, cmp_str, "three"));
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));

  /* Free non-empty list */
  grpc_list_push_back(list, (void *)"leftover1");
  grpc_list_push_back(list, (void *)"leftover2");
  grpc_list_free(list);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_list_creation);
  RUN_TEST(test_list_push_and_pop_back_1);
  RUN_TEST(test_list_push_and_pop_back_2);
  RUN_TEST(test_list_push_and_pop_front_1);
  RUN_TEST(test_list_push_and_pop_front_2);
  RUN_TEST(test_list_concat);
  RUN_TEST(test_list_random);
  RUN_TEST(test_list_peek_find_remove_and_free);
  return UNITY_END();
}
