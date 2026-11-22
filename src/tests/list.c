#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include "grpc_list.h"
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

/* The list stores pointers, so the tests check identity (the very pointer pushed comes back),
 * using distinct named objects rather than string literals the compiler may merge or split. */
static char ALPHA[] = "Alpha";
static char BETA[] = "Beta";
static char GAMMA[] = "Gamma";
static char A[] = "A";
static char B[] = "B";
static char ONE[] = "one";
static char TWO[] = "two";
static char THREE[] = "three";

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
  grpc_list_push_back(list, ALPHA);
  TEST_ASSERT_EQUAL_UINT64(1, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_PTR(ALPHA, grpc_list_pop_back(list));
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_free(list);
}

void test_list_push_and_pop_back_2(void) {
  grpc_list *list = grpc_list_new();
  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_push_back(list, ALPHA);
  TEST_ASSERT_EQUAL_UINT64(1, (uint64_t)grpc_list_count(list));
  grpc_list_push_back(list, BETA);
  TEST_ASSERT_EQUAL_UINT64(2, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_PTR(BETA, grpc_list_pop_back(list));
  TEST_ASSERT_EQUAL_UINT64(1, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_PTR(ALPHA, grpc_list_pop_back(list));
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_free(list);
}

void test_list_push_and_pop_front_1(void) {
  grpc_list *list = grpc_list_new();
  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_push_front(list, ALPHA);
  TEST_ASSERT_EQUAL_UINT64(1, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_PTR(ALPHA, grpc_list_pop_front(list));
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_free(list);
}

void test_list_push_and_pop_front_2(void) {
  grpc_list *list = grpc_list_new();
  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_push_front(list, ALPHA);
  TEST_ASSERT_EQUAL_UINT64(1, (uint64_t)grpc_list_count(list));
  grpc_list_push_front(list, BETA);
  TEST_ASSERT_EQUAL_UINT64(2, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_PTR(BETA, grpc_list_pop_front(list));
  TEST_ASSERT_EQUAL_UINT64(1, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_PTR(ALPHA, grpc_list_pop_front(list));
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  grpc_list_free(list);
}

void test_list_concat(void) {
  grpc_list *dst = grpc_list_new();
  grpc_list *src = grpc_list_new();
  TEST_ASSERT_NOT_NULL(dst);
  TEST_ASSERT_NOT_NULL(src);
  grpc_list_push_back(dst, A);
  grpc_list_push_back(src, B);
  grpc_list_concat_and_empty_src(dst, src);
  TEST_ASSERT_EQUAL_UINT64(2, (uint64_t)grpc_list_count(dst));
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(src));
  TEST_ASSERT_EQUAL_PTR(A, grpc_list_pop_front(dst));
  TEST_ASSERT_EQUAL_PTR(B, grpc_list_pop_front(dst));
  grpc_list_free(dst);
  grpc_list_free(src);
}

void test_list_random(void) {
  grpc_list *list = grpc_list_new();
  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_NULL(grpc_list_pop_back(list));
  TEST_ASSERT_NULL(grpc_list_pop_front(list));

  grpc_list_push_front(list, ALPHA);
  TEST_ASSERT_EQUAL_PTR(ALPHA, grpc_list_pop_back(list));
  TEST_ASSERT_NULL(grpc_list_pop_back(list));
  TEST_ASSERT_NULL(grpc_list_pop_front(list));
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));

  grpc_list_push_back(list, BETA);
  grpc_list_push_front(list, GAMMA);
  TEST_ASSERT_EQUAL_PTR(BETA, grpc_list_pop_back(list));
  TEST_ASSERT_EQUAL_PTR(GAMMA, grpc_list_pop_back(list));
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_NULL(grpc_list_pop_back(list));
  TEST_ASSERT_NULL(grpc_list_pop_front(list));
  grpc_list_push_back(list, ALPHA);
  grpc_list_push_back(list, BETA);
  grpc_list_push_back(list, GAMMA);

  TEST_ASSERT_EQUAL_UINT64(3, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_EQUAL_PTR(ALPHA, grpc_list_pop_front(list));
  TEST_ASSERT_EQUAL_PTR(BETA, grpc_list_pop_front(list));
  TEST_ASSERT_EQUAL_PTR(GAMMA, grpc_list_pop_front(list));
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_NULL(grpc_list_pop_back(list));
  TEST_ASSERT_NULL(grpc_list_pop_front(list));

  grpc_list_free(list);
}

typedef struct collected {
  const void *items[8];
  size_t n;
} collected;

/* records the visited pointers in order */
static void collect_cb(const void *data, const void *userdata) {
  collected *c = (collected *)(uintptr_t)userdata;
  if (c->n < sizeof(c->items) / sizeof(c->items[0]))
    c->items[c->n] = data;
  c->n++;
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
  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_NULL(grpc_list_peek_front(list));
  TEST_ASSERT_NULL(grpc_list_peek_back(list));

  grpc_list_push_back(list, ONE);
  grpc_list_push_back(list, TWO);
  grpc_list_push_back(list, THREE);
  int seen = 0;
  grpc_list_foreach(list, count_cb, &seen);
  TEST_ASSERT_EQUAL_INT(3, seen);
  collected order = {};
  grpc_list_foreach(list, collect_cb, &order);
  TEST_ASSERT_EQUAL_UINT64(3, (uint64_t)order.n);
  TEST_ASSERT_EQUAL_PTR(ONE, order.items[0]);
  TEST_ASSERT_EQUAL_PTR(TWO, order.items[1]);
  TEST_ASSERT_EQUAL_PTR(THREE, order.items[2]);
  grpc_list_free(nullptr);
  TEST_ASSERT_EQUAL_PTR(ONE, grpc_list_peek_front(list));
  TEST_ASSERT_EQUAL_PTR(THREE, grpc_list_peek_back(list));
  /* find and remove compare by value (the key is a different buffer) and return the stored
   * pointer */
  char key[] = "two";
  TEST_ASSERT_EQUAL_PTR(TWO, grpc_list_find(list, cmp_str, key));
  TEST_ASSERT_NULL(grpc_list_find(list, cmp_str, "missing"));

  TEST_ASSERT_EQUAL_PTR(TWO, grpc_list_remove(list, cmp_str, key));
  TEST_ASSERT_EQUAL_UINT64(2, (uint64_t)grpc_list_count(list));
  TEST_ASSERT_NULL(grpc_list_find(list, cmp_str, key));
  TEST_ASSERT_EQUAL_PTR(ONE, grpc_list_remove(list, cmp_str, "one"));
  TEST_ASSERT_EQUAL_PTR(THREE, grpc_list_remove(list, cmp_str, "three"));
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
