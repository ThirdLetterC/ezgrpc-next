/* Unit tests for grpc_msg_queue (include/internal/grpc_msg_queue.h): FIFO order across buffer
 * growth and ring wrap-around, ownership on pop and clear, and reuse after clear. Allocation
 * failures are covered by the oom test. */
#include <stdint.h>
#include <string.h>
#include "grpc.h"
#include "grpc_msg_queue.h"
#include "test.h"

void setUp(void) {}
void tearDown(void) {}

/* A one-byte message holding id, so order can be checked after popping. */
static grpc_message *msg_with_id(uint8_t id) {
  grpc_message *m = grpc_message_new(&id, 1);
  TEST_ASSERT_NOT_NULL(m);
  return m;
}

/* Pops one message, checks it carries id and frees it. */
static void pop_expect(grpc_msg_queue *q, uint8_t id) {
  grpc_message *m = msg_queue_pop(q);
  TEST_ASSERT_NOT_NULL(m);
  uint8_t got = m->data[0];
  grpc_message_free(m);
  TEST_ASSERT_EQUAL_UINT8(id, got);
}

void test_msg_queue_empty(void) {
  grpc_msg_queue q = {0};
  TEST_ASSERT_TRUE(msg_queue_is_empty(&q));
  TEST_ASSERT_EQUAL_size_t(0, msg_queue_count(&q));
  TEST_ASSERT_NULL(msg_queue_peek(&q));
  TEST_ASSERT_NULL(msg_queue_pop(&q));
  msg_queue_clear(&q); /* clearing an empty, never-allocated queue is a no-op */
  TEST_ASSERT_NULL(q.items);
  TEST_ASSERT_EQUAL_size_t(0, q.cap);
}

void test_msg_queue_fifo_across_growth(void) {
  grpc_msg_queue q = {0};
  for (uint8_t i = 0; i < 100; i++) {
    TEST_ASSERT_EQUAL_INT(GRPC_OK, msg_queue_push(&q, msg_with_id(i)));
    TEST_ASSERT_EQUAL_size_t((size_t)i + 1, msg_queue_count(&q));
  }
  TEST_ASSERT_TRUE(q.cap >= 100);
  TEST_ASSERT_EQUAL_UINT8(0, msg_queue_peek(&q)->data[0]);
  for (uint8_t i = 0; i < 100; i++)
    pop_expect(&q, i);
  TEST_ASSERT_TRUE(msg_queue_is_empty(&q));
  TEST_ASSERT_NULL(msg_queue_pop(&q));
  msg_queue_clear(&q);
}

void test_msg_queue_wraparound_then_grow(void) {
  grpc_msg_queue q = {0};
  /* fill the first buffer (4 slots), free two slots at the front, then push past the end so the
   * queue wraps, and push more so it grows while wrapped */
  for (uint8_t i = 0; i < 4; i++)
    TEST_ASSERT_EQUAL_INT(GRPC_OK, msg_queue_push(&q, msg_with_id(i)));
  TEST_ASSERT_EQUAL_size_t(4, q.cap);
  pop_expect(&q, 0);
  pop_expect(&q, 1);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, msg_queue_push(&q, msg_with_id(4)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, msg_queue_push(&q, msg_with_id(5)));
  TEST_ASSERT_EQUAL_size_t(4, q.cap); /* wrapped, not grown */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, msg_queue_push(&q, msg_with_id(6)));
  TEST_ASSERT_EQUAL_size_t(8, q.cap);
  TEST_ASSERT_EQUAL_size_t(5, msg_queue_count(&q));
  for (uint8_t i = 2; i <= 6; i++)
    pop_expect(&q, i);

  /* steady push/pop keeps cycling through the buffer without growing it */
  for (uint8_t i = 0; i < 200; i++) {
    TEST_ASSERT_EQUAL_INT(GRPC_OK, msg_queue_push(&q, msg_with_id(i)));
    pop_expect(&q, i);
  }
  TEST_ASSERT_EQUAL_size_t(8, q.cap);
  msg_queue_clear(&q);
}

void test_msg_queue_clear_frees_and_reuses(void) {
  grpc_msg_queue q = {0};
  for (uint8_t i = 0; i < 10; i++)
    TEST_ASSERT_EQUAL_INT(GRPC_OK, msg_queue_push(&q, msg_with_id(i)));
  pop_expect(&q, 0);
  msg_queue_clear(&q); /* frees the nine remaining messages (checked by ASan/LSan builds) */
  TEST_ASSERT_TRUE(msg_queue_is_empty(&q));
  TEST_ASSERT_NULL(q.items);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, msg_queue_push(&q, msg_with_id(42)));
  TEST_ASSERT_EQUAL_size_t(1, msg_queue_count(&q));
  pop_expect(&q, 42);
  msg_queue_clear(&q);
}

void test_msg_queue_reserve(void) {
  grpc_msg_queue q = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, msg_queue_reserve(&q, 0));
  TEST_ASSERT_NULL(q.items);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, msg_queue_reserve(&q, 5));
  TEST_ASSERT_EQUAL_size_t(8, q.cap);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, msg_queue_reserve(&q, 3)); /* never shrinks */
  TEST_ASSERT_EQUAL_size_t(8, q.cap);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_NOMEM, msg_queue_reserve(&q, SIZE_MAX));
  TEST_ASSERT_EQUAL_size_t(8, q.cap); /* unchanged on failure */
  msg_queue_clear(&q);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_msg_queue_empty);
  RUN_TEST(test_msg_queue_fifo_across_growth);
  RUN_TEST(test_msg_queue_wraparound_then_grow);
  RUN_TEST(test_msg_queue_clear_frees_and_reuses);
  RUN_TEST(test_msg_queue_reserve);
  return UNITY_END();
}
