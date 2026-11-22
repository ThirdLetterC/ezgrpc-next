/*
 * test.h - the small unit-test harness used by src/tests.
 *
 * It provides the API the tests are written against: RUN_TEST(), UNITY_BEGIN() / UNITY_END(),
 * setUp() / tearDown() fixtures that every test file defines, and the TEST_ASSERT_* macros (the
 * names follow the Unity framework this replaced). It is header-only: include it from exactly
 * one source file per test executable, the one with main().
 *
 * Each test prints "file:line:test:PASS", "...:FAIL: <detail>[. <message>]" or
 * "...:IGNORE: <message>", and UNITY_END() prints "N Tests F Failures I Ignored" followed by OK or
 * FAIL and returns the failure count, which main() returns as the exit status.
 *
 * A failed assertion longjmps out of the running test; tearDown() still runs afterwards (and also
 * runs when setUp() fails). Assertions are therefore only allowed on the thread that called
 * RUN_TEST(). A failure on any other thread, or outside a test, prints the failure and aborts the
 * process rather than jumping onto another thread's stack. Arguments are evaluated exactly once.
 */
#ifndef GRPC_TESTS_TEST_H
#define GRPC_TESTS_TEST_H

#include <setjmp.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_MSC_VER) && !defined(__clang__)
#define TEST_H_THREAD_LOCAL __declspec(thread)
#else
/* thread_local is a keyword only from GCC 13 in C23 */
#define TEST_H_THREAD_LOCAL _Thread_local
#endif

/* Defined by every test file: run before and after each test. */
void setUp(void);
void tearDown(void);

static struct {
  const char *test_name; /* the running test, or NULL between tests */
  unsigned tests, failures, ignored;
  bool failed, ignored_now;
  jmp_buf jump;
} test_h_state;

/* true on the thread inside RUN_TEST(), the only one allowed to longjmp out of a test */
static TEST_H_THREAD_LOCAL bool test_h_on_runner;

enum test_h_op { TEST_H_EQ, TEST_H_NE, TEST_H_GT, TEST_H_LT };

/* Prints "file:line:test:" for an assertion at file:line. */
static inline void test_h_prefix(const char *file, int line) {
  printf("%s:%d:%s:", file, line,
         test_h_state.test_name != NULL ? test_h_state.test_name : "(outside a test)");
}

/* Ends a failure line with the optional user message and leaves the test (or the process, when
 * not on the test thread). */
static inline void test_h_leave(const char *msg, bool ignore) {
  if (msg != NULL)
    printf(". %s", msg);
  printf("\n");
  fflush(stdout);
  if (!test_h_on_runner) {
    fputs("test.h: assertion outside RUN_TEST() or on another thread; aborting\n", stderr);
    fflush(stderr);
    abort();
  }
  if (ignore)
    test_h_state.ignored_now = true;
  else
    test_h_state.failed = true;
  longjmp(test_h_state.jump, 1);
}

/* Fails with "FAIL: <what>" (or "FAIL: <msg>" when what is NULL) unless ok. */
static inline void test_h_check(bool ok, const char *what, const char *msg, const char *file,
                                int line) {
  if (ok)
    return;
  test_h_prefix(file, line);
  if (what == NULL) {
    printf("FAIL: %s", msg != NULL ? msg : "");
    msg = NULL;
  } else {
    printf("FAIL: %s", what);
  }
  test_h_leave(msg, false);
}

static inline const char *test_h_op_text(enum test_h_op op) {
  switch (op) {
  case TEST_H_NE:
    return "Not-Equal to ";
  case TEST_H_GT:
    return "Greater Than ";
  case TEST_H_LT:
    return "Less Than ";
  default:
    return "";
  }
}

/* Signed comparison: EQ/NE compare actual with expected, GT/LT require actual > / < threshold. */
static inline void test_h_signed(long long expected, long long actual, enum test_h_op op,
                                 const char *msg, const char *file, int line) {
  bool ok = op == TEST_H_EQ   ? actual == expected
            : op == TEST_H_NE ? actual != expected
            : op == TEST_H_GT ? actual > expected
                              : actual < expected;
  if (ok)
    return;
  test_h_prefix(file, line);
  printf("FAIL: Expected %s%lld Was %lld", test_h_op_text(op), expected, actual);
  test_h_leave(msg, false);
}

/* Unsigned comparison as test_h_signed(); hex_digits > 0 prints 0x-prefixed values. */
static inline void test_h_unsigned(unsigned long long expected, unsigned long long actual,
                                   enum test_h_op op, int hex_digits, const char *msg,
                                   const char *file, int line) {
  bool ok = op == TEST_H_EQ   ? actual == expected
            : op == TEST_H_NE ? actual != expected
            : op == TEST_H_GT ? actual > expected
                              : actual < expected;
  if (ok)
    return;
  test_h_prefix(file, line);
  if (hex_digits > 0)
    printf("FAIL: Expected %s0x%0*llX Was 0x%0*llX", test_h_op_text(op), hex_digits, expected,
           hex_digits, actual);
  else
    printf("FAIL: Expected %s%llu Was %llu", test_h_op_text(op), expected, actual);
  test_h_leave(msg, false);
}

static inline void test_h_ptr(const void *expected, const void *actual, const char *msg,
                              const char *file, int line) {
  if (expected == actual)
    return;
  test_h_prefix(file, line);
  printf("FAIL: Expected %p Was %p", expected, actual);
  test_h_leave(msg, false);
}

/* NUL-terminated strings; two NULLs are equal. */
static inline void test_h_string(const char *expected, const char *actual, const char *msg,
                                 const char *file, int line) {
  if (expected == actual || (expected != NULL && actual != NULL && strcmp(expected, actual) == 0))
    return;
  test_h_prefix(file, line);
  printf("FAIL: Expected ");
  printf(expected != NULL ? "'%s'" : "%s", expected != NULL ? expected : "NULL");
  printf(" Was ");
  printf(actual != NULL ? "'%s'" : "%s", actual != NULL ? actual : "NULL");
  test_h_leave(msg, false);
}

/* len bytes; comparing zero bytes is a test bug and fails, as a NULL buffer does. */
static inline void test_h_memory(const void *expected, const void *actual, size_t len,
                                 const char *msg, const char *file, int line) {
  if (len == 0) {
    test_h_check(false, "Compared zero bytes", msg, file, line);
    return;
  }
  if (expected == actual)
    return;
  if (expected == NULL || actual == NULL) {
    test_h_check(false, expected == NULL ? "Expected NULL buffer" : "Was NULL buffer", msg, file,
                 line);
    return;
  }
  const unsigned char *e = expected, *a = actual;
  for (size_t i = 0; i < len; i++) {
    if (e[i] != a[i]) {
      test_h_prefix(file, line);
      printf("FAIL: Memory Mismatch. Byte %zu Expected 0x%02X Was 0x%02X", i, e[i], a[i]);
      test_h_leave(msg, false);
    }
  }
}

static inline void test_h_ignore(const char *msg, const char *file, int line) {
  test_h_prefix(file, line);
  printf("IGNORE: %s", msg != NULL ? msg : "");
  test_h_leave(NULL, true);
}

/* Runs one test: setUp() and the test, then tearDown() even if either failed. */
static inline void test_h_run(void (*fn)(void), const char *name, const char *file, int line) {
  test_h_state.test_name = name;
  test_h_state.failed = false;
  test_h_state.ignored_now = false;
  test_h_state.tests++;
  test_h_on_runner = true;
  if (setjmp(test_h_state.jump) == 0) {
    setUp();
    fn();
  }
  if (setjmp(test_h_state.jump) == 0)
    tearDown();
  if (test_h_state.failed) {
    test_h_state.failures++;
  } else if (test_h_state.ignored_now) {
    test_h_state.ignored++;
  } else {
    printf("%s:%d:%s:PASS\n", file, line, name);
  }
  fflush(stdout);
  test_h_on_runner = false;
  test_h_state.test_name = NULL;
}

static inline void test_h_begin(void) {
  test_h_state.tests = test_h_state.failures = test_h_state.ignored = 0;
}

/* Prints the summary and returns the number of failed tests. */
static inline int test_h_end(void) {
  printf("\n-----------------------\n%u Tests %u Failures %u Ignored \n%s\n", test_h_state.tests,
         test_h_state.failures, test_h_state.ignored, test_h_state.failures == 0 ? "OK" : "FAIL");
  fflush(stdout);
  return (int)test_h_state.failures;
}

#define UNITY_BEGIN() test_h_begin()
#define UNITY_END()   test_h_end()
#define RUN_TEST(fn)  test_h_run((fn), #fn, __FILE__, __LINE__)

#define TEST_FAIL_MESSAGE(m)   test_h_check(false, NULL, (m), __FILE__, __LINE__)
#define TEST_IGNORE_MESSAGE(m) test_h_ignore((m), __FILE__, __LINE__)

#define TEST_ASSERT_TRUE_MESSAGE(c, m) \
  test_h_check((c) ? true : false, "Expected TRUE Was FALSE", (m), __FILE__, __LINE__)
#define TEST_ASSERT_FALSE_MESSAGE(c, m) \
  test_h_check((c) ? false : true, "Expected FALSE Was TRUE", (m), __FILE__, __LINE__)
#define TEST_ASSERT_NULL_MESSAGE(p, m) \
  test_h_check((p) == NULL, "Expected NULL", (m), __FILE__, __LINE__)
#define TEST_ASSERT_NOT_NULL_MESSAGE(p, m) \
  test_h_check((p) != NULL, "Expected Non-NULL", (m), __FILE__, __LINE__)
#define TEST_ASSERT_NOT_EQUAL_MESSAGE(e, a, m) \
  test_h_check((e) != (a), "Expected Not-Equal", (m), __FILE__, __LINE__)

#define TEST_H_S(e, a, type, op, m) \
  test_h_signed((long long)(type)(e), (long long)(type)(a), (op), (m), __FILE__, __LINE__)
#define TEST_H_U(e, a, type, op, hex, m)                                                          \
  test_h_unsigned((unsigned long long)(type)(e), (unsigned long long)(type)(a), (op), (hex), (m), \
                  __FILE__, __LINE__)

#define TEST_ASSERT_EQUAL_INT_MESSAGE(e, a, m)           TEST_H_S(e, a, long long, TEST_H_EQ, m)
#define TEST_ASSERT_NOT_EQUAL_INT_MESSAGE(e, a, m)       TEST_H_S(e, a, long long, TEST_H_NE, m)
#define TEST_ASSERT_EQUAL_INT32_MESSAGE(e, a, m)         TEST_H_S(e, a, int32_t, TEST_H_EQ, m)
#define TEST_ASSERT_EQUAL_INT64_MESSAGE(e, a, m)         TEST_H_S(e, a, int64_t, TEST_H_EQ, m)
#define TEST_ASSERT_EQUAL_CHAR_MESSAGE(e, a, m)          TEST_H_S(e, a, char, TEST_H_EQ, m)
#define TEST_ASSERT_LESS_THAN_INT_MESSAGE(t, a, m)       TEST_H_S(t, a, long long, TEST_H_LT, m)
#define TEST_ASSERT_EQUAL_UINT8_MESSAGE(e, a, m)         TEST_H_U(e, a, uint8_t, TEST_H_EQ, 0, m)
#define TEST_ASSERT_EQUAL_UINT16_MESSAGE(e, a, m)        TEST_H_U(e, a, uint16_t, TEST_H_EQ, 0, m)
#define TEST_ASSERT_EQUAL_UINT32_MESSAGE(e, a, m)        TEST_H_U(e, a, uint32_t, TEST_H_EQ, 0, m)
#define TEST_ASSERT_EQUAL_UINT64_MESSAGE(e, a, m)        TEST_H_U(e, a, uint64_t, TEST_H_EQ, 0, m)
#define TEST_ASSERT_EQUAL_size_t_MESSAGE(e, a, m)        TEST_H_U(e, a, size_t, TEST_H_EQ, 0, m)
#define TEST_ASSERT_GREATER_THAN_UINT32_MESSAGE(t, a, m) TEST_H_U(t, a, uint32_t, TEST_H_GT, 0, m)
#define TEST_ASSERT_EQUAL_HEX8_MESSAGE(e, a, m)          TEST_H_U(e, a, uint8_t, TEST_H_EQ, 2, m)

#define TEST_ASSERT_EQUAL_PTR_MESSAGE(e, a, m) \
  test_h_ptr((const void *)(e), (const void *)(a), (m), __FILE__, __LINE__)
#define TEST_ASSERT_EQUAL_STRING_MESSAGE(e, a, m) test_h_string((e), (a), (m), __FILE__, __LINE__)
#define TEST_ASSERT_EQUAL_MEMORY_MESSAGE(e, a, len, m) \
  test_h_memory((const void *)(e), (const void *)(a), (size_t)(len), (m), __FILE__, __LINE__)

#define TEST_ASSERT_TRUE(c)                   TEST_ASSERT_TRUE_MESSAGE(c, NULL)
#define TEST_ASSERT_FALSE(c)                  TEST_ASSERT_FALSE_MESSAGE(c, NULL)
#define TEST_ASSERT_NULL(p)                   TEST_ASSERT_NULL_MESSAGE(p, NULL)
#define TEST_ASSERT_NOT_NULL(p)               TEST_ASSERT_NOT_NULL_MESSAGE(p, NULL)
#define TEST_ASSERT_NOT_EQUAL(e, a)           TEST_ASSERT_NOT_EQUAL_MESSAGE(e, a, NULL)
#define TEST_ASSERT_EQUAL_INT(e, a)           TEST_ASSERT_EQUAL_INT_MESSAGE(e, a, NULL)
#define TEST_ASSERT_NOT_EQUAL_INT(e, a)       TEST_ASSERT_NOT_EQUAL_INT_MESSAGE(e, a, NULL)
#define TEST_ASSERT_EQUAL_INT32(e, a)         TEST_ASSERT_EQUAL_INT32_MESSAGE(e, a, NULL)
#define TEST_ASSERT_EQUAL_INT64(e, a)         TEST_ASSERT_EQUAL_INT64_MESSAGE(e, a, NULL)
#define TEST_ASSERT_EQUAL_CHAR(e, a)          TEST_ASSERT_EQUAL_CHAR_MESSAGE(e, a, NULL)
#define TEST_ASSERT_LESS_THAN_INT(t, a)       TEST_ASSERT_LESS_THAN_INT_MESSAGE(t, a, NULL)
#define TEST_ASSERT_EQUAL_UINT8(e, a)         TEST_ASSERT_EQUAL_UINT8_MESSAGE(e, a, NULL)
#define TEST_ASSERT_EQUAL_UINT16(e, a)        TEST_ASSERT_EQUAL_UINT16_MESSAGE(e, a, NULL)
#define TEST_ASSERT_EQUAL_UINT32(e, a)        TEST_ASSERT_EQUAL_UINT32_MESSAGE(e, a, NULL)
#define TEST_ASSERT_EQUAL_UINT64(e, a)        TEST_ASSERT_EQUAL_UINT64_MESSAGE(e, a, NULL)
#define TEST_ASSERT_EQUAL_size_t(e, a)        TEST_ASSERT_EQUAL_size_t_MESSAGE(e, a, NULL)
#define TEST_ASSERT_GREATER_THAN_UINT32(t, a) TEST_ASSERT_GREATER_THAN_UINT32_MESSAGE(t, a, NULL)
#define TEST_ASSERT_EQUAL_HEX8(e, a)          TEST_ASSERT_EQUAL_HEX8_MESSAGE(e, a, NULL)
#define TEST_ASSERT_EQUAL_PTR(e, a)           TEST_ASSERT_EQUAL_PTR_MESSAGE(e, a, NULL)
#define TEST_ASSERT_EQUAL_STRING(e, a)        TEST_ASSERT_EQUAL_STRING_MESSAGE(e, a, NULL)
#define TEST_ASSERT_EQUAL_MEMORY(e, a, len)   TEST_ASSERT_EQUAL_MEMORY_MESSAGE(e, a, len, NULL)

#endif
