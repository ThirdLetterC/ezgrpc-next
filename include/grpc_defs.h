#pragma once

/* grpc_defs.h - portability definitions shared by every public header
 *
 * Provides GRPC_API (symbol export), GRPC_NODISCARD, grpc_max_align_t, and C23 spellings
 * (nullptr, alignas, alignof, static_assert) for compilers that lack them. Included by all other
 * headers; applications rarely need it directly.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* MSVC's C-compatible headers do not provide max_align_t. Its most aligned scalar is
 * double (also the type used by the MSVC C++ library for max_align_t). */
#if defined(_MSC_VER)
typedef double grpc_max_align_t;
#else
typedef max_align_t grpc_max_align_t;
#endif

#if defined(_MSC_VER)
#ifndef alignas
#define alignas _Alignas
#endif
#ifndef alignof
#define alignof _Alignof
#endif
#endif

/* Marks exported library functions. May be predefined by the build. */
#ifndef GRPC_API
#ifdef _WIN32
#define GRPC_API
#else
#define GRPC_API [[gnu::visibility("default")]]
#endif
#endif

/* Warns when the result of a function (typically an allocation) is ignored. */
#ifndef GRPC_NODISCARD
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 202311L
#define GRPC_NODISCARD [[nodiscard]]
#elif defined(__GNUC__) || defined(__clang__)
#define GRPC_NODISCARD __attribute__((warn_unused_result))
#elif defined(_MSC_VER)
#define GRPC_NODISCARD _Check_return_
#else
#define GRPC_NODISCARD
#endif
#endif

/* Provide nullptr for compilers that accept C2x but lack the C23 keyword. */
#if defined(_MSC_VER) || !defined(__STDC_VERSION__) || __STDC_VERSION__ < 202311L
#ifndef nullptr
#define nullptr ((void *)0)
#endif
#ifndef static_assert
#define static_assert _Static_assert
#endif
#endif
