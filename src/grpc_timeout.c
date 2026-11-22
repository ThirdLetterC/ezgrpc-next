/* grpc_timeout.c - monotonic clock, grpc-timeout parsing and server call deadlines
 *
 * Deadlines are absolute times in milliseconds on the libuv high-resolution monotonic clock
 * (grpc_time_now_ms). When a request carries a grpc-timeout header, the nghttp2 callbacks parse it
 * with grpc_timeout_parse_ms, store the deadline on the stream and call server_arm_deadline.
 *
 * The server uses a single one-shot uv timer for all deadlines, armed for the earliest one
 * (server->next_deadline_ms). When it fires, every session's streams are scanned, expired calls
 * are aborted with DEADLINE_EXCEEDED and the timer is re-armed for the next deadline. Everything
 * here except grpc_timeout_parse_ms and grpc_time_now_ms runs on the server's loop thread.
 */

#include <time.h>
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <limits.h>
#include "core.h"
#include "grpc_timeout.h"

/* Returns libuv's monotonic high-resolution clock (nanoseconds) truncated to milliseconds. */
uint64_t grpc_time_now_ms(void) {
  return uv_hrtime() / 1000000ULL;
}

/* Parses a grpc-timeout header value: ASCII digits followed by exactly one unit character
 * (H hours, M minutes, S seconds, m milliseconds, u microseconds, n nanoseconds). Sub-millisecond
 * units round up so a nonzero timeout never becomes 0 ms. Returns the timeout in milliseconds,
 * clamped to INT64_MAX, or -1 for null/empty input, a missing or unknown unit, trailing bytes, a
 * zero value, more digits than fit in 64 bits, or a value that overflows during unit conversion. The spec's 8-digit limit is not
 * enforced. */
int64_t grpc_timeout_parse_ms(const char *timeout_str) {
  if (timeout_str == nullptr || *timeout_str == '\0') {
    return -1;
  }

  // Find where numeric value ends
  const char *p = timeout_str;
  while (*p && isdigit((unsigned char)*p)) {
    p++;
  }

  if (p == timeout_str || *p == '\0') {
    return -1; // No digits or missing unit
  }

  char unit = *p;
  // If there are extra characters after the unit
  if (*(p + 1) != '\0') {
    return -1;
  }

  // Parse numeric part
  char *endptr = nullptr;
  errno = 0;
  unsigned long long val = strtoull(timeout_str, &endptr, 10);
  /* ERANGE: more digits than 64 bits hold (strtoull then returns ULLONG_MAX) */
  if (endptr != p || val == 0 || errno == ERANGE) {
    return -1;
  }

  uint64_t ms = 0;
  switch (unit) {
  case 'H': // Hour
    if (val > (ULLONG_MAX / (3600ULL * 1000ULL)))
      return -1;
    ms = val * 3600ULL * 1000ULL;
    break;
  case 'M': // Minute
    if (val > (ULLONG_MAX / (60ULL * 1000ULL)))
      return -1;
    ms = val * 60ULL * 1000ULL;
    break;
  case 'S': // Second
    if (val > (ULLONG_MAX / 1000ULL))
      return -1;
    ms = val * 1000ULL;
    break;
  case 'm': // Millisecond
    ms = val;
    break;
  /* round up so a nonzero timeout never becomes 0 ms; (val + 999) / 1000 would wrap near
   * ULLONG_MAX and turn a huge timeout into 0 */
  case 'u': // Microsecond
    ms = val / 1000ULL + (val % 1000ULL != 0);
    break;
  case 'n': // Nanosecond
    ms = val / 1000000ULL + (val % 1000000ULL != 0);
    break;
  default:
    return -1;
  }

  if (ms > (uint64_t)INT64_MAX) {
    return INT64_MAX;
  }

  return (int64_t)ms;
}

/* Aborts every live call whose deadline is at or before now_ms with DEADLINE_EXCEEDED and clears
 * its deadline, skipping inactive or closing sessions. Returns the earliest deadline still
 * pending, or UINT64_MAX if none. Runs on the server loop thread. */
static u64 expire_deadlines(grpc_server *server, u64 now_ms) {
  u64 next_deadline_ms = UINT64_MAX;
  for (size_t i = 0; i < server->nb_sessions; i++) {
    grpc_session *session = &server->sessions[i];
    if (!session->is_active || session->is_closing || session->ops == nullptr)
      continue;
    grpc_stream *stream = session->streams;
    while (stream != nullptr) {
      /* call_abort() may not free the stream, but read next first anyway */
      grpc_stream *next = stream->next;
      if (stream->is_deadline_set) {
        if (now_ms >= stream->deadline_ms) {
          stream->is_deadline_set = false;
          if (stream->call != nullptr)
            call_abort(stream->call, GRPC_STATUS_DEADLINE_EXCEEDED, nullptr);
        } else if (stream->deadline_ms < next_deadline_ms) {
          next_deadline_ms = stream->deadline_ms;
        }
      }
      stream = next;
    }
  }
  return next_deadline_ms;
}

/* uv timer callback for server->deadline_timer: bumps ready_events, forgets the fired deadline
 * (so server_arm_deadline accepts any new one), expires overdue calls and re-arms for the next
 * remaining deadline. */
static void on_deadline_timer(uv_timer_t *timer) {
  grpc_server *server = timer->data;
  server->ready_events++;
  server->next_deadline_ms = UINT64_MAX;
  /* call_abort() runs the application's on_done callbacks; a grpc_server_flush() from there
   * could close a session (on a write error) while it is being walked, so flushes wait until the
   * scan is over (the poll writes them right after) */
  server->in_deadline_scan = true;
  u64 next = expire_deadlines(server, grpc_time_now_ms());
  server->in_deadline_scan = false;
  if (next != UINT64_MAX)
    server_arm_deadline(server, next);
}

/* Ensures the deadline timer fires no later than deadline_ms (absolute, grpc_time_now_ms clock).
 * Does nothing if the timer is already due earlier or at the same time; otherwise (re)starts it,
 * firing immediately for a deadline in the past. Must run on the server loop thread. */
void server_arm_deadline(grpc_server *server, u64 deadline_ms) {
  /* the timer already fires earlier */
  if (deadline_ms >= server->next_deadline_ms)
    return;
  server->next_deadline_ms = deadline_ms;
  u64 now = grpc_time_now_ms();
  uv_timer_start(&server->deadline_timer, on_deadline_timer,
                 deadline_ms > now ? deadline_ms - now : 0, 0);
}
