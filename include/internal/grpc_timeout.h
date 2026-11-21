#pragma once

/* grpc_timeout.h - monotonic clock, grpc-timeout parsing and call deadlines
 *
 * Deadlines are absolute milliseconds on grpc_time_now_ms(). The server keeps one deadline timer
 * armed for the earliest call deadline; see src/grpc_timeout.c.
 */

#include <stdint.h>

struct grpc_server;

/* Return the monotonic time in milliseconds (libuv uv_hrtime()). Thread-safe. */
uint64_t grpc_time_now_ms(void);

/* Parse a grpc-timeout value ("100m", "5S", ...) into milliseconds, rounding sub-millisecond
 * units up; return -1 if invalid or zero. */
int64_t grpc_timeout_parse_ms(const char *timeout_str);

/* Ensure the deadline timer fires no later than `deadline_ms` (absolute, grpc_time_now_ms()).
 * Server loop thread only. */
void server_arm_deadline(struct grpc_server *server, uint64_t deadline_ms);
