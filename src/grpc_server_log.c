/* grpc_server_log.c - server logging through ulog: per-server topics and the shared output
 *
 * Part of the server described in grpc_server.c.
 */

#include <errno.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifndef _WIN32
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>
#endif

#include "core.h"
#include "grpc_timeout.h"
#include "internal_helpers.h"
#include "internal_nghttp2_callbacks.h"
#include "internal_http3.h"
#include "internal_server.h"
#include "ulog/ulog.h"

/* ---- logging ------------------------------------------------------------------ */

static pthread_mutex_t ulog_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_registry_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_level_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_once_t logger_once = PTHREAD_ONCE_INIT;
static grpc_server *log_registry;
static atomic_uint_fast64_t next_log_topic = 1;
static ulog_output_id log_output = ULOG_OUTPUT_INVALID;
static ulog_status logger_status = ULOG_STATUS_ERROR;

/* Locks (lock == true) or unlocks ulog_mutex; installed as ulog's lock function so that events
 * from servers on different threads are serialized. Returns ULOG_STATUS_BUSY if the mutex call
 * fails. */
static ulog_status logger_lock(bool lock, [[maybe_unused]] void *arg) {
  int rc = lock ? pthread_mutex_lock(&ulog_mutex) : pthread_mutex_unlock(&ulog_mutex);
  return rc == 0 ? ULOG_STATUS_OK : ULOG_STATUS_BUSY;
}

/* The one ulog output shared by all servers: looks up the server whose topic id matches the
 * event and writes the formatted message to its log_file under log_registry_mutex. Drops
 * events without a topic or for a server no longer registered. Messages are cut to 1023 bytes.
 */
static void logger_output(ulog_event *event, [[maybe_unused]] void *arg) {
  int topic_id = ulog_event_get_topic(event);
  char message[1024];
  if (topic_id == ULOG_TOPIC_ID_INVALID ||
      ulog_event_get_message(event, message, sizeof(message)) != ULOG_STATUS_OK)
    return;

  pthread_mutex_lock(&log_registry_mutex);
  grpc_server *server = log_registry;
  while (server != nullptr && server->log_topic_id != topic_id)
    server = server->log_registry_next;
  if (server != nullptr)
    (void)fwrite(message, 1, strlen(message), server->log_file);
  pthread_mutex_unlock(&log_registry_mutex);
}

/* Runs once per process through pthread_once(): installs logger_lock() and registers
 * logger_output(). On failure logger_status stays ULOG_STATUS_ERROR, so every later
 * logger_server_add(), and hence grpc_server_new_ex(), fails with GRPC_ERR_IO. */
static void logger_initialize(void) {
  if (ulog_lock_set_fn(logger_lock, nullptr) != ULOG_STATUS_OK)
    return;
  log_output = ulog_output_add(logger_output, nullptr, ULOG_LEVEL_TRACE);
  logger_status = log_output == ULOG_OUTPUT_INVALID ? ULOG_STATUS_ERROR : ULOG_STATUS_OK;
}

/* Maps a grpc_log_level to the ulog level of a server topic. OFF and DEFAULT map to ERROR: OFF
 * is enforced by server_log() before ulog is reached, and DEFAULT is resolved by the callers.
 */
static ulog_level ulog_level_for(grpc_log_level level) {
  switch (level) {
  case GRPC_LOG_TRACE:
    return (ulog_level)ULOG_LEVEL_TRACE;
  case GRPC_LOG_DEBUG:
    return (ulog_level)ULOG_LEVEL_DEBUG;
  case GRPC_LOG_INFO:
    return (ulog_level)ULOG_LEVEL_INFO;
  case GRPC_LOG_WARNING:
    return (ulog_level)ULOG_LEVEL_WARN;
  case GRPC_LOG_ERROR:
  case GRPC_LOG_OFF:
  case GRPC_LOG_DEFAULT:
  default:
    return (ulog_level)ULOG_LEVEL_ERROR;
  }
}

grpc_err logger_server_add(grpc_server *server) {
  if (pthread_once(&logger_once, logger_initialize) != 0 || logger_status != ULOG_STATUS_OK)
    return GRPC_ERR_IO;

  uint_fast64_t topic = atomic_fetch_add(&next_log_topic, 1);
  snprintf(server->log_topic, sizeof(server->log_topic), "grpc-server-%" PRIuFAST64, topic);
  server->log_topic_id =
      ulog_topic_add(server->log_topic, log_output,
                     ulog_level_for((grpc_log_level)atomic_load(&server->log_level)));
  if (server->log_topic_id == ULOG_TOPIC_ID_INVALID)
    return GRPC_ERR_IO;

  pthread_mutex_lock(&log_registry_mutex);
  server->log_registry_next = log_registry;
  log_registry = server;
  server->log_topic_initialized = true;
  pthread_mutex_unlock(&log_registry_mutex);
  return GRPC_OK;
}

void logger_server_remove(grpc_server *server) {
  if (!server->log_topic_initialized)
    return;

  (void)ulog_topic_remove(server->log_topic);
  pthread_mutex_lock(&log_registry_mutex);
  grpc_server **link = &log_registry;
  while (*link != nullptr && *link != server)
    link = &(*link)->log_registry_next;
  if (*link == server)
    *link = server->log_registry_next;
  server->log_topic_initialized = false;
  pthread_mutex_unlock(&log_registry_mutex);
}

/* Formats a timestamped line and emits it on the server's topic; usually reached through the
 * LOG_* macros. Does nothing when server is nullptr, logging is off or level is more verbose
 * than the server's level. Lines are cut to 1023 bytes. Safe from any thread: the level is
 * atomic and ulog is locked. */
void server_log(grpc_server *server, grpc_log_level level, const char *fmt, ...) {
  if (server == nullptr)
    return;
  int log_level = atomic_load(&server->log_level);
  if (log_level == GRPC_LOG_OFF || level > log_level)
    return;

  /* one write per line, so that lines of several servers do not interleave */
  char line[1024];
  char dt[128] = {};
  int n = snprintf(line, sizeof(line), "[%s] ", ezgetdt(dt, sizeof(dt)));
  va_list ap;
  va_start(ap, fmt);
  if (n >= 0 && (size_t)n < sizeof(line))
    vsnprintf(line + n, sizeof(line) - (size_t)n, fmt, ap);
  va_end(ap);

  switch (level) {
  case GRPC_LOG_ERROR:
    ulog_t_error(server->log_topic, "%s", line);
    break;
  case GRPC_LOG_WARNING:
    ulog_t_warn(server->log_topic, "%s", line);
    break;
  case GRPC_LOG_INFO:
    ulog_t_info(server->log_topic, "%s", line);
    break;
  case GRPC_LOG_DEBUG:
    ulog_t_debug(server->log_topic, "%s", line);
    break;
  case GRPC_LOG_TRACE:
    ulog_t_trace(server->log_topic, "%s", line);
    break;
  default:
    break;
  }
}

/* Changes the ulog topic level and the server's atomic level together under log_level_mutex so
 * concurrent callers cannot leave them out of sync. Safe from any thread. */
GRPC_API grpc_err grpc_server_set_log_level(grpc_server *server, grpc_log_level level) {
  if (server == nullptr || level < GRPC_LOG_DEFAULT || level > GRPC_LOG_TRACE)
    return GRPC_ERR_INVALID;
  if (level == GRPC_LOG_DEFAULT)
    level = GRPC_LOG_INFO;

  pthread_mutex_lock(&log_level_mutex);
  ulog_status status = ulog_topic_level_set(server->log_topic, ulog_level_for(level));
  if (status == ULOG_STATUS_OK)
    atomic_store(&server->log_level, level);
  pthread_mutex_unlock(&log_level_mutex);
  return status == ULOG_STATUS_OK ? GRPC_OK : GRPC_ERR_IO;
}
