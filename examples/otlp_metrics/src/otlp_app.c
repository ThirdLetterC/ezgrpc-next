#include "otlp_app.h"

#include "otlp_threads.h"
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef OTLP_METRICS_WAL
#include "otlp_wal.h"
#endif

/* ---- configuration -------------------------------------------------------------------------- */

void otlp_config_defaults(otlp_config *c) {
  *c = (otlp_config){
      .port = 4317,
      .loops = 1,
      .max_connections = 256,
      .max_streams = 64,
      .max_message_bytes = 4u << 20,
      .drain_ms = 5000,
      .log_level = GRPC_LOG_WARNING,
      .service =
          {
              .max_decoded_bytes = 32u << 20,
              .max_depth = 32,
              .retry_delay_ms = 1000,
              .request_timeout_ms = 10000,
              .workers = 0,
              .queue_capacity = 1024,
              .arena_keep_bytes = 1u << 20,
          },
      .memory_limit = (size_t)256 << 20,
      .sink = OTLP_SINK_KIND_NULL,
      .memory_sink_bytes = (size_t)64 << 20,
      .memory_policy = OTLP_MEMORY_REJECT,
      .wal_dir = "otlp-wal",
      .wal_sync = 1,
      .wal_segment_bytes = (size_t)64 << 20,
      .wal_max_bytes = (size_t)1 << 30,
      .wal_queue_bytes = (size_t)64 << 20,
      .fault_config = {.gate_capacity = 1024},
  };
}

static void usage(FILE *out, const char *argv0) {
  fprintf(
      out,
      "usage: %s [options]\n"
      "An OTLP/gRPC metrics receiver (opentelemetry.proto.collector.metrics.v1.MetricsService).\n"
      "Sizes accept K, M and G suffixes (powers of 1024).\n\n"
      "Transport:\n"
      "  --host ADDR              IPv4 address to listen on (default: all IPv4 and IPv6)\n"
      "  --host6 ADDR             IPv6 address to listen on\n"
      "  --port N                 TCP port (default 4317; 0 = ephemeral)\n"
      "  --unix PATH              listen on a Unix socket (Windows: named pipe) instead\n"
      "  --loops N                event loops on the port, one thread each (SO_REUSEPORT,\n"
      "                           Unix only; default 1)\n"
      "  --max-connections N      concurrent connections per loop (default 256)\n"
      "  --max-streams N          concurrent calls per connection (default 64)\n"
      "  --max-message-bytes N    request size limit, also after decompression (default 4M)\n"
      "  --tls-cert FILE --tls-key FILE   serve TLS\n"
      "  --drain-ms N             graceful shutdown budget (default 5000)\n"
      "  --log-level LEVEL        off|error|warning|info|debug (default warning)\n\n"
      "Processing:\n"
      "  --workers N              decode on N worker threads instead of the loops (default 0)\n"
      "  --queue-capacity N       requests queued for the workers (default 1024)\n"
      "  --max-decoded-bytes N    decoded data budget per request (default 32M)\n"
      "  --max-depth N            deepest message nesting accepted (default 32)\n"
      "  --memory-limit N         memory the receiver keeps for requests (default 256M)\n"
      "  --request-timeout-ms N   longest wait before the sink (default 10000; 0 = none)\n"
      "  --retry-delay-ms N       RetryInfo delay sent with retryable errors (default 1000)\n"
      "  --arena-keep-bytes N     decode arena kept per thread between requests (default 1M)\n"
      "  --auth-token TOKEN       require \"authorization: Bearer TOKEN\" (or OTLP_AUTH_TOKEN)\n"
      "  --admin                  serve /otlp_metrics.Admin/ (stats, wait, dump, gate)\n\n"
      "Sink:\n"
      "  --sink null|memory|wal   where accepted data goes (default null)\n"
      "  --memory-sink-bytes N    memory sink capacity (default 64M)\n"
      "  --memory-sink-batches N  memory sink batch limit (default 0 = none)\n"
      "  --memory-policy reject|evict   when the memory sink is full (default reject)\n"
      "  --wal-dir DIR            write-ahead log directory (default otlp-wal)\n"
      "  --wal-sync fsync|none    sync before acknowledging (default fsync)\n"
      "  --wal-segment-bytes N    segment size (default 64M)\n"
      "  --wal-max-bytes N        total log size; full = retryable error (default 1G)\n"
      "  --wal-queue-bytes N      bytes waiting for the writer (default 64M)\n\n"
      "Testing:\n"
      "  --fault SPEC             comma-separated: gate (start closed), gate-block (closed,\n"
      "                           blocking workers), gate-capacity=N,\n"
      "                           retryable-every=N, fail-every=N\n",
      argv0);
}

/* Parses an unsigned number with an optional K/M/G suffix. */
static bool parse_size(const char *text, uint64_t max, uint64_t *out) {
  if (text == nullptr || *text == '\0' || *text == '-')
    return false;
  errno = 0;
  char *end;
  unsigned long long value = strtoull(text, &end, 10);
  if (errno != 0 || end == text)
    return false;
  unsigned shift = 0;
  if (*end == 'K' || *end == 'k')
    shift = 10;
  else if (*end == 'M' || *end == 'm')
    shift = 20;
  else if (*end == 'G' || *end == 'g')
    shift = 30;
  if (shift != 0)
    end++;
  if (*end != '\0' || value > (max >> shift))
    return false;
  *out = (uint64_t)value << shift;
  return true;
}

static bool parse_fault(otlp_config *c, const char *spec) {
  char buf[256];
  if (strlen(spec) >= sizeof(buf))
    return false;
  strcpy(buf, spec);
  c->fault = true;
  for (char *item = strtok(buf, ","); item != nullptr; item = strtok(nullptr, ",")) {
    uint64_t n;
    if (strcmp(item, "gate") == 0)
      c->fault_config.gate_closed = true;
    else if (strcmp(item, "gate-block") == 0)
      c->fault_config.gate_closed = c->fault_config.gate_blocks = true;
    else if (strncmp(item, "gate-capacity=", 14) == 0 && parse_size(item + 14, SIZE_MAX, &n))
      c->fault_config.gate_capacity = (size_t)n;
    else if (strncmp(item, "retryable-every=", 16) == 0 && parse_size(item + 16, UINT32_MAX, &n))
      c->fault_config.retryable_every = (uint32_t)n;
    else if (strncmp(item, "fail-every=", 11) == 0 && parse_size(item + 11, UINT32_MAX, &n))
      c->fault_config.fail_every = (uint32_t)n;
    else
      return false;
  }
  return true;
}

int otlp_config_parse(otlp_config *c, int argc, char **argv) {
  const char *env_token = getenv("OTLP_AUTH_TOKEN");
  if (env_token != nullptr && env_token[0] != '\0')
    c->service.auth_token = env_token;
  for (int i = 1; i < argc; i++) {
    const char *arg = argv[i];
    if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0) {
      usage(stdout, argv[0]);
      return 1;
    }
    if (strcmp(arg, "--admin") == 0) {
      c->service.admin = true;
      continue;
    }
    /* every other option takes a value: --name=value or --name value */
    char name[64];
    const char *value;
    const char *eq = strchr(arg, '=');
    if (strncmp(arg, "--", 2) != 0)
      goto bad;
    if (eq != nullptr) {
      size_t len = (size_t)(eq - arg);
      if (len >= sizeof(name))
        goto bad;
      memcpy(name, arg, len);
      name[len] = '\0';
      value = eq + 1;
    } else {
      if (strlen(arg) >= sizeof(name) || i + 1 >= argc)
        goto bad;
      strcpy(name, arg);
      value = argv[++i];
    }
    uint64_t n = 0;
#define SIZE_OPT(opt, field, max)      \
  if (strcmp(name, opt) == 0) {        \
    if (!parse_size(value, (max), &n)) \
      goto bad;                        \
    field = (typeof(field))n;          \
    continue;                          \
  }
    SIZE_OPT("--port", c->port, UINT16_MAX)
    SIZE_OPT("--loops", c->loops, 256)
    SIZE_OPT("--max-connections", c->max_connections, 1000000)
    SIZE_OPT("--max-streams", c->max_streams, UINT32_MAX)
    SIZE_OPT("--max-message-bytes", c->max_message_bytes, INT32_MAX)
    SIZE_OPT("--drain-ms", c->drain_ms, INT32_MAX)
    SIZE_OPT("--workers", c->service.workers, 1024)
    SIZE_OPT("--queue-capacity", c->service.queue_capacity, 1u << 24)
    SIZE_OPT("--max-decoded-bytes", c->service.max_decoded_bytes, SIZE_MAX)
    SIZE_OPT("--max-depth", c->service.max_depth, 256)
    SIZE_OPT("--memory-limit", c->memory_limit, SIZE_MAX)
    SIZE_OPT("--request-timeout-ms", c->service.request_timeout_ms, UINT32_MAX)
    SIZE_OPT("--retry-delay-ms", c->service.retry_delay_ms, UINT32_MAX)
    SIZE_OPT("--arena-keep-bytes", c->service.arena_keep_bytes, SIZE_MAX)
    SIZE_OPT("--memory-sink-bytes", c->memory_sink_bytes, SIZE_MAX)
    SIZE_OPT("--memory-sink-batches", c->memory_sink_batches, SIZE_MAX)
    SIZE_OPT("--wal-segment-bytes", c->wal_segment_bytes, SIZE_MAX)
    SIZE_OPT("--wal-max-bytes", c->wal_max_bytes, SIZE_MAX)
    SIZE_OPT("--wal-queue-bytes", c->wal_queue_bytes, SIZE_MAX)
#undef SIZE_OPT
    if (strcmp(name, "--host") == 0)
      c->host = value;
    else if (strcmp(name, "--host6") == 0)
      c->host6 = value;
    else if (strcmp(name, "--unix") == 0)
      c->unix_path = value;
    else if (strcmp(name, "--tls-cert") == 0)
      c->tls_cert = value;
    else if (strcmp(name, "--tls-key") == 0)
      c->tls_key = value;
    else if (strcmp(name, "--auth-token") == 0)
      c->service.auth_token = value;
    else if (strcmp(name, "--wal-dir") == 0)
      c->wal_dir = value;
    else if (strcmp(name, "--fault") == 0) {
      if (!parse_fault(c, value))
        goto bad;
    } else if (strcmp(name, "--sink") == 0) {
      if (strcmp(value, "null") == 0)
        c->sink = OTLP_SINK_KIND_NULL;
      else if (strcmp(value, "memory") == 0)
        c->sink = OTLP_SINK_KIND_MEMORY;
      else if (strcmp(value, "wal") == 0)
        c->sink = OTLP_SINK_KIND_WAL;
      else
        goto bad;
    } else if (strcmp(name, "--memory-policy") == 0) {
      if (strcmp(value, "reject") == 0)
        c->memory_policy = OTLP_MEMORY_REJECT;
      else if (strcmp(value, "evict") == 0)
        c->memory_policy = OTLP_MEMORY_EVICT;
      else
        goto bad;
    } else if (strcmp(name, "--wal-sync") == 0) {
      if (strcmp(value, "fsync") == 0)
        c->wal_sync = 1;
      else if (strcmp(value, "none") == 0)
        c->wal_sync = 0;
      else
        goto bad;
    } else if (strcmp(name, "--log-level") == 0) {
      static const char *const levels[] = {"off", "error", "warning", "info", "debug"};
      int level = -1;
      for (int l = 0; l < 5; l++) {
        if (strcmp(value, levels[l]) == 0)
          level = GRPC_LOG_OFF + l;
      }
      if (level < 0)
        goto bad;
      c->log_level = level;
    } else {
      goto bad;
    }
    continue;
  bad:
    fprintf(stderr, "%s: invalid option or value: %s%s%s (see --help)\n", argv[0], arg,
            eq == nullptr && i < argc && argv[i] != arg ? " " : "",
            eq == nullptr && i < argc && argv[i] != arg ? argv[i] : "");
    return -1;
  }

  if (c->loops == 0 || c->service.max_depth == 0 || c->max_message_bytes == 0 ||
      c->service.max_decoded_bytes == 0 ||
      (c->service.workers > 0 && c->service.queue_capacity == 0)) {
    fprintf(stderr,
            "%s: --loops, --max-depth, --max-message-bytes, --max-decoded-bytes and "
            "--queue-capacity must be positive\n",
            argv[0]);
    return -1;
  }
#ifdef _WIN32
  if (c->loops > 1) {
    fprintf(stderr, "%s: --loops > 1 needs SO_REUSEPORT, which Windows lacks; use --workers\n",
            argv[0]);
    return -1;
  }
#endif
  if (c->unix_path != nullptr && c->loops > 1) {
    fprintf(stderr, "%s: a Unix socket cannot be shared by several loops\n", argv[0]);
    return -1;
  }
  if ((c->tls_cert == nullptr) != (c->tls_key == nullptr)) {
    fprintf(stderr, "%s: --tls-cert and --tls-key go together\n", argv[0]);
    return -1;
  }
#ifndef OTLP_METRICS_WAL
  if (c->sink == OTLP_SINK_KIND_WAL) {
    fprintf(stderr, "%s: built without the WAL sink (configure with -DOTLP_METRICS_WAL=ON)\n",
            argv[0]);
    return -1;
  }
#endif
  return 0;
}

/* ---- app ------------------------------------------------------------------------------------ */

typedef struct loop_thread {
  otlp_app *app;
  grpc_server *server;
  pthread_t thread;
  bool running;
  grpc_err drained;
} loop_thread;

struct otlp_app {
  otlp_config config;
  otlp_budget budget;
  otlp_service *service;
  loop_thread *loops;
  unsigned nb_loops;
  uint16_t port;
  pthread_mutex_t lock;
  pthread_cond_t cond;
  unsigned drained; /* guarded by lock */
  bool release;     /* guarded by lock */
};

static void *loop_main(void *arg) {
  loop_thread *lt = arg;
  otlp_app *app = lt->app;
  (void)grpc_server_run(lt->server);
  /* one loop stopping (signal, failure) stops them all */
  otlp_service_begin_shutdown(app->service);
  otlp_app_stop(app);
  lt->drained = grpc_server_drain(lt->server, (int)app->config.drain_ms);

  pthread_mutex_lock(&app->lock);
  app->drained++;
  pthread_cond_broadcast(&app->cond);
  while (!app->release)
    pthread_cond_wait(&app->cond, &app->lock);
  pthread_mutex_unlock(&app->lock);
  /* runs the completions the workers and the sink queued after the drain */
  grpc_server_free(lt->server);
  return nullptr;
}

static otlp_sink *make_sink(const otlp_config *c, otlp_budget *budget) {
  otlp_sink *sink = nullptr;
  switch (c->sink) {
  case OTLP_SINK_KIND_NULL:
    sink = otlp_null_sink_new();
    break;
  case OTLP_SINK_KIND_MEMORY:
    sink = otlp_memory_sink_new(c->memory_sink_bytes, c->memory_sink_batches, c->memory_policy);
    break;
  case OTLP_SINK_KIND_WAL:
#ifdef OTLP_METRICS_WAL
    sink = otlp_wal_sink_new(
        &(otlp_wal_config){
            .dir = c->wal_dir,
            .sync = c->wal_sync != 0 ? OTLP_WAL_SYNC_FSYNC : OTLP_WAL_SYNC_NONE,
            .segment_bytes = c->wal_segment_bytes,
            .max_bytes = c->wal_max_bytes,
            .queue_bytes = c->wal_queue_bytes,
        },
        budget);
#endif
    break;
  }
  if (sink != nullptr && c->fault) {
    otlp_sink *wrapped = otlp_fault_sink_new(sink, &c->fault_config, budget);
    if (wrapped == nullptr) {
      otlp_sink_shutdown(sink);
      otlp_sink_destroy(sink);
      return nullptr;
    }
    sink = wrapped;
  }
  return sink;
}

/* Frees what otlp_app_start() created before the loop threads started. */
static void app_destroy_unstarted(otlp_app *app) {
  for (unsigned i = 0; i < app->nb_loops; i++)
    grpc_server_free(app->loops[i].server);
  if (app->service != nullptr) {
    otlp_service_stop(app->service);
    otlp_service_free(app->service);
  }
  otlp_cond_destroy(&app->cond);
  pthread_mutex_destroy(&app->lock);
  grpc_free(app->loops);
  grpc_free(app);
}

grpc_err otlp_app_start(const otlp_config *config, otlp_app **out) {
  *out = nullptr;
  otlp_app *app = grpc_calloc(1, sizeof(*app));
  if (app == nullptr)
    return GRPC_ERR_NOMEM;
  app->config = *config;
  pthread_mutex_init(&app->lock, nullptr);
  otlp_cond_init(&app->cond);
  otlp_budget_init(&app->budget, config->memory_limit);
  app->loops = grpc_calloc(config->loops, sizeof(loop_thread));
  if (app->loops == nullptr) {
    app_destroy_unstarted(app);
    return GRPC_ERR_NOMEM;
  }

  otlp_sink *sink = make_sink(config, &app->budget);
  if (sink == nullptr) {
    fprintf(stderr, "otlp_metrics: cannot create the %s sink\n",
            config->sink == OTLP_SINK_KIND_MEMORY ? "memory"
            : config->sink == OTLP_SINK_KIND_WAL  ? "wal"
                                                  : "null");
    app_destroy_unstarted(app);
    return GRPC_ERR_IO;
  }
  app->service = otlp_service_new(&config->service, sink, &app->budget);
  if (app->service == nullptr) {
    otlp_sink_shutdown(sink);
    otlp_sink_destroy(sink);
    app_destroy_unstarted(app);
    return GRPC_ERR_NOMEM;
  }

  grpc_err err = GRPC_OK;
  for (unsigned i = 0; i < config->loops; i++) {
    grpc_config sc = {
        .port = i == 0 ? config->port : app->port,
        .ipv4_addr = config->host,
        .ipv6_addr = config->host6,
        .unix_path = config->unix_path,
        .reuse_port = config->loops > 1,
        .max_connections = config->max_connections,
        .max_concurrent_streams = config->max_streams,
        .max_message_size = config->max_message_bytes,
        .tls = {.cert_file = config->tls_cert, .key_file = config->tls_key},
        .log_level = (grpc_log_level)config->log_level,
    };
    grpc_server *server = grpc_server_new_ex(&sc, &err);
    if (server == nullptr)
      break;
    app->loops[i] = (loop_thread){.app = app, .server = server};
    app->nb_loops++;
    if (i == 0)
      app->port = grpc_server_port(server);
    err = otlp_service_attach(app->service, server);
    if (err != GRPC_OK)
      break;
  }
  if (err != GRPC_OK) {
    app_destroy_unstarted(app);
    return err;
  }

  for (unsigned i = 0; i < app->nb_loops; i++) {
    if (pthread_create(&app->loops[i].thread, nullptr, loop_main, &app->loops[i]) != 0) {
      /* servers without a thread never ran: free them here; the started loops then stop and
       * tear down as usual */
      for (unsigned j = i; j < app->nb_loops; j++)
        grpc_server_free(app->loops[j].server);
      app->nb_loops = i;
      *out = app;
      otlp_app_stop(app);
      (void)otlp_app_wait(app);
      *out = nullptr;
      return GRPC_ERR_NOMEM;
    }
    app->loops[i].running = true;
  }
  *out = app;
  return GRPC_OK;
}

uint16_t otlp_app_port(const otlp_app *app) {
  return app->port;
}

otlp_service *otlp_app_service(otlp_app *app) {
  return app->service;
}

void otlp_app_stop(otlp_app *app) {
  /* grpc_server_stop() is async-signal-safe and thread-safe */
  for (unsigned i = 0; i < app->nb_loops; i++)
    grpc_server_stop(app->loops[i].server);
}

int otlp_app_wait(otlp_app *app) {
  pthread_mutex_lock(&app->lock);
  while (app->drained < app->nb_loops)
    pthread_cond_wait(&app->cond, &app->lock);
  pthread_mutex_unlock(&app->lock);

  int status = 0;
  for (unsigned i = 0; i < app->nb_loops; i++) {
    if (app->loops[i].drained != GRPC_OK)
      status = 1;
  }
  /* finish queued work and pending sink batches; their answers wait in the servers' queues */
  otlp_service_stop(app->service);

  pthread_mutex_lock(&app->lock);
  app->release = true;
  pthread_cond_broadcast(&app->cond);
  pthread_mutex_unlock(&app->lock);
  for (unsigned i = 0; i < app->nb_loops; i++)
    pthread_join(app->loops[i].thread, nullptr);

  otlp_service_free(app->service);
  otlp_cond_destroy(&app->cond);
  pthread_mutex_destroy(&app->lock);
  grpc_free(app->loops);
  grpc_free(app);
  return status;
}
