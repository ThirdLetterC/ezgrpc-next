/* otlp_metrics_server - an OTLP/gRPC metrics receiver built on ezGRPC-next
 *
 *   ./otlp_metrics_server [options]          (--help lists them)
 *
 * Prints "READY port=<n>" on stdout once it listens, and stops gracefully on SIGINT or SIGTERM.
 * See examples/otlp_metrics/README.md.
 */
#include <signal.h>
#include <stdio.h>

#include "otlp_app.h"

static otlp_app *g_app;

static void on_signal(int sig) {
  /* a second signal ends the process at once (default action) instead of waiting for the drain;
   * resetting also keeps this handler away from the app while it is being freed */
  signal(sig, SIG_DFL);
  otlp_app_stop(g_app); /* async-signal-safe */
}

int main(int argc, char **argv) {
  otlp_config config;
  otlp_config_defaults(&config);
  int parsed = otlp_config_parse(&config, argc, argv);
  if (parsed != 0)
    return parsed > 0 ? 0 : 2;

  grpc_err err = otlp_app_start(&config, &g_app);
  if (err != GRPC_OK) {
    fprintf(stderr, "otlp_metrics: cannot start: %s\n", grpc_strerror(err));
    return 1;
  }
  signal(SIGINT, on_signal);
  signal(SIGTERM, on_signal);
  printf("READY port=%u\n", (unsigned)otlp_app_port(g_app));
  fflush(stdout);
  int status = otlp_app_wait(g_app);
  fprintf(stderr, "otlp_metrics: stopped%s\n", status != 0 ? " (drain timed out)" : "");
  return status;
}
