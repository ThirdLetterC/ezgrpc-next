/* protoc_gen_c_shim.c - lets an unmodified protoc-c plugin compile proto3 `optional` fields
 *
 * protoc only accepts proto3 files with `optional` fields from plugins that report
 * FEATURE_PROTO3_OPTIONAL in CodeGeneratorResponse.supported_features. No protobuf-c release
 * (1.5.2 at the time of writing) reports it, although protoc already describes such a field as the
 * single member of a synthetic oneof, which protoc-c handles like any oneof: the generated struct
 * gets a `_<field>_case` member that records presence. The OpenTelemetry metrics protos use
 * `optional` for HistogramDataPoint.sum/min/max, so they need this.
 *
 * The shim runs the real plugin (OTLP_REAL_PLUGIN, fixed at build time) with protoc's request and
 * forwards its response, appending `supported_features = 1` (field 2, varint). Appending a field
 * to a serialized message overrides it (protobuf merge semantics), so the response is not parsed.
 * The .proto files and the generator are used exactly as published.
 *
 * Portable C: the request is written to a temporary file and the plugin started with popen(), its
 * stdin redirected from that file.
 */
#if !defined(_WIN32) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L /* popen, mkstemp, fdopen under -std=c23 */
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#define popen      _popen
#define pclose     _pclose
#define POPEN_READ "rb"
#else
#include <unistd.h>
#define POPEN_READ "r"
#endif

#ifndef OTLP_REAL_PLUGIN
#error "OTLP_REAL_PLUGIN must name the protoc-c plugin executable"
#endif

/* Copies all of `in` to `out`. Returns 0, or -1 on a read or write error. */
static int copy_stream(FILE *in, FILE *out) {
  char buf[65536];
  size_t n;
  while ((n = fread(buf, 1, sizeof(buf), in)) > 0) {
    if (fwrite(buf, 1, n, out) != n)
      return -1;
  }
  return ferror(in) ? -1 : 0;
}

int main(void) {
#ifdef _WIN32
  (void)_setmode(_fileno(stdin), _O_BINARY);
  (void)_setmode(_fileno(stdout), _O_BINARY);
  char path[L_tmpnam_s];
  if (tmpnam_s(path, sizeof(path)) != 0) {
    fputs("protoc_gen_c_shim: cannot name a temporary file\n", stderr);
    return 1;
  }
  FILE *request = fopen(path, "w+b");
#else
  char path[] = "/tmp/protoc-gen-c-shim-XXXXXX";
  int fd = mkstemp(path);
  FILE *request = fd >= 0 ? fdopen(fd, "w+b") : nullptr;
#endif
  if (request == nullptr) {
    fputs("protoc_gen_c_shim: cannot create a temporary file\n", stderr);
    return 1;
  }
  int status = 1;
  if (copy_stream(stdin, request) != 0 || fflush(request) != 0) {
    fputs("protoc_gen_c_shim: cannot store the request\n", stderr);
    goto out;
  }

  char command[4096];
#ifdef _WIN32
  /* cmd.exe /c drops the outermost quotes of a command that starts with one */
  int len = snprintf(command, sizeof(command), "\"\"%s\" < \"%s\"\"", OTLP_REAL_PLUGIN, path);
#else
  int len = snprintf(command, sizeof(command), "\"%s\" < \"%s\"", OTLP_REAL_PLUGIN, path);
#endif
  if (len < 0 || (size_t)len >= sizeof(command)) {
    fputs("protoc_gen_c_shim: plugin path too long\n", stderr);
    goto out;
  }
  FILE *plugin = popen(command, POPEN_READ);
  if (plugin == nullptr) {
    fprintf(stderr, "protoc_gen_c_shim: cannot run %s\n", OTLP_REAL_PLUGIN);
    goto out;
  }
  int copied = copy_stream(plugin, stdout);
  int plugin_status = pclose(plugin);
  if (copied != 0 || plugin_status != 0) {
    fprintf(stderr, "protoc_gen_c_shim: %s failed (status %d)\n", OTLP_REAL_PLUGIN, plugin_status);
    goto out;
  }
  /* CodeGeneratorResponse.supported_features (field 2, varint) = FEATURE_PROTO3_OPTIONAL (1) */
  static const unsigned char features[] = {0x10, 0x01};
  if (fwrite(features, 1, sizeof(features), stdout) != sizeof(features) || fflush(stdout) != 0)
    goto out;
  status = 0;
out:
  fclose(request);
  remove(path);
  return status;
}
