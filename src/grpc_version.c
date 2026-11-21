/* grpc_version.c - runtime library version query
 *
 * Exposes the version string baked in at build time (GRPC_VERSION_STRING, generated from
 * grpc_version_config.h.in) so applications linked against a shared library can check which
 * version they actually loaded, as opposed to the header macros they compiled against.
 */

#include "grpc_version.h"

/* Returns the compile-time version string of this library build (static storage). */
GRPC_API const char *grpc_version(void) {
  return GRPC_VERSION_STRING;
}
