#pragma once

/* grpc_version.h - library version
 *
 * The GRPC_VERSION_STRING / GRPC_VERSION_MAJOR / _MINOR / _PATCH macros from the generated
 * grpc_version_config.h give the version compiled against; grpc_version() gives the version of
 * the library actually linked.
 */

#include "grpc_defs.h"
#include "grpc_version_config.h"

/**
 * Returns the linked library's version, e.g. ``"1.0.0"``. The returned string has
 * static storage duration and must not be modified or freed. Header macros
 * describe the version used at compile time, which may differ from a loaded DLL.
 */
GRPC_API const char *grpc_version(void);
