#pragma once

/* otlp_response.h - ExportMetricsServiceResponse and google.rpc.RetryInfo encoding */

#include <stddef.h>
#include <stdint.h>

#include "grpc.h"

/** Type URL of google.rpc.RetryInfo in a google.rpc.Status detail. */
#define OTLP_RETRY_INFO_TYPE_URL "type.googleapis.com/google.rpc.RetryInfo"

/** Trailer carrying a serialized RetryInfo on its own (read by the OpenTelemetry Python exporter). */
#define OTLP_RETRY_INFO_TRAILER "google.rpc.retryinfo-bin"

/** Longest error_message put in a partial success response. */
enum { OTLP_ERROR_MESSAGE_MAX = 256 };

/**
 * Encodes an ExportMetricsServiceResponse. With `rejected` == 0 and no `error_message` it is the
 * full-success response, with `partial_success` unset (an empty message); otherwise
 * `partial_success` carries `rejected` and `error_message` (truncated to OTLP_ERROR_MESSAGE_MAX
 * bytes; nullptr or "" leaves it empty). Returns nullptr when out of memory.
 */
grpc_message *otlp_response_new(uint64_t rejected, const char *error_message);

/**
 * Encodes google.rpc.RetryInfo{retry_delay: `delay_ms`} into `out` (at least 32 bytes). Returns
 * the encoded length.
 */
size_t otlp_retry_info_encode(uint8_t *out, uint32_t delay_ms);
