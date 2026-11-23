#pragma once

/* otlp_wal_format.h - on-disk format of the write-ahead log, shared by the sink and the tools
 *
 * A log is a directory of segment files named "<first sequence, 20 digits>.wal", written in
 * sequence order. All integers are little-endian.
 *
 *   segment header (16 bytes):  "OTLPWAL1"  u32 version (1)  u32 reserved (0)
 *   record header  (32 bytes):  u32 magic "OTRC"  u32 payload length
 *                               u64 sequence      u64 data points
 *                               u32 crc32c        u32 reserved (0)
 *   payload: one serialized ExportMetricsServiceRequest holding exactly the accepted data
 *
 * The CRC-32C covers the header (with the crc field zero) and the payload. A record whose magic,
 * length or CRC does not check out ends the valid part of its segment: after a crash that is a
 * torn write at the tail of the last segment, which recovery truncates away.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

enum {
  OTLP_WAL_SEGMENT_HEADER = 16,
  OTLP_WAL_RECORD_HEADER = 32,
  OTLP_WAL_VERSION = 1,
  /** Largest payload a record may claim (bounds what a reader allocates). */
  OTLP_WAL_MAX_PAYLOAD = 1 << 30,
};

#define OTLP_WAL_MAGIC        "OTLPWAL1"
#define OTLP_WAL_RECORD_MAGIC 0x4352544Fu /* "OTRC" */

/** CRC-32C (Castagnoli) of `len` bytes, continuing from `crc` (start with 0). */
uint32_t otlp_crc32c(uint32_t crc, const void *data, size_t len);

/** Writes the segment header into `out` (OTLP_WAL_SEGMENT_HEADER bytes). */
void otlp_wal_segment_header(uint8_t *out);

/** True when `in` is a valid segment header. */
bool otlp_wal_segment_header_valid(const uint8_t *in);

/** Writes a record header for `payload` into `out` (OTLP_WAL_RECORD_HEADER bytes). */
void otlp_wal_record_header(uint8_t *out, uint64_t sequence, uint64_t points,
                            const uint8_t *payload, uint32_t payload_len);

typedef struct otlp_wal_record {
  uint64_t sequence;
  uint64_t points;
  uint32_t payload_len;
  uint32_t crc;
} otlp_wal_record;

/** Parses a record header. False when its magic or length is invalid. */
bool otlp_wal_record_parse(const uint8_t *in, otlp_wal_record *out);

/** True when `payload` matches the CRC of the parsed header `in` (the raw header bytes). */
bool otlp_wal_record_check(const uint8_t *header, const uint8_t *payload, uint32_t payload_len);

/** Formats a segment file name ("<20 digits>.wal") for its first sequence number. */
void otlp_wal_segment_name(char out[32], uint64_t first_sequence);

/** Parses a segment file name. False for other files. */
bool otlp_wal_segment_name_parse(const char *name, uint64_t *first_sequence);

typedef enum otlp_wal_scan_end {
  /** Every byte belongs to a valid record. */
  OTLP_WAL_SCAN_CLEAN = 0,
  /** The segment ends with an incomplete or corrupt record (a torn write). */
  OTLP_WAL_SCAN_TORN,
  /** The segment header is missing or invalid. */
  OTLP_WAL_SCAN_BAD_HEADER,
  /** The file could not be read. */
  OTLP_WAL_SCAN_IO_ERROR,
} otlp_wal_scan_end;

typedef struct otlp_wal_scan {
  otlp_wal_scan_end end;
  /** Offset just past the last valid record (where a torn tail starts). */
  uint64_t valid_bytes;
  /** File size. */
  uint64_t file_bytes;
  uint64_t records;
  uint64_t points;
  /** Sequence number of the last valid record (0 when there is none). */
  uint64_t last_sequence;
} otlp_wal_scan;

/**
 * Reads a segment and calls `fn` (may be nullptr) for every valid record, with the payload valid
 * during the call; `fn` returning false stops the scan. Fills `*out`. Synchronous file I/O.
 */
void otlp_wal_scan_segment(const char *path,
                           bool (*fn)(void *ctx, const otlp_wal_record *record,
                                      const uint8_t *payload),
                           void *ctx, otlp_wal_scan *out);

/**
 * Lists the segment files of `dir` in sequence order: allocates `*names` (grpc_malloc; each name
 * too, free with otlp_wal_list_free()). Returns the count, 0 for an empty or missing directory,
 * or -1 on another error.
 */
long otlp_wal_list_segments(const char *dir, char ***names);

void otlp_wal_list_free(char **names, long count);
