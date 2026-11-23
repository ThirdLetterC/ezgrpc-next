/* otlp_wal_dump - verify and export a write-ahead log of the OTLP metrics receiver
 *
 *   otlp_wal_dump DIR                  per-segment summary, verifying every record's CRC and
 *                                      decoding its payload as an ExportMetricsServiceRequest
 *   otlp_wal_dump DIR --export FILE    also writes the payloads, concatenated, to FILE: that is
 *                                      one ExportMetricsServiceRequest with every stored batch
 *
 * Exit status: 0 when the log is intact, 1 when a segment is torn or damaged or a payload does
 * not decode, 2 on usage or I/O errors. The log is only read.
 */
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include "grpc_mem.h"
#include "otlp_proto.h"
#include "otlp_validate.h"
#include "otlp_wal_format.h"

typedef struct dump_state {
  FILE *export_file;
  uint64_t expected_sequence; /* 0 = any */
  uint64_t sequence_gaps;
  uint64_t bad_payloads;
  uint64_t points_mismatch;
  uint64_t decoded_points;
  bool write_error;
} dump_state;

static bool on_record(void *ctx, const otlp_wal_record *rec, const uint8_t *payload) {
  dump_state *st = ctx;
  if (st->expected_sequence != 0 && rec->sequence != st->expected_sequence)
    st->sequence_gaps++;
  st->expected_sequence = rec->sequence + 1;
  otlp_pb_request *req = (otlp_pb_request *)protobuf_c_message_unpack(
      OTLP_PB_REQUEST_DESCRIPTOR, nullptr, rec->payload_len, payload);
  if (req == nullptr) {
    st->bad_payloads++;
  } else {
    otlp_counts counts;
    otlp_validate_request(req, &counts);
    st->decoded_points += counts.points;
    if (counts.points != rec->points || counts.rejected != 0)
      st->points_mismatch++;
    protobuf_c_message_free_unpacked(&req->base, nullptr);
  }
  if (st->export_file != nullptr && rec->payload_len > 0 &&
      fwrite(payload, 1, rec->payload_len, st->export_file) != rec->payload_len) {
    st->write_error = true;
    return false;
  }
  return true;
}

int main(int argc, char **argv) {
  const char *dir = nullptr;
  const char *export_path = nullptr;
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--export") == 0 && i + 1 < argc)
      export_path = argv[++i];
    else if (dir == nullptr && argv[i][0] != '-')
      dir = argv[i];
    else
      dir = nullptr, i = argc;
  }
  if (dir == nullptr) {
    fprintf(stderr, "usage: %s DIR [--export FILE]\n", argv[0]);
    return 2;
  }
  dump_state st = {0};
  if (export_path != nullptr) {
    st.export_file = fopen(export_path, "wb");
    if (st.export_file == nullptr) {
      perror(export_path);
      return 2;
    }
  }
  char **names;
  long count = otlp_wal_list_segments(dir, &names);
  if (count < 0) {
    fprintf(stderr, "cannot list %s\n", dir);
    return 2;
  }
  static const char *const ends[] = {"clean", "torn", "bad header", "I/O error"};
  uint64_t records = 0, points = 0, bytes = 0;
  int status = 0;
  for (long i = 0; i < count; i++) {
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s", dir, names[i]);
    otlp_wal_scan scan;
    otlp_wal_scan_segment(path, on_record, &st, &scan);
    printf("%s  records=%" PRIu64 " points=%" PRIu64 " bytes=%" PRIu64 "/%" PRIu64 "  %s\n",
           names[i], scan.records, scan.points, scan.valid_bytes, scan.file_bytes, ends[scan.end]);
    records += scan.records;
    points += scan.points;
    bytes += scan.valid_bytes;
    if (scan.end == OTLP_WAL_SCAN_IO_ERROR)
      status = 2;
    else if (scan.end != OTLP_WAL_SCAN_CLEAN && status == 0)
      status = 1;
  }
  otlp_wal_list_free(names, count);
  printf("segments=%ld records=%" PRIu64 " points=%" PRIu64 " bytes=%" PRIu64
         " decoded_points=%" PRIu64 " bad_payloads=%" PRIu64 " points_mismatch=%" PRIu64
         " sequence_gaps=%" PRIu64 "\n",
         count, records, points, bytes, st.decoded_points, st.bad_payloads, st.points_mismatch,
         st.sequence_gaps);
  if ((st.bad_payloads != 0 || st.points_mismatch != 0) && status == 0)
    status = 1;
  if (st.export_file != nullptr && (fclose(st.export_file) != 0 || st.write_error)) {
    fprintf(stderr, "cannot write %s\n", export_path);
    status = 2;
  }
  return status;
}
