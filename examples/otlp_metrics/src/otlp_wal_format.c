#include "otlp_wal_format.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#include "grpc_mem.h"

/* ---- CRC-32C, slicing by 8 ------------------------------------------------------------------ */

static uint32_t crc_table[8][256];
static uv_once_t crc_once = UV_ONCE_INIT;

static void crc_init(void) {
  for (uint32_t i = 0; i < 256; i++) {
    uint32_t c = i;
    for (int k = 0; k < 8; k++)
      c = (c & 1) ? (c >> 1) ^ 0x82F63B78u : c >> 1;
    crc_table[0][i] = c;
  }
  for (uint32_t i = 0; i < 256; i++) {
    for (int t = 1; t < 8; t++)
      crc_table[t][i] = (crc_table[t - 1][i] >> 8) ^ crc_table[0][crc_table[t - 1][i] & 0xff];
  }
}

uint32_t otlp_crc32c(uint32_t crc, const void *data, size_t len) {
  uv_once(&crc_once, crc_init);
  const uint8_t *p = data;
  crc = ~crc;
  while (len >= 8) {
    uint32_t lo =
        crc ^ ((uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24);
    uint32_t hi =
        (uint32_t)p[4] | (uint32_t)p[5] << 8 | (uint32_t)p[6] << 16 | (uint32_t)p[7] << 24;
    crc = crc_table[7][lo & 0xff] ^ crc_table[6][(lo >> 8) & 0xff] ^
          crc_table[5][(lo >> 16) & 0xff] ^ crc_table[4][lo >> 24] ^ crc_table[3][hi & 0xff] ^
          crc_table[2][(hi >> 8) & 0xff] ^ crc_table[1][(hi >> 16) & 0xff] ^ crc_table[0][hi >> 24];
    p += 8;
    len -= 8;
  }
  while (len-- > 0)
    crc = (crc >> 8) ^ crc_table[0][(crc ^ *p++) & 0xff];
  return ~crc;
}

/* ---- little-endian fields ------------------------------------------------------------------ */

static void put32(uint8_t *p, uint32_t v) {
  for (int i = 0; i < 4; i++)
    p[i] = (uint8_t)(v >> (8 * i));
}

static void put64(uint8_t *p, uint64_t v) {
  for (int i = 0; i < 8; i++)
    p[i] = (uint8_t)(v >> (8 * i));
}

static uint32_t get32(const uint8_t *p) {
  return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

static uint64_t get64(const uint8_t *p) {
  return (uint64_t)get32(p) | (uint64_t)get32(p + 4) << 32;
}

void otlp_wal_segment_header(uint8_t *out) {
  memcpy(out, OTLP_WAL_MAGIC, 8);
  put32(out + 8, OTLP_WAL_VERSION);
  put32(out + 12, 0);
}

bool otlp_wal_segment_header_valid(const uint8_t *in) {
  return memcmp(in, OTLP_WAL_MAGIC, 8) == 0 && get32(in + 8) == OTLP_WAL_VERSION;
}

void otlp_wal_record_header(uint8_t *out, uint64_t sequence, uint64_t points,
                            const uint8_t *payload, uint32_t payload_len) {
  put32(out, OTLP_WAL_RECORD_MAGIC);
  put32(out + 4, payload_len);
  put64(out + 8, sequence);
  put64(out + 16, points);
  put32(out + 24, 0);
  put32(out + 28, 0);
  uint32_t crc = otlp_crc32c(0, out, OTLP_WAL_RECORD_HEADER);
  crc = otlp_crc32c(crc, payload, payload_len);
  put32(out + 24, crc);
}

bool otlp_wal_record_parse(const uint8_t *in, otlp_wal_record *out) {
  if (get32(in) != OTLP_WAL_RECORD_MAGIC || get32(in + 28) != 0)
    return false;
  *out = (otlp_wal_record){
      .payload_len = get32(in + 4),
      .sequence = get64(in + 8),
      .points = get64(in + 16),
      .crc = get32(in + 24),
  };
  return out->payload_len <= OTLP_WAL_MAX_PAYLOAD;
}

bool otlp_wal_record_check(const uint8_t *header, const uint8_t *payload, uint32_t payload_len) {
  uint8_t copy[OTLP_WAL_RECORD_HEADER];
  memcpy(copy, header, sizeof(copy));
  put32(copy + 24, 0);
  uint32_t crc = otlp_crc32c(0, copy, sizeof(copy));
  crc = otlp_crc32c(crc, payload, payload_len);
  return crc == get32(header + 24);
}

void otlp_wal_segment_name(char out[32], uint64_t first_sequence) {
  snprintf(out, 32, "%020" PRIu64 ".wal", first_sequence);
}

bool otlp_wal_segment_name_parse(const char *name, uint64_t *first_sequence) {
  if (strlen(name) != 24 || strcmp(name + 20, ".wal") != 0)
    return false;
  uint64_t v = 0;
  for (int i = 0; i < 20; i++) {
    if (name[i] < '0' || name[i] > '9')
      return false;
    uint64_t digit = (uint64_t)(name[i] - '0');
    if (v > (UINT64_MAX - digit) / 10)
      return false;
    v = v * 10 + digit;
  }
  *first_sequence = v;
  return true;
}

/* ---- reading -------------------------------------------------------------------------------- */

/* Reads exactly `len` bytes at `offset`. Returns the bytes read (< len at end of file), or -1. */
static int64_t read_at(uv_loop_t *loop, uv_file fd, void *buf, size_t len, uint64_t offset) {
  size_t done = 0;
  while (done < len) {
    uv_fs_t req;
    uv_buf_t b = uv_buf_init((char *)buf + done, (unsigned)(len - done));
    int n = uv_fs_read(loop, &req, fd, &b, 1, (int64_t)(offset + done), nullptr);
    uv_fs_req_cleanup(&req);
    if (n < 0)
      return -1;
    if (n == 0)
      break;
    done += (size_t)n;
  }
  return (int64_t)done;
}

void otlp_wal_scan_segment(const char *path,
                           bool (*fn)(void *ctx, const otlp_wal_record *record,
                                      const uint8_t *payload),
                           void *ctx, otlp_wal_scan *out) {
  *out = (otlp_wal_scan){.end = OTLP_WAL_SCAN_IO_ERROR};
  uv_loop_t loop;
  if (uv_loop_init(&loop) != 0)
    return;
  uv_fs_t req;
  int fd = uv_fs_open(&loop, &req, path, UV_FS_O_RDONLY, 0, nullptr);
  uv_fs_req_cleanup(&req);
  if (fd < 0) {
    uv_loop_close(&loop);
    return;
  }
  uv_fs_t st;
  if (uv_fs_fstat(&loop, &st, fd, nullptr) == 0)
    out->file_bytes = st.statbuf.st_size;
  uv_fs_req_cleanup(&st);

  uint8_t header[OTLP_WAL_RECORD_HEADER];
  uint8_t *payload = nullptr;
  size_t payload_cap = 0;
  if (read_at(&loop, fd, header, OTLP_WAL_SEGMENT_HEADER, 0) != OTLP_WAL_SEGMENT_HEADER ||
      !otlp_wal_segment_header_valid(header)) {
    out->end = OTLP_WAL_SCAN_BAD_HEADER;
    goto done;
  }
  uint64_t offset = OTLP_WAL_SEGMENT_HEADER;
  out->valid_bytes = offset;
  out->end = OTLP_WAL_SCAN_CLEAN;
  for (;;) {
    int64_t n = read_at(&loop, fd, header, OTLP_WAL_RECORD_HEADER, offset);
    if (n < 0) {
      out->end = OTLP_WAL_SCAN_IO_ERROR;
      break;
    }
    if (n == 0)
      break; /* clean end */
    otlp_wal_record rec;
    if (n < OTLP_WAL_RECORD_HEADER || !otlp_wal_record_parse(header, &rec) ||
        rec.payload_len > out->file_bytes - offset - OTLP_WAL_RECORD_HEADER) {
      out->end = OTLP_WAL_SCAN_TORN;
      break;
    }
    if (rec.payload_len > payload_cap) {
      uint8_t *grown = grpc_realloc(payload, rec.payload_len);
      if (grown == nullptr) {
        out->end = OTLP_WAL_SCAN_IO_ERROR;
        break;
      }
      payload = grown;
      payload_cap = rec.payload_len;
    }
    if (read_at(&loop, fd, payload, rec.payload_len, offset + OTLP_WAL_RECORD_HEADER) !=
            (int64_t)rec.payload_len ||
        !otlp_wal_record_check(header, payload, rec.payload_len)) {
      out->end = OTLP_WAL_SCAN_TORN;
      break;
    }
    offset += OTLP_WAL_RECORD_HEADER + rec.payload_len;
    out->valid_bytes = offset;
    out->records++;
    out->points += rec.points;
    out->last_sequence = rec.sequence;
    if (fn != nullptr && !fn(ctx, &rec, payload))
      break;
  }
done:
  grpc_free(payload);
  uv_fs_close(&loop, &req, fd, nullptr);
  uv_fs_req_cleanup(&req);
  uv_loop_close(&loop);
}

static int compare_names(const void *a, const void *b) {
  return strcmp(*(char *const *)a, *(char *const *)b);
}

long otlp_wal_list_segments(const char *dir, char ***names) {
  *names = nullptr;
  uv_loop_t loop;
  if (uv_loop_init(&loop) != 0)
    return -1;
  uv_fs_t req;
  int n = uv_fs_scandir(&loop, &req, dir, 0, nullptr);
  if (n == UV_ENOENT) {
    uv_fs_req_cleanup(&req);
    uv_loop_close(&loop);
    return 0;
  }
  if (n < 0) {
    uv_fs_req_cleanup(&req);
    uv_loop_close(&loop);
    return -1;
  }
  char **list = grpc_calloc((size_t)n + 1, sizeof(char *));
  long count = 0;
  uv_dirent_t ent;
  while (list != nullptr && uv_fs_scandir_next(&req, &ent) != UV_EOF) {
    uint64_t seq;
    if (ent.type != UV_DIRENT_DIR && otlp_wal_segment_name_parse(ent.name, &seq)) {
      list[count] = grpc_strdup(ent.name);
      if (list[count] == nullptr) {
        otlp_wal_list_free(list, count);
        list = nullptr;
        break;
      }
      count++;
    }
  }
  uv_fs_req_cleanup(&req);
  uv_loop_close(&loop);
  if (list == nullptr)
    return -1;
  /* zero-padded names sort like their sequence numbers */
  qsort(list, (size_t)count, sizeof(char *), compare_names);
  *names = list;
  return count;
}

void otlp_wal_list_free(char **names, long count) {
  if (names == nullptr)
    return;
  for (long i = 0; i < count; i++)
    grpc_free(names[i]);
  grpc_free(names);
}
