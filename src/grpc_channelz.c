#include <string.h>
#include "core.h"

static size_t pb_varint_len(uint64_t v) {
  size_t n = 1;
  while (v >= 0x80) {
    v >>= 7;
    n++;
  }
  return n;
}

static size_t pb_write_varint(uint8_t *buf, uint64_t v) {
  size_t n = 0;
  while (v >= 0x80) {
    buf[n++] = (uint8_t)((v & 0x7f) | 0x80);
    v >>= 7;
  }
  buf[n++] = (uint8_t)v;
  return n;
}

/* Encodes grpc.channelz.v1.GetServersResponse:
 *   repeated Server server = 1;
 *   bool end = 2;
 * Server { ServerRef ref = 1 { int64 server_id = 1; string name = 2; };
 *          ServerData data = 2 { int64 calls_started = 2; int64 calls_succeeded = 3; int64 calls_failed = 4; }; }
 */
static void channelz_get_servers(grpc_call *call, [[maybe_unused]] const grpc_message *request,
                                 void *userdata) {
  grpc_server *server = userdata;
  grpc_server_stats st = {0};
  (void)grpc_server_get_stats(server, &st);

  /* ServerRef { server_id = 1, name = "ezGRPC-next" } */
  const char *name = "ezGRPC-next";
  size_t name_len = strlen(name);
  uint8_t ref_buf[64];
  size_t ref_len = 0;
  ref_buf[ref_len++] = (1 << 3) | 0;
  ref_len += pb_write_varint(ref_buf + ref_len, 1);
  ref_buf[ref_len++] = (2 << 3) | 2;
  ref_len += pb_write_varint(ref_buf + ref_len, name_len);
  memcpy(ref_buf + ref_len, name, name_len);
  ref_len += name_len;

  /* ServerData { calls_started = 2, calls_succeeded = 3, calls_failed = 4 } */
  uint8_t data_buf[64];
  size_t data_len = 0;
  if (st.total_calls > 0) {
    data_buf[data_len++] = (2 << 3) | 0;
    data_len += pb_write_varint(data_buf + data_len, st.total_calls);
  }
  if (st.calls_ok > 0) {
    data_buf[data_len++] = (3 << 3) | 0;
    data_len += pb_write_varint(data_buf + data_len, st.calls_ok);
  }
  if (st.calls_error > 0) {
    data_buf[data_len++] = (4 << 3) | 0;
    data_len += pb_write_varint(data_buf + data_len, st.calls_error);
  }

  /* Server { ref = 1, data = 2 } */
  uint8_t srv_buf[192];
  size_t srv_len = 0;
  srv_buf[srv_len++] = (1 << 3) | 2;
  srv_len += pb_write_varint(srv_buf + srv_len, ref_len);
  memcpy(srv_buf + srv_len, ref_buf, ref_len);
  srv_len += ref_len;
  if (data_len > 0) {
    srv_buf[srv_len++] = (2 << 3) | 2;
    srv_len += pb_write_varint(srv_buf + srv_len, data_len);
    memcpy(srv_buf + srv_len, data_buf, data_len);
    srv_len += data_len;
  }

  /* GetServersResponse { server = 1, end = 2 (true) } */
  uint8_t resp[256];
  size_t resp_len = 0;
  resp[resp_len++] = (1 << 3) | 2;
  resp_len += pb_write_varint(resp + resp_len, srv_len);
  memcpy(resp + resp_len, srv_buf, srv_len);
  resp_len += srv_len;
  resp[resp_len++] = (2 << 3) | 0;
  resp[resp_len++] = 1;

  (void)grpc_call_reply_data(call, resp, resp_len);
}

/* Encodes grpc.channelz.v1.GetServerSocketsResponse:
 *   repeated SocketRef socket_ref = 1 { int64 socket_id = 1; string name = 2; };
 *   bool end = 2;
 */
static void channelz_get_server_sockets(grpc_call *call,
                                        [[maybe_unused]] const grpc_message *request,
                                        void *userdata) {
  grpc_server *server = userdata;
  size_t total = 2; /* end = true */
  for (size_t i = 0; i < server->nb_sessions; i++) {
    if (!server->sessions[i].is_active || server->sessions[i].is_closing)
      continue;
    size_t plen = strlen(server->sessions[i].peer);
    size_t inner = 1 + pb_varint_len(i + 1) + (plen > 0 ? 1 + pb_varint_len(plen) + plen : 0);
    total += 1 + pb_varint_len(inner) + inner;
  }

  uint8_t *resp = grpc_malloc(total);
  if (resp == nullptr) {
    (void)grpc_call_finish(call, GRPC_STATUS_RESOURCE_EXHAUSTED, "out of memory");
    return;
  }
  size_t off = 0;
  for (size_t i = 0; i < server->nb_sessions; i++) {
    if (!server->sessions[i].is_active || server->sessions[i].is_closing)
      continue;
    const char *peer = server->sessions[i].peer;
    size_t plen = strlen(peer);
    size_t inner = 1 + pb_varint_len(i + 1) + (plen > 0 ? 1 + pb_varint_len(plen) + plen : 0);
    resp[off++] = (1 << 3) | 2;
    off += pb_write_varint(resp + off, inner);
    resp[off++] = (1 << 3) | 0;
    off += pb_write_varint(resp + off, i + 1);
    if (plen > 0) {
      resp[off++] = (2 << 3) | 2;
      off += pb_write_varint(resp + off, plen);
      memcpy(resp + off, peer, plen);
      off += plen;
    }
  }
  resp[off++] = (2 << 3) | 0;
  resp[off++] = 1;

  (void)grpc_call_reply_data(call, resp, off);
  grpc_free(resp);
}

GRPC_API grpc_err grpc_channelz_enable(grpc_server *server) {
  if (server == nullptr)
    return GRPC_ERR_INVALID;
  grpc_err err =
      grpc_server_handle(server, GRPC_CHANNELZ_PATH_GET_SERVERS, channelz_get_servers, server);
  if (err != GRPC_OK)
    return err;
  err = grpc_server_handle(server, GRPC_CHANNELZ_PATH_GET_SERVER_SOCKETS,
                           channelz_get_server_sockets, server);
  if (err != GRPC_OK)
    (void)grpc_server_unregister(server, GRPC_CHANNELZ_PATH_GET_SERVERS);
  return err;
}
