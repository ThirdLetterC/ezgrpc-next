/* grpc_http3.c - HTTP/3 feature queries, and stubs for builds without HTTP/3
 *
 * grpc_http3_available() and grpc_http3_versions() are always present. With GRPC_ENABLE_HTTP3 the
 * transport lives in grpc_http3_server*.c / grpc_http3_client*.c; without it, the internal hooks of
 * internal_http3.h are stubbed here so the server core links unchanged and enabling HTTP/3 fails
 * with GRPC_ERR_UNSUPPORTED.
 */
#include <errno.h>
#include <stdio.h>

#include "core.h"
#include "internal_client.h"
#include "internal_http3.h"

#if defined(GRPC_ENABLE_HTTP3)
#include <nghttp3/nghttp3.h>
#include <ngtcp2/ngtcp2.h>
#include "internal_helpers.h"
#include "internal_quic_tls.h"

const ngtcp2_mem grpc_quic_mem = {
    .user_data = nullptr,
    .malloc = grpc_mem_hook_malloc,
    .free = grpc_mem_hook_free,
    .calloc = grpc_mem_hook_calloc,
    .realloc = grpc_mem_hook_realloc,
};
const nghttp3_mem grpc_h3_mem = {
    .user_data = nullptr,
    .malloc = grpc_mem_hook_malloc,
    .free = grpc_mem_hook_free,
    .calloc = grpc_mem_hook_calloc,
    .realloc = grpc_mem_hook_realloc,
};

#if defined(__linux__)
#include <netinet/udp.h>
#include <sys/socket.h>
#endif

int grpc_h3_send_gso(uv_udp_t *udp, const struct sockaddr *to, const uint8_t *data, size_t len,
                     size_t gsolen) {
#if defined(__linux__) && defined(UDP_SEGMENT)
  uv_os_fd_t fd;
  if (gsolen == 0 || gsolen > UINT16_MAX || uv_fileno((const uv_handle_t *)udp, &fd) != 0)
    return UV_ENOTSUP;
  struct iovec iov = {.iov_base = grpc_unconst(data), .iov_len = len};
  union {
    char buf[CMSG_SPACE(sizeof(uint16_t))];
    struct cmsghdr align;
  } control;
  memset(&control, 0, sizeof(control));
  struct msghdr msg = {
      .msg_name = grpc_unconst(to),
      .msg_namelen = to == nullptr               ? 0
                     : to->sa_family == AF_INET6 ? (socklen_t)sizeof(struct sockaddr_in6)
                                                 : (socklen_t)sizeof(struct sockaddr_in),
      .msg_iov = &iov,
      .msg_iovlen = 1,
      .msg_control = control.buf,
      .msg_controllen = sizeof(control.buf),
  };
  struct cmsghdr *cm = CMSG_FIRSTHDR(&msg);
  cm->cmsg_level = SOL_UDP;
  cm->cmsg_type = UDP_SEGMENT;
  cm->cmsg_len = CMSG_LEN(sizeof(uint16_t));
  uint16_t seg = (uint16_t)gsolen;
  memcpy(CMSG_DATA(cm), &seg, sizeof(seg));
  ssize_t n;
  do {
    n = sendmsg(fd, &msg, 0);
  } while (n < 0 && errno == EINTR);
  if (n >= 0)
    return 0;
#if EWOULDBLOCK != EAGAIN
  if (errno == EWOULDBLOCK)
    return UV_EAGAIN;
#endif
  if (errno == EAGAIN)
    return UV_EAGAIN;
  if (errno == ENOBUFS)
    return UV_ENOBUFS;
  /* EIO: the route has no checksum offload; EINVAL / ENOPROTOOPT: no UDP GSO in this kernel */
  if (errno == EIO || errno == EINVAL || errno == ENOPROTOOPT || errno == EOPNOTSUPP)
    return UV_ENOTSUP;
  return uv_translate_sys_error(errno);
#else
  (void)udp;
  (void)to;
  (void)data;
  (void)len;
  (void)gsolen;
  return UV_ENOTSUP;
#endif
}

GRPC_API bool grpc_http3_available(void) {
  return true;
}

void http3_thread_cleanup(void) {
  quic_tls_thread_cleanup();
}

static char versions[96];
static uv_once_t versions_once = UV_ONCE_INIT;

/* Formats the run-time ngtcp2 / nghttp3 versions and the crypto helper, once. */
static void build_versions(void) {
  const ngtcp2_info *tcp = ngtcp2_version(0);
  const nghttp3_info *h3 = nghttp3_version(0);
  snprintf(versions, sizeof(versions), "ngtcp2/%s nghttp3/%s crypto=%s",
           tcp != nullptr ? tcp->version_str : "?", h3 != nullptr ? h3->version_str : "?",
           quic_tls_crypto_name());
}

GRPC_API const char *grpc_http3_versions(void) {
  uv_once(&versions_once, build_versions);
  return versions;
}
#else

GRPC_API bool grpc_http3_available(void) {
  return false;
}

void http3_thread_cleanup(void) {}

GRPC_API const char *grpc_http3_versions(void) {
  return "none";
}

grpc_err http3_server_start(grpc_server *server, const grpc_tls_config *tls) {
  (void)tls;
  LOG_ERROR(server, "http3 was requested but the library was built without HTTP/3\n");
  return GRPC_ERR_UNSUPPORTED;
}

void http3_server_leave_cluster(grpc_server *server) {
  (void)server;
}

void http3_server_stop_accepting(grpc_server *server) {
  (void)server;
}

void http3_server_close_handles(grpc_server *server) {
  (void)server;
}

void http3_server_free(grpc_server *server) {
  (void)server;
}

grpc_err http3_server_tls_prepare(grpc_server *server, const grpc_tls_config *cfg, void **out) {
  (void)server;
  (void)cfg;
  *out = nullptr;
  return GRPC_OK;
}

void http3_server_tls_install(grpc_server *server, void *ctx) {
  (void)server;
  (void)ctx;
}

void http3_server_tls_discard(grpc_server *server, void *ctx) {
  (void)server;
  (void)ctx;
}

uint16_t http3_server_port(const grpc_server *server) {
  (void)server;
  return 0;
}

/* client stubs: grpc_client_new() rejects GRPC_TRANSPORT_HTTP3 before any of these run */
void *h3c_tls_ctx_new(const grpc_client_config *config) {
  (void)config;
  return nullptr;
}
void h3c_tls_ctx_free(void *ctx) {
  (void)ctx;
}
grpc_err h3c_connect(client_subchannel *s) {
  (void)s;
  return GRPC_ERR_UNSUPPORTED;
}
void h3c_disconnect(client_subchannel *s) {
  (void)s;
}
bool h3c_usable(client_subchannel *s) {
  (void)s;
  return false;
}
size_t h3c_orphan_count(const client_subchannel *s) {
  (void)s;
  return 0;
}
void h3c_send_counters(const client_subchannel *s, uint64_t *queued, uint64_t *dropped,
                       uint64_t *gso_sends) {
  (void)s;
  *queued = *dropped = *gso_sends = 0;
}
int h3c_submit_request(client_subchannel *s, grpc_client_stream *st, const grpc_nv *nva,
                       size_t nvlen) {
  (void)s;
  (void)st;
  (void)nva;
  (void)nvlen;
  return -1;
}
void h3c_resume(client_subchannel *s, grpc_client_stream *st) {
  (void)s;
  (void)st;
}
void h3c_reset(client_subchannel *s, grpc_client_stream *st) {
  (void)s;
  (void)st;
}
void h3c_flush(client_subchannel *s) {
  (void)s;
}
void h3c_stream_free(grpc_client_stream *st) {
  (void)st;
}
#endif
