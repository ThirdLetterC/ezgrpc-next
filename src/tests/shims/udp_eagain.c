/* LD_PRELOAD shim for the http3_eagain test: every third sendmsg() on a UDP socket fails with
 * EAGAIN, as if the socket buffer were full. (On loopback a sender practically never sees that,
 * since datagrams go straight to the receiver.) sendmmsg(), which libuv uses for its queued sends,
 * passes through, so whatever was queued gets out. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdatomic.h>
#include <stddef.h>
#include <sys/socket.h>

static atomic_uint udp_sends;

ssize_t sendmsg(int fd, const struct msghdr *msg, int flags) {
  static ssize_t (*real_sendmsg)(int, const struct msghdr *, int);
  if (real_sendmsg == NULL)
    real_sendmsg = (ssize_t (*)(int, const struct msghdr *, int))dlsym(RTLD_NEXT, "sendmsg");
  int type = 0;
  socklen_t len = sizeof(type);
  if (getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &len) == 0 && type == SOCK_DGRAM &&
      atomic_fetch_add(&udp_sends, 1) % 3 == 2) {
    errno = EAGAIN;
    return -1;
  }
  return real_sendmsg(fd, msg, flags);
}
