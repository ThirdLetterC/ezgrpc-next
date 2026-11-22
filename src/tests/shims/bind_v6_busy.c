/* LD_PRELOAD shim for the port_retry test: the first IPv6 bind() to a given (nonzero) port fails
 * with EADDRINUSE, as if the port the IPv4 listener picked (with port 0) were already taken on
 * IPv6. Binds to port 0 pass through, so the test can still probe for IPv6 support. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stddef.h>
#include <stdatomic.h>
#include <netinet/in.h>
#include <sys/socket.h>

static atomic_int v6_binds;

int bind(int fd, const struct sockaddr *addr, socklen_t len) {
  static int (*real_bind)(int, const struct sockaddr *, socklen_t);
  if (real_bind == NULL)
    real_bind = (int (*)(int, const struct sockaddr *, socklen_t))dlsym(RTLD_NEXT, "bind");
  if (addr->sa_family == AF_INET6 && ((const struct sockaddr_in6 *)addr)->sin6_port != 0 &&
      atomic_fetch_add(&v6_binds, 1) == 0) {
    errno = EADDRINUSE;
    return -1;
  }
  return real_bind(fd, addr, len);
}
