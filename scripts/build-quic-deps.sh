#!/usr/bin/env bash
# Builds ngtcp2 (QUIC) and nghttp3 (HTTP/3) for the HTTP/3 transport and installs them into a
# prefix that the grpc build can use:
#
#   scripts/build-quic-deps.sh <openssl|boringssl|awslc|wolfssl|gnutls> <prefix> [workdir]
#   cmake -S . -B build-h3 -DGRPC_HTTP3=ON -DGRPC_TLS_BACKEND=<backend> -DGRPC_TLS_PREFIX=<prefix>
#
# ngtcp2's crypto helper is built for the given TLS backend. For openssl, boringssl and awslc
# the TLS library must already be in <prefix> (scripts/build-tls-backend.sh); OpenSSL must be
# 3.5 or newer. wolfssl and gnutls are taken from the system. Distribution packages of
# ngtcp2/nghttp3 are too old (Ubuntu 24.04 ships pre-1.0 releases), so both are built here.
# Sources are pinned to a release and verified against a SHA-256 checksum.
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "usage: $0 <openssl|boringssl|awslc|wolfssl|gnutls> <prefix> [workdir]" >&2
  exit 2
fi

backend=$1
prefix=$(realpath -m "$2")
workdir=$(realpath -m "${3:-$(mktemp -d)}")

case "$backend" in
  openssl) crypto=(-DENABLE_OPENSSL=ON -DENABLE_STATIC_LIB=OFF) ;;
  # ngtcp2 builds its BoringSSL helper only as a static library; PIC lets it go into libgrpc.so
  boringssl | awslc) crypto=(-DENABLE_OPENSSL=OFF -DENABLE_BORINGSSL=ON -DENABLE_STATIC_LIB=ON
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DBORINGSSL_INCLUDE_DIR="$prefix/include"
    "-DBORINGSSL_LIBRARIES=$prefix/lib/libssl.so;$prefix/lib/libcrypto.so") ;;
  wolfssl) crypto=(-DENABLE_OPENSSL=OFF -DENABLE_WOLFSSL=ON -DENABLE_STATIC_LIB=OFF) ;;
  gnutls) crypto=(-DENABLE_OPENSSL=OFF -DENABLE_GNUTLS=ON -DENABLE_STATIC_LIB=OFF) ;;
  *)
    echo "unsupported backend for HTTP/3: $backend (libressl and rustls have no ngtcp2 helper)" >&2
    exit 2
    ;;
esac

nghttp3_version=1.19.0
nghttp3_sha256=0dc9a55d38ca04c282524bfecdc5e31d393a4bb430200263dd06a87801a150af
ngtcp2_version=1.26.0
ngtcp2_sha256=85174d674e7f8fd911a827c6e483fd417b21b1f54d7853dac464cee446c8d35f

mkdir -p "$workdir" "$prefix"

# fetch <name> <version> <sha256>: downloads, verifies and unpacks a release into $workdir
fetch() {
  local name=$1 version=$2 sha256=$3
  local tarball=$workdir/$name-$version.tar.gz
  if [ ! -f "$tarball" ]; then
    curl -fsSL -o "$tarball.part" \
      "https://github.com/ngtcp2/$name/releases/download/v$version/$name-$version.tar.gz"
    mv "$tarball.part" "$tarball"
  fi
  echo "$sha256  $tarball" | sha256sum -c -
  rm -rf "${workdir:?}/$name-$version"
  tar -xzf "$tarball" -C "$workdir"
}

# cmake_install <srcdir> [cmake args...]
cmake_install() {
  local src=$1
  shift
  cmake -S "$src" -B "$src/build" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$prefix" \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_PREFIX_PATH="$prefix" \
    -DENABLE_SHARED_LIB=ON \
    "$@"
  cmake --build "$src/build"
  cmake --install "$src/build"
}

fetch nghttp3 "$nghttp3_version" "$nghttp3_sha256"
cmake_install "$workdir/nghttp3-$nghttp3_version" -DENABLE_LIB_ONLY=ON -DENABLE_STATIC_LIB=OFF

fetch ngtcp2 "$ngtcp2_version" "$ngtcp2_sha256"
# OPENSSL_ROOT_DIR makes FindOpenSSL pick the prefix's OpenSSL 3.5 over the system one
cmake_install "$workdir/ngtcp2-$ngtcp2_version" -DENABLE_LIB_ONLY=ON \
  -DOPENSSL_ROOT_DIR="$prefix" "${crypto[@]}"

echo "installed nghttp3 $nghttp3_version and ngtcp2 $ngtcp2_version ($backend) into $prefix"
