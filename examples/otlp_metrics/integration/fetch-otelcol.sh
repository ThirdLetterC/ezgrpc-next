#!/usr/bin/env bash
# Downloads the OpenTelemetry Collector core distribution ("otelcol") for the collector
# integration test, verifies it against the SHA-256 published next to the archive, and
# installs the binary into <dir>:
#
#   examples/otlp_metrics/integration/fetch-otelcol.sh .tls/otelcol
#   cmake -S . -B build -DOTLP_OTELCOL_EXECUTABLE=$PWD/.tls/otelcol/otelcol
#
# The version is pinned below. Linux and macOS (amd64, arm64).
set -euo pipefail

version=0.165.0
dir=$(realpath -m "${1:?usage: $0 <dir>}")

case "$(uname -s)" in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *) echo "unsupported OS $(uname -s)" >&2; exit 2 ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) arch=amd64 ;;
  aarch64 | arm64) arch=arm64 ;;
  *) echo "unsupported architecture $(uname -m)" >&2; exit 2 ;;
esac

base=https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v$version
archive=otelcol_${version}_${os}_${arch}.tar.gz

mkdir -p "$dir"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
curl -fsSL -o "$work/$archive" "$base/$archive"
expected=$(curl -fsSL "$base/$archive.sha256" | tr -d '[:space:]')
if command -v sha256sum > /dev/null; then sha=(sha256sum); else sha=(shasum -a 256); fi
actual=$("${sha[@]}" "$work/$archive" | cut -d' ' -f1)
if [[ ! $expected =~ ^[0-9a-f]{64}$ || $actual != "$expected" ]]; then
  echo "checksum mismatch for $archive: expected '$expected', got '$actual'" >&2
  exit 1
fi
tar -xzf "$work/$archive" -C "$dir" otelcol
echo "installed $("$dir/otelcol" --version) into $dir"
