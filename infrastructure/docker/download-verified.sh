#!/bin/sh
set -eu

if [ "$#" -lt 3 ]; then
  echo "usage: download-verified.sh DESTINATION SHA256 URL [URL ...]" >&2
  exit 2
fi

destination="$1"
expected_sha256="$2"
shift 2
partial="${destination}.part"

cleanup() {
  rm -f "$partial"
}
trap cleanup 0 1 2 15

for url in "$@"; do
  for delay in 0 2 5 10; do
    if [ "$delay" -gt 0 ]; then
      sleep "$delay"
    fi
    rm -f "$partial"
    if wget -nv --timeout=60 --tries=2 -O "$partial" "$url" \
      && printf '%s  %s\n' "$expected_sha256" "$partial" | sha256sum -c -; then
      mv "$partial" "$destination"
      trap - 0 1 2 15
      exit 0
    fi
  done
done

echo "failed to download a checksum-valid artifact: $destination" >&2
exit 1
