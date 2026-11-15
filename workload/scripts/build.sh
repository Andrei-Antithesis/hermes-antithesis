#!/bin/sh
# Build every image the workload uses.
set -eu
cd "$(dirname "$0")/../../.."   # repository root
for c in frontend consumers management; do
  docker build -f antithesis/hermes/sut/Dockerfile --target "$c" -t "hermes-$c:antithesis" .
done
docker build -f antithesis/hermes/driver/Dockerfile -t hermes-driver:antithesis antithesis/hermes
