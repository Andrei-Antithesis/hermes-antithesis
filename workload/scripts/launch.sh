#!/bin/sh
# Build, validate and launch the Hermes workload in Antithesis.
# Usage: scripts/launch.sh <max-compute-hours> [description]
set -eu
hours="${1:?max compute hours}"; desc="${2:-hermes at-least-once + lifecycle}"
here="$(cd "$(dirname "$0")/.." && pwd)"
"$here/scripts/build.sh"
snouty validate "$here/config"
snouty launch --launcher basic_test --config "$here/config" \
  --test-name "hermes-delivery-lifecycle" \
  --description "$desc" \
  --fault-tolerance high_availability \
  --fault-exclusions '^driver$' \
  --max-compute-hours "$hours" \
  --setup-timeout 15m \
  --param attrs.plugin_version=0.1.0 \
  --param attrs.sut=hermes-2.22.2 \
  --param attrs.workload=hermes-delivery-lifecycle
