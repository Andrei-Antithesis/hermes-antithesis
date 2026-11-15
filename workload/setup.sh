#!/bin/sh
# Lay the Antithesis workload over a clean Hermes 2.22.2 checkout.
# Usage: workload/setup.sh [dir]   (default ./hermes-2.22.2-antithesis)
set -eu
here="$(cd "$(dirname "$0")" && pwd)"
dst="${1:-hermes-2.22.2-antithesis}"
[ -d "$dst/.git" ] || git clone --quiet --branch hermes-2.22.2 https://github.com/allegro/hermes.git "$dst"
cd "$dst"
git apply "$here/hermes-assertions.patch"          # Antithesis SDK assertions in hermes-consumers
cp "$here/dockerignore" .dockerignore
mkdir -p antithesis/hermes
for d in config driver local scripts sut; do rm -rf "antithesis/hermes/$d"; cp -r "$here/$d" "antithesis/hermes/$d"; done
echo "ready: cd $dst && antithesis/hermes/scripts/build.sh"
echo "  local loop:  antithesis/hermes/scripts/local-run.sh 600 /tmp/out"
echo "  Antithesis:  antithesis/hermes/scripts/launch.sh 24"
