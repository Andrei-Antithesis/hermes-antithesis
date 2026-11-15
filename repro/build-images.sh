#!/bin/sh
# Build plain Docker images of unmodified Hermes 2.22.2 (and a consumers image with the one-line fix).
# Needs git and Docker. The Gradle build runs inside Docker; it takes a few minutes the first time.
set -eu
here="$(cd "$(dirname "$0")" && pwd)"; root="$(cd "$here/.." && pwd)"
src="${HERMES_SRC:-$root/.hermes-2.22.2}"
[ -d "$src/.git" ] || git clone --quiet --branch hermes-2.22.2 https://github.com/allegro/hermes.git "$src"
cd "$src"
git checkout --quiet -f hermes-2.22.2
mkdir -p antithesis/hermes && rm -rf antithesis/hermes/sut && cp -r "$root/workload/sut" antithesis/hermes/sut
cp "$root/workload/dockerignore" .dockerignore
for c in consumers frontend management; do
  docker build -q -f antithesis/hermes/sut/Dockerfile --target "$c" -t "hermes-$c:upstream-2.22.2" .
done
git apply "$here/fix-batchconsumer-consuming-flag.diff"
docker build -q -f antithesis/hermes/sut/Dockerfile --target consumers -t hermes-consumers:fixed-2.22.2 .
git checkout --quiet -- hermes-consumers
git apply "$here/fix-batchconsumer-skip-empty-batch.diff"
docker build -q -f antithesis/hermes/sut/Dockerfile --target consumers -t hermes-consumers:fixed-empty-2.22.2 .
git checkout --quiet -- hermes-consumers
git apply "$here/fix-zookeeper-ensurepathexists-race.diff"
for c in consumers frontend management; do
  docker build -q -f antithesis/hermes/sut/Dockerfile --target "$c" -t "hermes-$c:fixed-zk-2.22.2" .
done
git checkout --quiet -- hermes-common
echo "built hermes-{consumers,frontend,management}:upstream-2.22.2, hermes-consumers:fixed-2.22.2 (bug 1 fix),"
echo "hermes-consumers:fixed-empty-2.22.2 (bug 3 fix) and hermes-{consumers,frontend,management}:fixed-zk-2.22.2 (bug 4 fix)"
