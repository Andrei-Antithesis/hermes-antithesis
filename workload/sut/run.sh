#!/bin/sh
# Start a Hermes component with java as PID 1.
# WAIT_FOR_URL (optional): wait up to 120s for it to answer 200 first. Frontends and
# consumers wait for management, which creates the shared ZooKeeper paths; starting
# them together on a fresh ZooKeeper hits a check-then-create race in
# ZookeeperBasedRepository.ensurePathExists (NodeExistsException crashes the node).
if [ -n "${WAIT_FOR_URL:-}" ]; then
  i=0
  until curl -sf --connect-timeout 1 --max-time 2 -o /dev/null "$WAIT_FOR_URL" || [ $i -ge 120 ]; do i=$((i+1)); sleep 1; done
fi
# Directory wildcards, so jars Antithesis injects next to the instrumented ones load too.
# Antithesis injects slf4j-simple into /opt/hermes/app; pin SLF4J to Logback (Spring Boot
# refuses to start otherwise) and keep the application's own libraries first.
exec java $JAVA_OPTS $HERMES_OPTS \
  -Dslf4j.provider=ch.qos.logback.classic.spi.LogbackServiceProvider \
  -cp "/opt/hermes/lib/*:/opt/hermes/app/*" "$HERMES_MAIN"
