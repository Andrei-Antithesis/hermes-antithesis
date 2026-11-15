# Instrumented Hermes images (frontend, consumers, management) for Antithesis.
# Build context: repository root. Select a component with --target.
FROM gradle:9.2.1-jdk25 AS build
COPY --chown=gradle:gradle . /home/gradle/src/
WORKDIR /home/gradle/src/
RUN --mount=type=cache,target=/home/gradle/.gradle/caches,uid=1000,gid=1000 \
    gradle --no-daemon :hermes-frontend:distZip :hermes-consumers:distZip :hermes-management:distZip \
      -x buildHermesConsole -x attachHermesConsole -x prepareIndexTemplate -x javadoc \
 && mkdir -p /out \
 && for c in frontend consumers management; do \
      mkdir -p /out/$c && cd /out/$c && unzip -q /home/gradle/src/hermes-$c/build/distributions/*.zip \
      && mv hermes-$c-* dist \
      && mkdir app && mv dist/lib/hermes-*.jar app/ && cd /home/gradle/src; \
    done

FROM eclipse-temurin:25.0.1_8-jre AS base
RUN apt-get update && apt-get -y install --no-install-recommends curl jq procps iproute2 netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*
# Antithesis catalogs/instruments every jar under /opt/antithesis/catalog (one symlink level).
# Only Hermes' own jars are instrumented; third-party libs stay in /opt/hermes/lib.
ENV JAVA_OPTS="-Xms128m -Xmx384m -XX:+UseSerialGC -XX:TieredStopAtLevel=1"
COPY antithesis/hermes/sut/run.sh /usr/local/bin/run-hermes
RUN chmod 755 /usr/local/bin/run-hermes

FROM base AS frontend
COPY --from=build /out/frontend/app /opt/hermes/app
COPY --from=build /out/frontend/dist/lib /opt/hermes/lib
RUN mkdir -p /opt/antithesis/catalog && ln -s /opt/hermes/app /opt/antithesis/catalog/hermes-frontend
COPY antithesis/hermes/sut/frontend.yaml /etc/hermes/frontend.yaml
COPY docker/latest/frontend/logback.xml /etc/hermes/logback.xml
ENV HERMES_MAIN=pl.allegro.tech.hermes.frontend.HermesFrontend \
    HERMES_OPTS="-Dspring.config.location=file:///etc/hermes/frontend.yaml -Dlogback.configurationFile=/etc/hermes/logback.xml"
CMD ["/usr/local/bin/run-hermes"]

FROM base AS consumers
COPY --from=build /out/consumers/app /opt/hermes/app
COPY --from=build /out/consumers/dist/lib /opt/hermes/lib
RUN mkdir -p /opt/antithesis/catalog && ln -s /opt/hermes/app /opt/antithesis/catalog/hermes-consumers
COPY antithesis/hermes/sut/consumers.yaml /etc/hermes/consumers.yaml
COPY docker/latest/consumers/logback.xml /etc/hermes/logback.xml
ENV HERMES_MAIN=pl.allegro.tech.hermes.consumers.HermesConsumers \
    HERMES_OPTS="-Dspring.config.location=file:///etc/hermes/consumers.yaml -Dlogback.configurationFile=/etc/hermes/logback.xml"
CMD ["/usr/local/bin/run-hermes"]

FROM base AS management
COPY --from=build /out/management/app /opt/hermes/app
COPY --from=build /out/management/dist/lib /opt/hermes/lib
RUN mkdir -p /opt/antithesis/catalog && ln -s /opt/hermes/app /opt/antithesis/catalog/hermes-management
COPY antithesis/hermes/sut/management.yaml /etc/hermes/management.yaml
ENV HERMES_MAIN=pl.allegro.tech.hermes.management.HermesManagement \
    HERMES_OPTS="-Dspring.config.location=file:///etc/hermes/management.yaml"
CMD ["/usr/local/bin/run-hermes"]
