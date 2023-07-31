FROM adoptopenjdk/openjdk11

COPY clickhouse-connector /clickhouse-connector
ENV JAVA_OPTS=-verbose:gc
WORKDIR /clickhouse-connector

ENTRYPOINT ["/bin/sh", "-c", "./runner.sh"]