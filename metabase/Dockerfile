FROM metabase/metabase:latest

RUN mkdir -p /plugins

RUN curl -L -o /plugins/clickhouse-jdbc-0.8.4-shaded.jar \
https://github.com/ClickHouse/clickhouse-java/releases/download/v0.8.4/clickhouse-jdbc-0.8.4-shaded.jar
