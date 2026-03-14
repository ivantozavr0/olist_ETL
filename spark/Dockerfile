FROM apache/spark:3.5.8-scala2.12-java11-r-ubuntu

USER root

RUN mkdir -p /opt/spark/jars

# Postgres
RUN curl -L -o /opt/spark/jars/postgresql-42.7.4.jar \
https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

# ClickHouse JDBC
RUN curl -L -o /opt/spark/jars/clickhouse-jdbc-0.8.4-all.jar \
https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.8.4/clickhouse-jdbc-0.8.4-all.jar

# Hadoop AWS
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# AWS SDK
RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

USER spark

#COPY ./jars/*.jar /opt/spark/jars/
