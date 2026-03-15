FROM apache/airflow:2.9.3-python3.10

USER airflow

RUN pip install apache-airflow-providers-apache-spark==4.4.0 --no-deps
#  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

USER root

# spark client
RUN apt-get update && apt-get install -y openjdk-17-jre-headless

# install spark binaries
ENV SPARK_VERSION=3.5.8
ENV HADOOP_VERSION=3

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/

RUN ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

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


USER airflow
