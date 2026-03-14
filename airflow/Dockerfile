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


USER airflow
