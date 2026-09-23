FROM apache/airflow:3.0.6-python3.10

USER root

# Java 17 is required by Spark 4.1.3
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Spark 4.1.3 client
RUN curl -fL \
    https://archive.apache.org/dist/spark/spark-4.1.3/spark-4.1.3-bin-hadoop3.tgz \
    -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && ln -s /opt/spark-4.1.3-bin-hadoop3 /opt/spark \
    && rm /tmp/spark.tgz

# Install Spark Kafka + Avro + PostgreSQL dependencies
RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar \
    -o /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.1.3.jar \
    && curl -fL \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar \
    -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.1.3.jar \
    && curl -fL \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
    -o /opt/spark/jars/kafka-clients-3.9.1.jar \
    && curl -fL \
    https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/4.1.3/spark-avro_2.13-4.1.3.jar \
    -o /opt/spark/jars/spark-avro_2.13-4.1.3.jar \
    && curl -fL \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar \
    -o /opt/spark/jars/postgresql-42.7.8.jar

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

# Airflow Spark provider + Kafka producer dependency
RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark==6.3.2" \
    confluent-kafka \
    pandas \
    fastavro \
    geopandas