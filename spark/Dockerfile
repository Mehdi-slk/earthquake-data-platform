FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu AS builder

USER root

RUN echo "print('Downloading Spark dependencies...')" > /tmp/dummy.py && \
    /opt/spark/bin/spark-submit \
      --packages \
      org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,com.mysql:mysql-connector-j:9.4.0 \
      /tmp/dummy.py && \
    cp -n /root/.ivy2.5.2/jars/*.jar /opt/spark/jars/ && \
    rm -rf /root/.ivy2 /tmp/dummy.py

USER 185


FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu AS runtime

COPY --from=builder /opt/spark/jars/*.jar /opt/spark/jars/