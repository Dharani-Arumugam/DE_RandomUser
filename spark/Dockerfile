FROM apache/spark:3.5.1

USER root

#Install python3 + PIP
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

#Install Cassandra driver for Pyspark \
RUN pip3 install --no-cache-dir cassandra-driver

#Ensure Python points to Python3
RUN ln -sf /usr/bin/python3 /usr/bin/python
# Remove any pre-existing Kafka-related jars
RUN rm -f /opt/spark/jars/kafka-clients-*.jar && \
    rm -f /opt/spark/jars/spark-sql-kafka-0-10_*.jar && \
    rm -f /opt/spark/jars/commons-pool2-*.jar


COPY extra-jars/* /opt/spark-extra-jars/

USER spark