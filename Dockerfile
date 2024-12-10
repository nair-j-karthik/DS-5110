FROM bde2020/spark-master:3.1.1-hadoop3.2

# Install Python3 and pip using apk
RUN apk add --no-cache python3 py3-pip && \
    pip3 install --no-cache-dir pyspark

# Copy your Spark script into the container
COPY spark/spark_streaming.py /opt/spark/scripts/spark_streaming.py

# Set the working directory
WORKDIR /opt/spark/scripts

# Command to run Spark with the Kafka package and your script
CMD /spark/bin/spark-submit --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
    /opt/spark/scripts/spark_streaming.py
