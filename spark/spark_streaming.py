#!/usr/bin/env python3
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaSparkStreaming")

def process_stream():
    try:
        logger.info("Starting Spark Session...")
        spark = SparkSession.builder \
            .appName("KafkaSparkStreaming") \
            .master("spark://spark-master:7077") \
            .getOrCreate()

        logger.info("Defining schema for Kafka messages...")
        schema = StructType([
            StructField("sale_id", IntegerType(), True),
            StructField("product", StringType(), True),
            StructField("quantity_sold", IntegerType(), True),
            StructField("each_price", FloatType(), True),
            StructField("sales", FloatType(), True),
            StructField("sale_date", TimestampType(), True)
        ])

        logger.info("Reading stream from Kafka topic 'sales'...")
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "sales") \
            .load() \
            .select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

        def write_to_postgres(batch_df, batch_id):
            logger.info("Writing batch to PostgreSQL...")
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/sales_data") \
                .option("dbtable", "sales_data") \
                .option("user", "postgres") \
                .option("password", "your_password") \
                .mode("append") \
                .save()

        logger.info("Starting streaming query...")
        query = df.writeStream \
            .foreachBatch(write_to_postgres) \
            .start()

        query.awaitTermination()
    except Exception as e:
        logger.error("Error in processing stream: %s", e, exc_info=True)

if __name__ == "__main__":
    logger.info("Starting Kafka-Spark-PostgreSQL streaming application...")
    process_stream()
