from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, stddev, expr
import pyspark.sql.functions as F

# Configuration
KAFKA_BROKER = "kafka:9092"
INPUT_TOPIC = "factory.sensor.telemetry"
OUTPUT_TOPIC = "factory.features.realtime"

def run_feature_engineering():
    spark = SparkSession.builder \
        .appName("RealTimeFeatureEngineering") \
        .getOrCreate()

    # Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .load()

    # Parse JSON/Avro (Simplified for demo)
    # In production, use from_avro
    telemetry_df = raw_df.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(F.from_json(col("json_payload"), "timestamp LONG, machine_id STRING, vibration FLOAT, temperature FLOAT").alias("data")) \
        .select("data.*")

    # Add Event Time Watermark
    telemetry_df = telemetry_df.withColumn("event_time", F.from_unixtime(col("timestamp")/1000).cast("timestamp")) \
        .withWatermark("event_time", "1 minute")

    # Calculate Rolling Window Features (Industrial Grade)
    features_df = telemetry_df.groupBy(
        window(col("event_time"), "30 seconds", "10 seconds"),
        col("machine_id")
    ).agg(
        avg("vibration").alias("vibration_mean"),
        stddev("vibration").alias("vibration_std"),
        avg("temperature").alias("temperature_mean"),
        # RMS: sqrt(mean(x^2))
        F.sqrt(avg(expr("vibration * vibration"))).alias("vibration_rms"),
        # Peak-to-Peak: max - min
        (F.max("vibration") - F.min("vibration")).alias("vibration_p2p"),
        # Kurtosis: (mean((x-mu)^4) / sigma^4)
        # Simplified Kurtosis using Spark expr
        expr("avg(pow(vibration - avg(vibration), 4)) / pow(stddev(vibration), 4)").alias("vibration_kurtosis"),
        # Skewness
        expr("avg(pow(vibration - avg(vibration), 3)) / pow(stddev(vibration), 3)").alias("vibration_skewness")
    )

    # Prepare for Kafka Output
    kafka_output = features_df.select(
        col("machine_id").alias("key"),
        F.to_json(F.struct("*")).alias("value")
    )

    query = kafka_output.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", "/tmp/spark_checkpoints/features") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_feature_engineering()
