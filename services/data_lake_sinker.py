from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_avro
from pyspark.sql.avro.functions import from_avro

# Configuration
KAFKA_BROKER = "kafka:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
TOPIC = "factory.sensor.telemetry"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
SINK_PATH = "s3a://industrial-data-lake/raw/telemetry/"

def run_sinker():
    spark = SparkSession.builder \
        .appName("DataLakeSinker") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Read from Kafka
    # Note: In a real environment, we'd use the abris or confluent-spark-avro library 
    # to handle Schema Registry integration automatically.
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Cast value to string/json or use from_avro
    # For simplicity in this demo script, we assume a JSON-like schema extraction 
    # if not using the full Avro binary deserialization in Spark.
    # In production, we'd use: from_avro(col("value"), json_schema)
    
    query = df.writeStream \
        .format("parquet") \
        .option("path", SINK_PATH) \
        .option("checkpointLocation", "/tmp/spark_checkpoints/data_lake_sinker") \
        .partitionBy("factory_id", "machine_id") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_sinker()
