import os
import dotenv
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

dotenv.load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
S3_BUCKET = os.getenv("S3_BUCKET")

print(f"AWS_REGION: {AWS_REGION}")
print(f"AWS_ACCESS_KEY_ID: {AWS_ACCESS_KEY_ID}")
print(f"AWS_SECRET_ACCESS_KEY: {AWS_SECRET_ACCESS_KEY}")
print(f"S3_BUCKET: {S3_BUCKET}")
print(f"KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"KAFKA_TOPIC: {KAFKA_TOPIC}")

print("Starting Spark Session...")

spark = (
    SparkSession
    .builder
    .appName("Spotify Analytics")
    .config("spark.master", "local[2]")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
    .config("spark.sql.defaultCatalog", "nessie")
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v2")
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.catalog.nessie.warehouse", f"s3a://spotify-warehouse")
    .config("spark.sql.catalog.nessie.authentication.type", "NONE")
    .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.nessie.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
    .config("spark.sql.catalog.nessie.s3.region", AWS_REGION)
    .config("spark.sql.catalog.nessie.s3.access-key-id", AWS_ACCESS_KEY_ID)
    .config("spark.sql.catalog.nessie.s3.secret-access-key", AWS_SECRET_ACCESS_KEY)
    .getOrCreate()
)

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("artist_id", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("song_name", StringType(), True),
    StructField("song_year", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("device", StringType(), True),
    StructField("country", StringType(), True),
    StructField("timestamp", StringType(), True),
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), event_schema).alias("e"))
    .select("e.*")
    .withColumn("timestamp", to_timestamp(col("timestamp")))
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.spotify")

spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.spotify.spotify_events (
        event_id string,
        user_id string,
        artist_id string,
        artist_name string,
        song_id string,
        song_name string,
        song_year string,
        event_type string,
        device string,
        country string,
        timestamp timestamp
    )
    USING iceberg
    PARTITIONED BY (days(timestamp))
    TBLPROPERTIES (
        'format-version'='2',
        'write.metadata.delete-after-commit.enabled'='true',
        'write.metadata.previous-versions-max'='5',
        'history.expire.max-snapshot-age-ms'='3600000',
        'history.expire.min-snapshots-to-keep'='5'
    )
""")

checkpoint = "/opt/checkpoints/spotify.spotify_events"

print("Starting streaming write to Iceberg table...")
query = (
    parsed_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .option("fanout-enabled", "true")
    .trigger(processingTime="60 seconds")
    .toTable("nessie.spotify.spotify_events")
)

print("✅ Streaming started successfully!")
print(f"Writing to: nessie.spotify.spotify_events")
print(f"Checkpoint: {checkpoint}")
query.awaitTermination()