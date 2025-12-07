from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import from_json, col, schema_of_json

MINIO_CONTAINER = "minio"
MINIO_ENDPOINT = f"http://{MINIO_CONTAINER}:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123456"
BUCKET = "spark-products-bucket"

DELTA_TABLE_PATH = f"s3a://{BUCKET}/products_delta"
CHECKPOINT_PATH = f"s3a://{BUCKET}/_checkpoints/products_console"

minio_client = Minio(
    f"{MINIO_CONTAINER}:9000",
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not minio_client.bucket_exists(BUCKET):
    minio_client.make_bucket(BUCKET)
    print(f"Bucket '{BUCKET}' created successfully")
else:
    print(f"Bucket '{BUCKET}' already exists")

spark = (SparkSession.builder
         .master("spark://spark-master:7077")
         .appName("ReadKafka")
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
                 "io.delta:delta-spark_2.13:4.0.0,"
                 "org.apache.hadoop:hadoop-aws:3.4.1,"
                 "software.amazon.awssdk:s3:2.24.6,"
                 "software.amazon.awssdk:dynamodb:2.24.6,"
                 "software.amazon.awssdk:sts:2.24.6")
         .config("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp")
         .config("spark.executor.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
         .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
         .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
         .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
         .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
         .config("spark.hadoop.fs.s3a.retry.limit", "3")
         .config("spark.hadoop.fs.s3a.retry.interval", "500")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         .config("spark.hadoop.fs.s3a.fast.upload", "true")
         .getOrCreate())

raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "kafka1:19092")
       .option("subscribe", "postgres-db.public.products")
       .option("startingOffsets", "latest")
       .load()
       )

payload_schema = StructType([
    StructField("before", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DecimalType(10, 2), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ]), True),
    StructField("after", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DecimalType(10, 2), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ]), True),
    StructField("op", StringType(), True),
    StructField("ts_ms", StringType(), True)
])

schema = StructType([
    StructField("payload", payload_schema, True)
])

out = (raw
       .selectExpr("CAST(value AS STRING) as json_str")
       .select(from_json(col("json_str"), schema).alias("data"))
       .select("data.payload.after.*")
       .filter(col("id").isNotNull()))

query = (out.writeStream
         .format("delta")
         .outputMode("append")
         .option("checkpointLocation", CHECKPOINT_PATH)
         .start(DELTA_TABLE_PATH))

query.awaitTermination()
