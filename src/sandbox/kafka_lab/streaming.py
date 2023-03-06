from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, StructField

scala_version = '2.12'
spark_version = '3.1.1'

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:2.8.1'
]

spark = SparkSession.builder \
                    .appName("KafkaSparkTest") \
                    .config("spark.jars.packages", ",".join(packages)) \
                    .getOrCreate()

# Set the Kafka broker and topic
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "test_topic_rtppm"

# Set the Kafka consumer options
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "subscribe": kafka_topic,
    "startingOffsets": "latest"
}

# Define the schema of the incoming Kafka messages
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType())
])

# Read from Kafka as a streaming DataFrame
df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select(col("data.*"))
)


# Print the streaming DataFrame schema and data
query = (
    df.writeStream
    .outputMode("append")
    .format("console")
    .start()
)

query.awaitTermination()


# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:2.8.1 streaming.py