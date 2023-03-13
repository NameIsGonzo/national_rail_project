from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from utils import incoming_schemas as schemas


scala_version = "2.12"
spark_version = "3.1.1"

packages = [
    f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
    "org.apache.kafka:kafka-clients:2.8.1",
]

spark = (
    SparkSession.builder.appName("KafkaSparkTest")
    .config("spark.jars.packages", ",".join(packages))
    .getOrCreate()
)

# Set the Kafka broker and topic
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "rtppmdata.nationalpage.nationalppm"

# Set the Kafka consumer options
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "subscribe": kafka_topic,
    "startingOffsets": "latest",
}


# Read from Kafka as a streaming DataFrame
# rtppmdata.nationalpage.nationalppm
df_nationalppm = (
    spark.readStream.format("kafka")
    .options(**kafka_options)
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schemas.nationalpage_national_ppm).alias("data"))
    .select("data.*")
)

df_nationalppm = (
    df_nationalppm.withColumn("Total", col("Total").cast("int"))
    .withColumn("OnTime", col("OnTime").cast("int"))
    .withColumn("Late", col("Late").cast("int"))
    .withColumn("CancelVeryLate", col("CancelVeryLate").cast("int"))
    .withColumn("PPM_text", col("PPM_text").cast("int"))
    .withColumn("RollingPPM_text", col("RollingPPM_text").cast("int"))
)
df_nationalppm.printSchema()
# Print the streaming DataFrame schema and data
query = df_nationalppm.writeStream.outputMode("append").format("console").start()

query.awaitTermination()


# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:2.8.1 streaming.py
