import logging
import os
from pyspark.sql import SparkSession
from spark_utils import save
from pyspark.sql.functions import from_json, col, lit
from spark_utils import incoming_schemas as schema
from spark_utils import casting_strings as cast
from spark_utils import aggregations as agg


logging.basicConfig(level=logging.INFO)
gcp_credentials: str = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

aggregations: dict = {
    "rtppmdata.nationalpage.nationalppm": agg.nationalpage_nationalppm,
    "rtppmdata.nationalpage.sector": agg.nationalpage_sector,
    "rtppmdata.nationalpage.operator": agg.nationalpage_operator,
    "rtppmdata.oocpage.operator": agg.
    oocpage_operator,
    "rtppmdata.focpage.nationalppm": agg.focpage_nationalppm,
    "rtppmdata.focpage.operator": agg.
    focpage_operator,
    "rtppmdata.operatorpage.operators": agg.
    operatorpage_operators,
    "rtppmdata.operatorpage.servicegroups": agg.
    operatorpage_servicegroups,
}


class SparkConsumer:
    def __init__(
        self,
        scala_version: str,
        spark_version: str,
        kafka_client: str,
        kafka_host: str,
        kafka_port: str,
        kafka_topics: list,
    ) -> None:
        self.scala_version = scala_version
        self.spark_version = spark_version
        self.kafka_client = kafka_client
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.kafka_topics = kafka_topics

    def process_topic(self, spark: SparkSession, topic: str, topic_name: str) -> None:
        """ Subscribes to topic and returns the streaming dataframe"""
        kafka_options = {
            "kafka.bootstrap.servers": f"{self.kafka_host}:{self.kafka_port}",
            "subscribe": topic,
            "startingOffsets": "latest",
            "failOnDataLoss": "false",
        }

        df = (
            spark.readStream.format("kafka")
            .options(**kafka_options)
            .load()
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), getattr(schema, topic_name)).alias("data"))
            .select("data.*")
            .selectExpr(getattr(cast, topic_name))
            .withColumn("topic", lit(topic))
        )

        return df

    def consumeTopics(self):
        """ """

        packages = [
            f"org.apache.spark:spark-sql-kafka-0-10_{self.scala_version}:{self.spark_version}",
            f"org.apache.kafka:kafka-clients:{self.kafka_client}",
            f"com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0",
        ]

        spark = (
            SparkSession.builder.appName("Spark Streaming Main Ingestion")
            .config("spark.executor.cores", "8")
            .config("spark.executor.memory", "12g")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", 24)
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.shuffle.service.enabled", "true")
            .config("spark.jars.packages", ",".join(packages))
            .config(
                "spark.jars",
                "/Users/gonzo/Desktop/RailScope/national_rail_project/local/src/hadoop/gcs-connector-hadoop3-latest.jar",
            )
            .config(
                "spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            )
            .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
            .config(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                gcp_credentials,
            )
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.streaming.fileSink.log.compactInterval", 100)
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
            .config("spark.cleaner.referenceTracking.blocking", "true")
            .config("spark.cleaner.referenceTracking.blockingFraction", "0.5")
            .getOrCreate()
        )

        queries: list = []

        for topic in self.kafka_topics:

            topic_name: str = topic.replace(".", "_")

            df = self.process_topic(spark, topic, topic_name)
        
            historical_query = save.save_historical_to_bq(df, topic_name)
            queries.append(historical_query)

            dfs = aggregations.get(topic)(df)
            
            for df, aggregation in dfs:
                save.save_to_realtime_data(df, topic_name, aggregation)


        for query in queries:
            query.awaitTermination()


if __name__ == "__main__":

    scala_version: str = "2.12"
    spark_version: str = "3.1.1"
    kafka_client: str = "2.8.1"
    kafka_host: str = "localhost"
    kafka_port: str = "9092"
    kafka_topics: list = [
        "rtppmdata.nationalpage.nationalppm",
        "rtppmdata.nationalpage.sector",
        "rtppmdata.nationalpage.operator",
        "rtppmdata.oocpage.operator",
        "rtppmdata.focpage.nationalppm",
        "rtppmdata.focpage.operator",
        "rtppmdata.operatorpage.operators",
        "rtppmdata.operatorpage.servicegroups",
    ]

    consumer = SparkConsumer(
        scala_version, spark_version, kafka_client, kafka_host, kafka_port, kafka_topics
    )

    consumer.consumeTopics()
