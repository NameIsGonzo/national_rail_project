import logging
import importlib
from pyspark.sql import SparkSession


logging.basicConfig(level=logging.INFO)


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

    def consumeTopics(self):
        """ """

        packages = [
            f"org.apache.spark:spark-sql-kafka-0-10_{self.scala_version}:{self.spark_version}",
            f"org.apache.kafka:kafka-clients:{self.kafka_client}",
        ]

        queries: list = []

        spark = (
            SparkSession.builder.appName("Spark Streaming Main Ingestion")
            .config("spark.executor.cores", "8")
            .config("spark.executor.memory", "8g")
            .config("spark.jars.packages", ",".join(packages))
            .config("spark.sql.shuffle.partitions", 16)
            .getOrCreate()
        )

        for topic in self.kafka_topics:

            topic_name = topic.replace(".", "_")

            process_module = importlib.import_module(
                name=f"spark_utils.process_topic_{topic_name}"
            )
            process_func = process_module.process_topic

            query = process_func(spark, topic, self.kafka_host, self.kafka_port)

            queries.append(query)

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
