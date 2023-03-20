import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from utils import incoming_schemas as schemas


logging.basicConfig(level=logging.INFO)

schema_dict: dict = {
    "rtppmdata.nationalpage.nationalppm": schemas.nationalpage_national_ppm,
    "rtppmdata.nationalpage.sector": schemas.nationalpage_national_sector,
    "rtppmdata.nationalpage.operator": schemas.nationalpage_national_operator,
    "rtppmdata.oocpage.operator": schemas.oocpage_operator,
    "rtppmdata.focpage.nationalppm": schemas.focpage_nationalppm,
    "rtppmdata.focpage.operator": schemas.focpage_operator,
    "rtppmdata.operatorpage.operators": schemas.operatorpage_operators,
    "rtppmdata.operatorpage.servicegroups": schemas.operatorpage_service_operators,
}


def proccessData(topic: str, df) -> None:
    return logging.info(f"Processing df from topic: {topic}")


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
        """"""
        packages = [
            f"org.apache.spark:spark-sql-kafka-0-10_{self.scala_version}:{self.spark_version}",
            f"org.apache.kafka:kafka-clients:{self.kafka_client}",
        ]

        queries: list = []

        spark = (
            SparkSession.builder.appName("Spark Streaming Main Ingestion")
            .config("spark.jars.packages", ",".join(packages))
            .getOrCreate()
        )

        for topic in self.kafka_topics:
            kafka_options = {
                "kafka.bootstrap.servers": f"{self.kafka_host}:{self.kafka_port}",
                "subscribe": topic,
                "startingOffsets": "latest",
            }

            df = (
                spark.readStream.format("kafka")
                .options(**kafka_options)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), schema_dict.get(topic)).alias("data"))
                .select("data.*")
            )

            proccessData(topic, df)

            query = (
                df.writeStream.outputMode("append")
                .format("console")
                .start()
            )

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
