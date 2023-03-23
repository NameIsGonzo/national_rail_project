from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from . import incoming_schemas as schema
from . import casting_strings as cast


def process_topic(
    spark: SparkSession, topic: str, kafka_host: str, kafka_port: str
) -> None:
    """Main processing for the topic rtppmdata.nationalpage.nationalppm"""

    kafka_options = {
        "kafka.bootstrap.servers": f"{kafka_host}:{kafka_port}",
        "subscribe": topic,
        "startingOffsets": "latest",
    }

    df = (
        spark.readStream.format("kafka")
        .options(**kafka_options)
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema.nationalpage_nationalppm).alias("data"))
        .select("data.*")
        .selectExpr(cast.nationalpage_nationalppm)
        .withColumn("topic", lit(topic))
    )

    query = (
        df.writeStream.outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("ignoreEmptyFiles", "true")
        .start()
    )

    return query, df
