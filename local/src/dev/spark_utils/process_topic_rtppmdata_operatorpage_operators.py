from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql import DataFrame
from . import incoming_schemas as schema
from . import casting_strings as cast
from . import common_aggregations as agg


def process_topic(
    spark: SparkSession, topic: str, kafka_host: str, kafka_port: str
) -> None:
    """Main processing for the topic rtppmdata.operatorpage.operator"""

    kafka_options = {
        "kafka.bootstrap.servers": f"{kafka_host}:{kafka_port}",
        "subscribe": topic,
        "startingOffsets": "latest",
        "failOnDataLoss": "false",
    }

    df = (
        spark.readStream.format("kafka")
        .options(**kafka_options)
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema.operatorpage_operators).alias("data"))
        .select("data.*")
        .selectExpr(cast.operatorpage_operators)
        .withColumn("topic", lit(topic))
    )

    return df


def aggregations(df: DataFrame, topic: str) -> DataFrame:
    """
    Calls the proper aggregations
    """
    results: list = []

    df_with_ratios = agg.performance_ratios(df, topic)
    results.append(df_with_ratios)


    return results