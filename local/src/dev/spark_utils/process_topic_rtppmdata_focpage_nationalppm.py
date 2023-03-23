from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from . import incoming_schemas as schema
from . import casting_strings as cast
from . import save_to_gcs as save

def process_topic(
    spark: SparkSession, topic: str, kafka_host: str, kafka_port: str
) -> None:
    """Main processing for the topic rtppmdata.nationalpage.nationalppm"""

    kafka_options = {
        "kafka.bootstrap.servers": f"{kafka_host}:{kafka_port}",
        "subscribe": topic,
        "startingOffsets": "latest",
    }

    topic_name: str = topic.replace('.', '_')

    df = (
        spark.readStream.format("kafka")
        .options(**kafka_options)
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema.focpage_nationalppm).alias("data"))
        .select("data.*")
        .selectExpr(cast.focpage_nationalppm)
        .withColumn("topic", lit(topic))
    )

    query = (
        df.writeStream.outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
    )

    save.save_to_railscope_historical_data(df, topic_name)

    return query
