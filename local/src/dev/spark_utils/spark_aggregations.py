from pyspark.sql import DataFrame
from pyspark.sql.functions import col

performance_topics = [
    "rtppmdata.focpage.nationalppm",
    "rtppmdata.nationalpage.nationalppm",
    "rtppmdata.nationalpage.sector",
    "rtppmdata.operatorpage.operators",
    "rtppmdata.operatorpage.servicegroups",
]


def dataframe_hub(topic: str, df: DataFrame) -> None:
    """Calls the corresponding functions for each topic"""

    if topic in performance_topics:
        performance_df = performance_ratio(df)

        query = (
            performance_df.writeStream
            .format("console")
            .outputMode("append") 
            .option("truncate", False)
            .start()
        )
        return query
    else:
        pass
    


def performance_ratio(df: DataFrame) -> DataFrame:
    """
    Compute performance ratios like [on-time, late, cancellation]
    to better understand of the overall performance
    """
    if "CancelVeryLate" in df.columns:
        df_with_ratios = df.select("topic", "Total", "OnTime", "Late", "CancelVeryLate")
        df_with_ratios = (
            df_with_ratios.withColumn("topic", col("topic"))
            .withColumn("on_time_ratio", col("OnTime") / col("Total"))
            .withColumn("late_ratio", col("Late") / col("Total"))
            .withColumn("cancel_very_late_ratio", col("CancelVeryLate") / col("Total"))
        )
    else:
        df_with_ratios = df.select("topic", "Total", "OnTime", "Late")
        df_with_ratios = df.withColumn(
            "on_time_ratio", col("OnTime") / col("Total")
        ).withColumn("late_ratio", col("Late") / col("Total"))

    return df_with_ratios


def performance_count(df: DataFrame) -> DataFrame:
    """
    These categorical fields can be used to count the occurrences
    of each category over time or to understand the distribution of performance indicators.
    """
    return
