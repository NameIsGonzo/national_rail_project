from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round
import logging

logging.basicConfig(level= logging.INFO)


def performance_ratios(df: DataFrame, topic: str) -> DataFrame:
    """
    Compute performance ratios like [on-time, late, cancellation]
    to better understand of the overall performance
    """
    if "CancelVeryLate" in df.columns:
        if "sectorCode" in df.columns:
            df_with_ratios = df.select(
                "sectorCode", "sectorName", "Total", "OnTime", "Late", "CancelVeryLate"
            )
            df_with_ratios = (
                df_with_ratios.withColumn("sectorCode", col("sectorCode"))
                .withColumn("sectorName", col("sectorName"))
                .withColumn(
                    "on_time_ratio", round((col("OnTime") / col("Total")) * 100, 2)
                )
                .withColumn("late_ratio", round((col("Late") / col("Total")) * 100, 2))
                .withColumn(
                    "cancel_very_late_ratio",
                    round((col("CancelVeryLate") / col("Total")) * 100, 2),
                )
            )
            return df_with_ratios
        else:
            df_with_ratios = df.select(
                "topic", "Total", "OnTime", "Late", "CancelVeryLate"
            )
            df_with_ratios = (
                df_with_ratios.withColumn("topic", col("topic"))
                .withColumn(
                    "on_time_ratio", round((col("OnTime") / col("Total")) * 100, 2)
                )
                .withColumn("late_ratio", round((col("Late") / col("Total")) * 100, 2))
                .withColumn(
                    "cancel_very_late_ratio",
                    round((col("CancelVeryLate") / col("Total")) * 100, 2),
                )
            )
            return df_with_ratios
    else:
        df_with_ratios = df.select("topic", "Total", "OnTime", "Late")
        df_with_ratios = (df.withColumn('topic', col('topic'))
                           .withColumn("on_time_ratio", round((col("OnTime") / col("Total")) * 100, 2))
                           .withColumn("late_ratio", round((col("Late") / col("Total")) * 100, 2)))

        return df_with_ratios


def performance_count(df: DataFrame) -> DataFrame:
    """
    These categorical fields can be used to count the occurrences
    of each category over time or to understand the distribution of performance indicators.
    """
    return
