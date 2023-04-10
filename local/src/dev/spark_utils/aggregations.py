from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round, count, avg, window
import logging

logging.basicConfig(level=logging.INFO)


def performance_ratios(df: DataFrame, topic: str) -> DataFrame:
    """
    Compute performance ratios like [on-time, late, cancellation]
    to better understand of the overall performance
    """
    if topic == "rtppmdata.nationalpage.nationalppm":
        df_with_ratios = (
            df.withColumn(
                "on_time_ratio", round((col("OnTime") / col("Total")) * 100, 2)
            )
            .withColumn("late_ratio", round((col("Late") / col("Total")) * 100, 2))
            .withColumn(
                "cancel_very_late_ratio",
                round((col("CancelVeryLate") / col("Total")) * 100, 2),
            )
            .select(
                "Total",
                "OnTime",
                "Late",
                "CancelVeryLate",
                "on_time_ratio",
                "late_ratio",
                "cancel_very_late_ratio",
                "timestamp",
            )
        )
        return df_with_ratios
    elif topic in ["rtppmdata.nationalpage.sector", "rtppmdata.operatorpage.operators"]:
        df_with_ratios = (
            df.withColumn(
                "on_time_ratio", round((col("OnTime") / col("Total")) * 100, 2)
            )
            .withColumn("late_ratio", round((col("Late") / col("Total")) * 100, 2))
            .withColumn(
                "cancel_very_late_ratio",
                round((col("CancelVeryLate") / col("Total")) * 100, 2),
            )
            .select(
                "sectorCode",
                "sectorName",
                "Total",
                "OnTime",
                "Late",
                "CancelVeryLate",
                "on_time_ratio",
                "late_ratio",
                "cancel_very_late_ratio",
                "timestamp",
            )
        )
        return df_with_ratios
    elif topic == "rtppmdata.operatorpage.servicegroups":
        df_with_ratios = (
            df.withColumn(
                "on_time_ratio", round((col("OnTime") / col("Total")) * 100, 2)
            )
            .withColumn("late_ratio", round((col("Late") / col("Total")) * 100, 2))
            .withColumn(
                "cancel_very_late_ratio",
                round((col("CancelVeryLate") / col("Total")) * 100, 2),
            )
            .select(
                "name",
                "Total",
                "OnTime",
                "Late",
                "CancelVeryLate",
                "on_time_ratio",
                "late_ratio",
                "cancel_very_late_ratio",
                "timestamp",
            )
        )
        return df_with_ratios
    else:
        df_with_ratios = (
            df.withColumn(
                "on_time_ratio", round((col("OnTime") / col("Total")) * 100, 2)
            )
            .withColumn("late_ratio", round((col("Late") / col("Total")) * 100, 2))
            .select(
                "Total",
                "OnTime",
                "Late",
                "on_time_ratio",
                "late_ratio",
                "timestamp",
            )
        )
        return df_with_ratios


def performance_count(df: DataFrame, topic: str) -> DataFrame:
    """
    Examine PPM_rag, RollingPPM_rag to
    understand the distribution of performance indicators across the entire FOC.
    """
    if topic in ["rtppmdata.focpage.nationalppm", "rtppmdata.nationalpage.nationalppm"]:
        df_grouped_by_rag = (
            df.groupBy(col("PPM_rag"), col("RollingPPM_rag"), col("timestamp"))
            .agg(count("*").alias("count"))
            .select("PPM_rag", "RollingPPM_rag", "count", "timestamp")
        )
        return df_grouped_by_rag
    elif topic in [
        "rtppmdata.focpage.operator",
        "rtppmdata.nationalpage.operator",
        "rtppmdata.oocpage.operator",
    ]:
        df_grouped_by_rag = (
            df.groupBy(
                col("operatorCode"),
                col("name"),
                col("PPM_rag"),
                col("RollingPPM_rag"),
                col("timestamp"),
            )
            .agg(count("*").alias("count"))
            .select(
                "operatorCode",
                "name",
                "PPM_rag",
                "RollingPPM_rag",
                "count",
                "timestamp",
            )
        )
        return df_grouped_by_rag
    elif topic in ["rtppmdata.nationalpage.sector", "rtppmdata.operatorpage.operators"]:
        df_grouped_by_rag = (
            df.groupBy(
                col("sectorCode"),
                col("sectorName"),
                col("PPM_rag"),
                col("RollingPPM_rag"),
                col("timestamp"),
            )
            .agg(count("*").alias("count"))
            .select(
                "sectorCode",
                "sectorName",
                "PPM_rag",
                "RollingPPM_rag",
                "count",
                "timestamp",
            )
        )
        return df_grouped_by_rag
    else:
        df_grouped_by_rag = (
            df.groupBy(
                col("name"), col("PPM_rag"), col("RollingPPM_rag"), col("timestamp")
            )
            .agg(count("*").alias("count"))
            .select("name", "PPM_rag", "RollingPPM_rag", "count", "timestamp")
        )
        return df_grouped_by_rag