from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round, count, avg, window
import logging

logging.basicConfig(level=logging.INFO)


def nationalpage_nationalppm(df: DataFrame) -> list[(DataFrame, str)]:
    """
    Returns a list with all the aggregations made to the DataFrame
    """
    dataframes: list = []

    df_with_ratios = (
        df.withColumn("on_time_ratio", round((col("OnTime") / col("Total")) * 100, 2))
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
    dataframes.append((df_with_ratios, "performance_ratios"))

    return dataframes


def nationalpage_operator(df: DataFrame) -> list[(DataFrame, str)]:
    """
    Returns a list with all the aggregations made to the DataFrame
    """
    dataframes: list = []

    # Count dataframe
    df_with_count = df.groupBy(
        col("operatorCode"),
        col("name"),
        col("PPM_rag"),
        col("RollingPPM_rag"),
        col("RollingPPM_trendInd"),
    ).agg(count("*").alias("count_per_operator"))
    dataframes.append((df_with_count, "performance_count"))

    return dataframes


def nationalpage_sector(df: DataFrame) -> list[(DataFrame, str)]:
    """
    Returns a list with all the aggregations made to the DataFrame
    """
    dataframes: list = []

    # Ratios dataframe
    df_with_ratios = (
        df.withColumn("on_time_ratio", round((col("OnTime") / col("Total")) * 100, 2))
        .withColumn("late_ratio", round((col("Late") / col("Total")) * 100, 2))
        .withColumn(
            "cancel_very_late_ratio",
            round((col("CancelVeryLate") / col("Total")) * 100, 2),
        )
        .select(
            "SectorCode",
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
    dataframes.append((df_with_ratios, "performance_ratios"))

    # Count dataframe
    df_with_count = df.groupBy(
        col("sectorCode"),
        col("sectorName"),
        col("PPM_rag"),
        col("RollingPPM_rag"),
        col("RollingPPM_trendInd"),
    ).agg(count("*").alias("count_per_operator"))

    dataframes.append((df_with_count, "performance_count"))

    return dataframes


def oocpage_operator(df: DataFrame) -> list[(DataFrame, str)]:
    """
    Returns a list with all the aggregations made to the DataFrame
    """
    dataframes: list = []

    # Count dataframe
    df_with_count = df.groupBy(
        col("name"),
        col("PPM_rag"),
        col("RollingPPM_rag"),
        col("RollingPPM_trendInd"),
    ).agg(count("*").alias("count_per_operator"))
    dataframes.append((df_with_count, "performance_count"))

    return dataframes


def focpage_nationalppm(df: DataFrame) -> list[(DataFrame, str)]:
    """
    Returns a list with all the aggregations made to the DataFrame
    """
    dataframes: list = []

    df_with_ratios = (
        df.withColumn("on_time_ratio", round((col("OnTime") / col("Total")) * 100, 2))
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
    dataframes.append((df_with_ratios, "performance_ratios"))

    return dataframes


def focpage_operator(df: DataFrame) -> list[(DataFrame, str)]:
    """
    Returns a list with all the aggregations made to the DataFrame
    """
    dataframes: list = []

    df_with_count = df.groupBy(
        col("operatorCode"),
        col("name"),
        col("PPM_rag"),
        col("RollingPPM_rag"),
        col("RollingPPM_trendInd"),
    ).agg(count("*").alias("count_per_operator"))
    dataframes.append((df_with_count, "performance_count"))

    return dataframes


def operatorpage_operators(df: DataFrame) -> list[(DataFrame, str)]:
    """
    Returns a list with all the aggregations made to the DataFrame
    """
    dataframes: list = []

    df_with_ratios = (
        df.withColumn("on_time_ratio", round((col("OnTime") / col("Total")) * 100, 2))
        .withColumn("late_ratio", round((col("Late") / col("Total")) * 100, 2))
        .withColumn(
            "cancel_very_late_ratio",
            round((col("CancelVeryLate") / col("Total")) * 100, 2),
        )
        .select(
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
    dataframes.append((df_with_ratios, "performance_ratios"))

    df_with_count = df.groupBy(
        col("SectorName"),
        col("PPM_rag"),
        col("RollingPPM_rag"),
        col("RollingPPM_trendInd"),
    ).agg(count("*").alias("count_per_operator"))
    dataframes.append((df_with_count, "performance_count"))

    return dataframes


def operatorpage_servicegroups(df: DataFrame) -> list[(DataFrame, str)]:
    """
    Returns a list with all the aggregations made to the DataFrame
    """
    dataframes: list = []

    df_with_avg_perf = (
        df.groupBy("sectorName")
        .agg(
            avg("PPM_text").alias("avg_ppm"),
            avg("RollingPPM_text").alias("avg_rolling_ppm"),
        )
        .select("name", "avg_ppm", "avg_rolling_ppm", "timestamp")
    )
    dataframes.append((df_with_avg_perf, "performance_avg"))
