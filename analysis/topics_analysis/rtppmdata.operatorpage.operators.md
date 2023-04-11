# UK Network Rail Feed - Analysis 

## Real Time Public Performance Measure (RTPPM)

## TOCs Public Performance Measure

## Description
This analysis focuses on the Public Performance Measure (PPM) data of Train Operating Companies (TOCs) within the UK National Railway System, as provided by the UK Network Rail Feed. The dataset includes various performance metrics such as total trains, on-time trains, late trains, cancellations, and rolling PPM values. By evaluating these metrics and trend indicators, the analysis aims to provide insights into the overall performance of each TOC.

These insights can help stakeholders identify performance patterns, monitor the effectiveness of interventions, and make data-driven decisions to improve the operational efficiency of train services across different TOCs in the rail network.

## Message schema

```json
{
    "sectorCode": "33",
    "sectorName": "Elizabeth line",
    "Total": "536",
    "OnTime": "507",
    "Late": "29",
    "CancelVeryLate": "24",
    "PPM_text": "94",
    "PPM_rag": "G",
    "RollingPPM_text": "88",
    "RollingPPM_rag": "R",
    "RollingPPM_trendInd": "-",
    "timestamp": "1678141260000"
}
```

## Analysis

### 1. Performance ratios by TOC
Calculate performance ratios such as on-time ratio, late ratio, and cancellation ratio for each TOC to better understand their overall performance.

```python
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
```

### 2. Analyze PPM_rag, RollingPPM_rag, and RollingPPM_trendInd by TOC
Calculate the distribution of performance indicators for each TOC to gain insights into their performance patterns.

```python
df_with_count = df.groupBy(
        col("SectorName"),
        col("PPM_rag"),
        col("RollingPPM_rag"),
        col("RollingPPM_trendInd"),
    ).agg(count("*").alias("count_per_operator"))
```