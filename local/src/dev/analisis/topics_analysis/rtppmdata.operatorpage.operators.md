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
    "total": "536",
    "onTime": "507",
    "late": "29",
    "cancelVeryLate": "24",
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
df_with_ratios = df.withColumn("on_time_ratio", col("onTime") / col("total")) \
    .withColumn("late_ratio", col("late") / col("total")) \
    .withColumn("cancel_very_late_ratio", col("cancelVeryLate") / col("total"))
```

### 2. Rolling averages by TOC
Calculate rolling averages for PPM_text and RollingPPM_text for each TOC to identify trends and understand how their performance is changing over time.

```python
window_spec = Window.partitionBy("sectorCode").orderBy(col("timestamp")).rowsBetween(-3, 0)  # Adjust the window size as needed

df_with_rolling_avg = df.withColumn("rolling_ppm_avg", avg(col("PPM_text")).over(window_spec)) \
    .withColumn("rolling_rolling_ppm_avg", avg(col("RollingPPM_text")).over(window_spec))
```

### 3. Analyze PPM_rag, RollingPPM_rag, and RollingPPM_trendInd by TOC
Calculate the distribution of performance indicators for each TOC to gain insights into their performance patterns.

```python
df_grouped_by_toc_and_rag = df.groupBy(col("sectorName"), col("PPM_rag"), col("RollingPPM_rag"), col("RollingPPM_trendInd")).agg(
    count("*").alias("count_per_toc_and_rag")
)
```

### 4. Identify top and bottom performing TOCs
Rank TOCs based on their PPM ratio and RollingPPM ratio to identify top and bottom performers. This will help stakeholders understand which TOCs are doing well and which ones might need improvement.

```python
window_spec = Window.orderBy(col("on_time_ratio").desc())

df_ranked_by_on_time_ratio = df_with_ratios.withColumn("on_time_rank", row_number().over(window_spec))
```