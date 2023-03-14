# UK Network Rail Feed - Analisis 
## Real Time Public Performance Measure (RTPPM)

## National PPM

### Description
The analysis leverages data ingested from the UK Network Rail Feed to calculate various performance metrics and ratios, identify trends, and understand the distribution of performance indicators. The project also includes the creation of a real-time dashboard to visualize the performance data.


### Message schema
```json
{
    "Total": "15056",
    "OnTime": "13375",
    "Late": "1681",
    "CancelVeryLate": "578",
    "PPM_text": "88",
    "PPM_rag": "R",
    "PPM_ragDisplayFlag": "Y",
    "RollingPPM_text": "87",
    "RollingPPM_rag": "R",
    "RollingPPM_trendInd": "-",
    "timestamp": "14-03-2022T15:46:10"
}
```

## Analisis
### 1. Performance ratios

Compute performance ratios like on-time ratio, late ratio, 
and cancellation ratio to better understand the overall performance.

```python
df_with_ratios = df.withColumn("on_time_ratio", col("OnTime") / col("Total")) \
    .withColumn("late_ratio", col("Late") / col("Total")) \
    .withColumn("cancel_very_late_ratio", col("CancelVeryLate") / col("Total"))
```

### 2. Rolling averages
Calculate rolling averages for PPM_text and RollingPPM_text to identify trends
and understand how performance is changing over time.

```python
window_spec = Window.orderBy(col("timestamp")).rowsBetween(-3, 0)  # Adjust the window size as needed

df_with_rolling_avg = df.withColumn("rolling_ppm_avg", avg(col("PPM_text")).over(window_spec)) \
    .withColumn("rolling_rolling_ppm_avg", avg(col("RollingPPM_text")).over(window_spec))
```

### 3. Analyze PPM_rag, PPM_ragDisplayFlag, RollingPPM_rag, and RollingPPM_trendInd
These categorical fields can be used to count the occurrences of each category 
over time or to understand the distribution of performance indicators.

```python
df_grouped_by_rag = df.groupBy(
    col("PPM_rag")
).agg(
    count("*").alias("count_per_rag")
```