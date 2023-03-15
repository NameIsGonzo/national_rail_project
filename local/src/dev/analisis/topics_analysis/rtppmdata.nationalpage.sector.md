# UK Network Rail Feed - Analysis 

## Real Time Public Performance Measure (RTPPM)

## Sectors

## Description
The analysis leverages data ingested from the UK Network Rail Feed to evaluate the performance of each sector within the rail network. By examining various performance metrics, such as the Public Performance Measure (PPM), Rolling PPM, and corresponding performance indicators (RAG status and trend), the analysis provides insights into the performance of the rail system across different geographical regions. This information can be valuable for stakeholders to identify areas that may require improvement or further investigation, as well as to track the impact of interventions on the rail system's performance within each sector.

## Message schema
```json
{
    "sectorCode": "LSE",
    "sectorName": "London and South East",
    "Total": "8881",
    "OnTime": "7898",
    "Late": "983",
    "CancelVeryLate": "322",
    "PPM_text": "88",
    "PPM_rag": "R",
    "RollingPPM_text": "84",
    "RollingPPM_rag": "R",
    "RollingPPM_trendInd": "-",
    "timestamp": "14-03-2022T15:46:10"
}
```

## Analysis

### 1. Calculate performance ratios for each sector
Compute performance ratios such as the on-time ratio, late ratio, and cancellation ratio for each sector to better understand the overall performance.

```python
df_with_ratios = df.withColumn("on_time_ratio", col("OnTime") / col("Total")) \
    .withColumn("late_ratio", col("Late") / col("Total")) \
    .withColumn("cancel_very_late_ratio", col("CancelVeryLate") / col("Total"))
```

### 2. Analyze PPM_rag, RollingPPM_rag, and RollingPPM_trendInd by sector:
Calculate the distribution of performance indicators for each sector to gain insights into their performance patterns.

```python
df_grouped_by_sector_and_rag = df.groupBy(col("sectorName"), col("PPM_rag"), col("RollingPPM_rag"), col("RollingPPM_trendInd")).agg(
    count("*").alias("count_per_sector_and_rag")
)
```

### 3. Calculate rolling averages for PPM_text and RollingPPM_text by sector:
Compute rolling averages for PPM_text and RollingPPM_text values to identify trends and understand how performance is changing over time in each sector.

```python
window_spec = Window.partitionBy("sectorName").orderBy(col("timestamp")).rowsBetween(-3, 0)  # Adjust the window size as needed

df_with_rolling_avg = df.withColumn("rolling_ppm_avg", avg(col("PPM_text")).over(window_spec)) \
    .withColumn("rolling_rolling_ppm_avg", avg(col("RollingPPM_text")).over(window_spec))
```

### 4. Identify top and bottom performing sectors:
Rank sectors based on their on-time ratio to identify top and bottom performers. This will help stakeholders understand which regions are doing well and which ones might need improvement.

```python
window_spec = Window.orderBy(col("on_time_ratio").desc())

df_ranked_by_on_time_ratio = df_with_ratios.withColumn("on_time_rank", row_number().over(window_spec))
```