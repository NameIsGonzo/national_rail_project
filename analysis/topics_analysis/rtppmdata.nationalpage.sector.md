# UK Network Rail Feed - Analysis 

## Real Time Public Performance Measure (RTPPM)

## Sectors
![national_sectors_dashboard](../../images/national_sectors_dashboard.png)

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
    "timestamp": "1678141260000"
}
```

## Analysis

### 1. Calculate performance ratios for each sector
Compute performance ratios such as the on-time ratio, late ratio, and cancellation ratio for each sector to better understand the overall performance.

```python
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
```

### 2. Analyze PPM_rag, RollingPPM_rag, and RollingPPM_trendInd by sector:
Calculate the distribution of performance indicators for each sector to gain insights into their performance patterns.

```python
df_with_count = df.groupBy(
        col("sectorCode"),
        col("sectorName"),
        col("PPM_rag"),
        col("RollingPPM_rag"),
        col("RollingPPM_trendInd"),
    ).agg(count("*").alias("count_per_operator"))
```