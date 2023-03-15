# UK Network Rail Feed - Analysis 

## Real Time Public Performance Measure (RTPPM)

## TOCs Service Groups - Public Performance Measure

## Description
The analysis delves into data from the UK Network Rail Feed to evaluate the performance of each Train Operating Company's (TOC) service groups. By breaking down the performance metrics, such as on-time arrivals, late arrivals, cancellations, and the Public Performance Measure (PPM), for each service group within a TOC, the analysis aims to provide granular insights into the operational efficiency of the services they provide. Understanding the performance of individual service groups can assist stakeholders in pinpointing specific areas that need improvement and implementing targeted interventions to enhance overall rail network performance.

## Message schema

```json
{
    "name": "Wirral Lines",
    "Total": "184",
    "OnTime": "181",
    "Late": "3",
    "CancelVeryLate": "0",
    "PPM_text": "98",
    "PPM_rag": "G",
    "RollingPPM_text": "100",
    "RollingPPM_rag": "G",
    "RollingPPM_trendInd": "+",
    "timestamp": "1678141260000"
}
```

## Analysis

### 1. Performance overview per service group 
Calculate the average PPM and Rolling PPM for each service group to get a general understanding of their performance.

```python
df_service_groups_agg = df.groupBy("name").agg(
    avg("PPM_text").alias("avg_ppm"),
    avg("RollingPPM_text").alias("avg_rolling_ppm")
)
```

### 2. Top and bottom performing service groups
Rank the service groups based on their average PPM and Rolling PPM to identify the top and bottom performers. This can help stakeholders understand which service groups are performing well and which ones might need improvement.

```python
window_spec_ppm = Window.orderBy(col("avg_ppm").desc())
window_spec_rolling_ppm = Window.orderBy(col("avg_rolling_ppm").desc())

df_ranked_service_groups = df_service_groups_agg.withColumn("ppm_rank", row_number().over(window_spec_ppm)) \
    .withColumn("rolling_ppm_rank", row_number().over(window_spec_rolling_ppm))
```

### 3. Performance trends analysis
Analyze the Rolling PPM trend indicator distribution for each service group. This can provide insights into the general direction of each service group's performance, helping stakeholders identify which groups have improving or deteriorating performance.

```python
df_trends = df.groupBy("name", "RollingPPM_trendInd").agg(
    count("*").alias("trend_count")
)
```

### 4. Performance distribution by RAG status
Calculate the distribution of PPM RAG and Rolling PPM RAG status for each service group. This can help stakeholders gain insights into the performance patterns of each service group.

```python
df_grouped_by_rag = df.groupBy("name", "PPM_rag", "RollingPPM_rag").agg(
    count("*").alias("count_per_service_group_and_rag")
)
```