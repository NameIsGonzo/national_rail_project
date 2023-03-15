# UK Network Rail Feed - Analysis 

## Real Time Public Performance Measure (RTPPM)

## FOC Public Performance Measure

## Description
The analysis focuses on data ingested from the UK Network Rail Feed to assess the performance of Freight Operating Companies (FOC) within the UK national railway system, specifically examining their Public Performance Measure (PPM) results. By analyzing various performance metrics, such as total trains, on-time trains, late trains, PPM percentage, and Rolling PPM percentage, along with their corresponding performance indicators (RAG status and trend), the analysis aims to provide insights into the operational efficiency of freight transport services. These insights can help stakeholders identify areas that require improvement, monitor the impact of interventions, and facilitate informed decision-making to enhance the performance of freight operators across the rail network.

## Message schema
```json
{
    "Total": "416",
    "OnTime": "348",
    "Late": "68",
    "PPM_text": "83",
    "PPM_rag": "A",
    "PPM_ragDisplayFlag": "Y",
    "RollingPPM_text": "92",
    "RollingPPM_rag": "G",
    "RollingPPM_trendInd": "+",
    "timestamp": "14-03-2022T15:46:10"
}
```

## Analysis

### 1. Compute FOC Performance Ratios
Calculate on-time and late performance ratios for the entire FOC to gain a better understanding of the overall performance.

```python
df_with_ratios = df.withColumn("on_time_ratio", col("OnTime") / col("Total")) \
    .withColumn("late_ratio", col("Late") / col("Total"))
```

### 2. FOC Rolling Averages
Compute rolling averages for PPM_text and RollingPPM_text to identify trends and understand how performance is changing over time.

```python
window_spec = Window.orderBy(col("timestamp")).rowsBetween(-3, 0)  # Adjust the window size as needed

df_with_rolling_avg = df.withColumn("rolling_ppm_avg", avg(col("PPM_text")).over(window_spec)) \
    .withColumn("rolling_rolling_ppm_avg", avg(col("RollingPPM_text")).over(window_spec))
```

### 3. Analyze FOC Performance Indicators
Examine PPM_rag, PPM_ragDisplayFlag, RollingPPM_rag, and RollingPPM_trendInd to understand the distribution of performance indicators across the entire FOC.

```python
df_grouped_by_rag = df.groupBy(
    col("PPM_rag"), col("PPM_ragDisplayFlag"), col("RollingPPM_rag"), col("RollingPPM_trendInd")
).agg(
    count("*").alias("count_per_rag")
)
```

### 4. Compare FOC Performance with Other Sectors
Assess the overall FOC performance in relation to other sectors or operator groups to understand how the FOC is performing compared to other parts of the rail system.


