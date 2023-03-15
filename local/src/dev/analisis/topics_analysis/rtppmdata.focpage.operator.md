# UK Network Rail Feed - Analysis 

## Real Time Public Performance Measure (RTPPM)

## FOC Operators

## Description
The analysis focuses on data ingested from the UK Network Rail Feed to assess the performance of Freight Operating Companies (FOC Operators) within the UK national railway system. By examining various performance metrics, such as the Public Performance Measure (PPM), Rolling PPM, and corresponding performance indicators (RAG status and trend), the analysis aims to provide insights into the operational efficiency of freight transport services. These insights can help stakeholders identify areas that require improvement, monitor the impact of interventions, and facilitate informed decision-making to enhance the performance of freight operators across the rail network.

```json
{
    "Total": "69",
    "PPM_text": "81",
    "PPM_rag": "A",
    "RollingPPM_text": "100",
    "RollingPPM_rag": "G",
    "RollingPPM_trendInd": "+",
    "operatorCode": "FHH",
    "name": "Freightliner Heavy Haul",
    "timestamp": "14-03-2022T15:46:10"
}
```

## Analysis

### 1. Calculate performance ratios for each FOC operator
Compute the PPM ratio (PPM_text / Total) and Rolling PPM ratio (RollingPPM_text / Total) for each operator to better understand their performance in the context of the total number of trains they operate.

```python
df_with_ratios = df.withColumn("ppm_ratio", col("PPM_text") / col("Total")) \
    .withColumn("rolling_ppm_ratio", col("RollingPPM_text") / col("Total"))
```

### 2. Analyze PPM_rag, RollingPPM_rag, and RollingPPM_trendInd by FOC operator
Calculate the distribution of performance indicators (RAG status and trend) for each FOC operator to gain insights into their performance patterns.

```python
df_grouped_by_operator_and_rag = df.groupBy(col("name"), col("PPM_rag"), col("RollingPPM_rag"), col("RollingPPM_trendInd")).agg(
    count("*").alias("count_per_operator_and_rag")
)
```

### 3. Identify top and bottom performing FOC operators:
Rank FOC operators based on their PPM ratio and Rolling PPM ratio to identify top and bottom performers. This will help stakeholders understand which operators are doing well and which ones might need improvement.

```python
window_spec = Window.orderBy(col("ppm_ratio").desc())

df_ranked_by_ppm = df_with_ratios.withColumn("ppm_rank", row_number().over(window_spec))

window_spec = Window.orderBy(col("rolling_ppm_ratio").desc())

df_ranked_by_ppm_and_rolling_ppm = df_ranked_by_ppm.withColumn("rolling_ppm_rank", row_number().over(window_spec))
```

### 4. Calculate rolling averages for PPM_text and RollingPPM_text by FOC operator:
Compute rolling averages for PPM_text and RollingPPM_text to identify trends and understand how FOC operator performance is changing over time.

```python
window_spec = Window.partitionBy("name").orderBy(col("timestamp")).rowsBetween(-3, 0)  # Adjust the window size as needed

df_with_rolling_avg = df.withColumn("rolling_ppm_avg", avg(col("PPM_text")).over(window_spec)) \
    .withColumn("rolling_rolling_ppm_avg", avg(col("RollingPPM_text")).over(window_spec))
```