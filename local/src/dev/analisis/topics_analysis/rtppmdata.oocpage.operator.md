# UK Network Rail Feed - Analysis 

## Real Time Public Performance Measure (RTPPM)

## OOC Public Performance Measure

## Description
The analysis focuses on data ingested from the UK Network Rail Feed to evaluate the performance of Open Access Operators Companies (OOC) within the UK national railway system. By examining various performance metrics, such as the Public Performance Measure (PPM), Rolling PPM, and corresponding performance indicators (RAG status and trend), the analysis aims to provide insights into the operational efficiency of these non-franchised train operators. These insights can help stakeholders identify areas that require improvement, monitor the impact of interventions, and facilitate informed decision-making to enhance the performance of Open Access Operators across the rail network.


## Message schema
```json
{
    "Total": "125",
    "PPM_text": "29",
    "PPM_rag": "R",
    "RollingPPM_text": "38",
    "RollingPPM_rag": "R",
    "RollingPPM_trendInd": "+",
    "operatorCode": "86",
    "name": "Heathrow Express"
}
```

## Analysis

### 1. Analyze PPM_rag, RollingPPM_rag, and RollingPPM_trendInd by operator
Calculate the distribution of performance indicators for each OOC operator to gain insights into their performance patterns.

```python
df_grouped_by_operator_and_rag = df.groupBy(col("name"), col("PPM_rag"), col("RollingPPM_rag"), col("RollingPPM_trendInd")).agg(
    count("*").alias("count_per_operator_and_rag")
)
```

### 2. Identify top and bottom performing operators
Rank OOC operators based on their PPM and RollingPPM to identify top and bottom performers. This will help stakeholders understand which operators are doing well and which ones might need improvement.

```python
window_spec = Window.orderBy(col("PPM_text").desc())

df_ranked_by_ppm = df.withColumn("ppm_rank", row_number().over(window_spec))

window_spec = Window.orderBy(col("RollingPPM_text").desc())

df_ranked_by_rolling_ppm = df_ranked_by_ppm.withColumn("rolling_ppm_rank", row_number().over(window_spec))
```

### 3. Performance ratios
Compute performance ratios like on-time ratio and late ratio to better understand the overall performance of OOC operators.

```python
df_with_ratios = df.withColumn("on_time_ratio", col("OnTime") / col("Total")) \
    .withColumn("late_ratio", col("Late") / col("Total"))
```