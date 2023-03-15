# UK Network Rail Feed - Analysis 

## Real Time Public Performance Measure (RTPPM)

## Operators

### Description
The analysis leverages data ingested from the UK Network Rail Feed to evaluate the performance of individual train operating companies. By examining metrics such as the Public Performance Measure (PPM), Rolling PPM, and corresponding performance indicators (RAG status and trend), the analysis provides insights into the relative performance of each operator. This information can be used by stakeholders to identify top-performing operators, as well as those that may require improvement or further investigation.

### Message schema
```json
{
    "Total": "125",
    "PPM_text": "74",
    "PPM_rag": "R",
    "RollingPPM_text": "94",
    "RollingPPM_rag": "G",
    "RollingPPM_trendInd": "+",
    "operatorCode": "61",
    "name": "London North Eastern Railway",
    "timestamp": "14-03-2022T15:46:10"
}
```

## Analysis

### 1. Analyze PPM_rag, RollingPPM_rag, and RollingPPM_trendInd by operator

Calculate the distribution of performance indicators for each operator to gain insights into their performance patterns.

```python
df_grouped_by_operator_and_rag = df.groupBy(col("name"), col("PPM_rag"), col("RollingPPM_rag"), col("RollingPPM_trendInd")).agg(
    count("*").alias("count_per_operator_and_rag")
)
```

### 2. Identify top and bottom performing operators
Rank operators based on their PPM ratio and RollingPPM ratio to identify top and bottom performers. This will help stakeholders understand which operators are doing well and which ones might need improvement.

```python
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

window_spec = Window.orderBy(col("avg_ppm_ratio").desc())

df_ranked_by_ppm = df_grouped_by_operator.withColumn("ppm_rank", row_number().over(window_spec))

window_spec = Window.orderBy(col("avg_rolling_ppm_ratio").desc())

df_ranked_by_ppm_and_rolling_ppm = df_ranked_by_ppm.withColumn("rolling_ppm_rank", row_number().over(window_spec))
```

