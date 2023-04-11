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
    "timestamp": "1678141260000"
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
