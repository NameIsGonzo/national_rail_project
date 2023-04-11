# UK Network Rail Feed - Analysis 

## Real Time Public Performance Measure (RTPPM)

## FOC Operators

## Description
The analysis focuses on data ingested from the UK Network Rail Feed to assess the performance of Freight Operating Companies (FOC Operators) within the UK national railway system. By examining various performance metrics, such as the Public Performance Measure (PPM), Rolling PPM, and corresponding performance indicators (RAG status and trend), the analysis aims to provide insights into the operational efficiency of freight transport services. These insights can help stakeholders identify areas that require improvement, monitor the impact of interventions, and facilitate informed decision-making to enhance the performance of freight operators across the rail network.

## Message schema
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
    "timestamp": "1678141260000"
}
```

## Analysis

### 1. Analyze PPM_rag, RollingPPM_rag, and RollingPPM_trendInd by FOC operator
Calculate the distribution of performance indicators (RAG status and trend) for each FOC operator to gain insights into their performance patterns.

```python
 df_with_count = df.groupBy(
        col("operatorCode"),
        col("name"),
        col("PPM_rag"),
        col("RollingPPM_rag"),
        col("RollingPPM_trendInd"),
    ).agg(count("*").alias("count_per_operator"))
```