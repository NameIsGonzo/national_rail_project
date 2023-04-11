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
    "timestamp": "1678141260000"
}
```

## Analysis

### 1. Compute FOC Performance Ratios
Calculate on-time and late performance ratios for the entire FOC to gain a better understanding of the overall performance.

```python
df_with_ratios = (
        df.withColumn("on_time_ratio", round((col("OnTime") / col("Total")) * 100, 2))
        .withColumn("late_ratio", round((col("Late") / col("Total")) * 100, 2))
        .select(
            "Total",
            "OnTime",
            "Late",
            "on_time_ratio",
            "late_ratio",
            "timestamp",
        )
    )
```