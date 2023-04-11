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
df_with_avg_perf = (
        df.groupBy("sectorName")
        .agg(
            avg("PPM_text").alias("avg_ppm"),
            avg("RollingPPM_text").alias("avg_rolling_ppm"),
        )
        .select("name", "avg_ppm", "avg_rolling_ppm", "timestamp")
    )
```
