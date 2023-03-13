from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    TimestampType,
    StructField,
)

# rtppmdata.nationalpage.nationalppm
nationalpage_national_ppm = StructType(
    [
        StructField("Total", StringType(), True),
        StructField("OnTime", StringType(), True),
        StructField("Late", StringType(), True),
        StructField("CancelVeryLate", StringType(), True),
        StructField("PPM_text", StringType(), True),
        StructField("PPM_rag", StringType(), True),
        StructField("PPM_ragDisplayFlag", StringType(), True),
        StructField("RollingPPM_text", StringType(), True),
        StructField("RollingPPM_rag", StringType(), True),
        StructField("RollingPPM_trendInd", StringType(), True),
    ]
)

# rtppmdata.nationalpage.sector
nationalpage_national_sector = StructType(
    [
        StructField("sectorCode", StringType(), True),
        StructField("sectorName", StringType(), True),
        StructField("Total", IntegerType(), True),
        StructField("OnTime", IntegerType(), True),
        StructField("Late", IntegerType(), True),
        StructField("CancelVeryLate", IntegerType(), True),
        StructField("PPM_text", IntegerType(), True),
        StructField("PPM_rag", StringType(), True),
        StructField("RollingPPM_text", StringType(), True),
        StructField("RollingPPM_rag", IntegerType(), True),
        StructField("RollingPPM_trendInd", StringType(), True),
    ]
)


# rtppmdata.nationalpage.operator
nationalpage_national_operator = StructType(
    [
        StructField("Total", IntegerType(), True),
        StructField("PPM_text", IntegerType(), True),
        StructField("PPM_rag", StringType(), True),
        StructField("RollingPPM_text", IntegerType(), True),
        StructField("RollingPPM_rag", StringType(), True),
        StructField("RollingPPM_trendInd", StringType(), True),
        StructField("operatorCode", StringType(), True),
        StructField("name", StringType(), True),
    ]
)


# rtppmdata.oocpage.operator
oocpage_operator = StructType(
    [
        StructField("Total", IntegerType()),
        StructField("PPM_text", IntegerType()),
        StructField("PPM_rag", StringType()),
        StructField("RollingPPM_text", IntegerType()),
        StructField("RollingPPM_rag", StringType()),
        StructField("RollingPPM_trendInd", StringType()),
        StructField("operatorCode", StringType()),
        StructField("name", StringType()),
    ]
)


# rtppmdata.focpage.nationalppm
focpage_nationalppm = StructType(
    [
        StructField("Total", IntegerType(), True),
        StructField("OnTime", IntegerType(), True),
        StructField("Late", IntegerType(), True),
        StructField("PPM_text", IntegerType(), True),
        StructField("PPM_rag", StringType(), True),
        StructField("PPM_ragDisplayFlag", StringType(), True),
        StructField("RollingPPM_text", IntegerType(), True),
        StructField("RollingPPM_rag", StringType(), True),
        StructField("RollingPPM_trendInd", StringType(), True),
    ]
)


# rtppmdata.focpage.operator
focpage_operator = StructType(
    [
        StructField("Total", IntegerType()),
        StructField("PPM_text", IntegerType()),
        StructField("PPM_rag", StringType()),
        StructField("RollingPPM_text", IntegerType()),
        StructField("RollingPPM_rag", StringType()),
        StructField("RollingPPM_trendInd", StringType()),
        StructField("operatorCode", StringType()),
        StructField("name", StringType()),
    ]
)


# rtppmdata.operatorpage.operators
operatorpage_operators = StructType(
    [
        StructField("sectorCode", StringType()),
        StructField("sectorName", StringType()),
        StructField("total", IntegerType()),
        StructField("onTime", IntegerType()),
        StructField("late", IntegerType()),
        StructField("cancelVeryLate", IntegerType()),
        StructField("PPM_text", IntegerType()),
        StructField("PPM_rag", StringType()),
        StructField("RollingPPM_text", IntegerType()),
        StructField("RollingPPM_rag", StringType()),
        StructField("RollingPPM_trendInd", StringType()),
    ]
)


# rtppmdata.operatorpage.servicegroups
operatorpage_service_operators = StructType(
    [
        StructField("name", StringType(), True),
        StructField("Total", IntegerType(), True),
        StructField("OnTime", IntegerType(), True),
        StructField("Late", IntegerType(), True),
        StructField("CancelVeryLate", IntegerType(), True),
        StructField("PPM_text", IntegerType(), True),
        StructField("PPM_rag", StringType(), True),
        StructField("RollingPPM_text", IntegerType(), True),
        StructField("RollingPPM_rag", StringType(), True),
        StructField("RollingPPM_trendInd", StringType(), True),
    ]
)
