from pyspark.sql.types import (
    StructType,
    StringType,
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
        StructField("timestamp", StringType(), True)
    ]
)

# rtppmdata.nationalpage.sector
nationalpage_national_sector = StructType(
    [
        StructField("sectorCode", StringType(), True),
        StructField("sectorName", StringType(), True),
        StructField("Total", StringType(), True),
        StructField("OnTime", StringType(), True),
        StructField("Late", StringType(), True),
        StructField("CancelVeryLate", StringType(), True),
        StructField("PPM_text", StringType(), True),
        StructField("PPM_rag", StringType(), True),
        StructField("RollingPPM_text", StringType(), True),
        StructField("RollingPPM_rag", StringType(), True),
        StructField("RollingPPM_trendInd", StringType(), True),
        StructField("timestamp", StringType(), True)
    ]
)


# rtppmdata.nationalpage.operator
nationalpage_national_operator = StructType(
    [
        StructField("Total", StringType(), True),
        StructField("PPM_text", StringType(), True),
        StructField("PPM_rag", StringType(), True),
        StructField("RollingPPM_text", StringType(), True),
        StructField("RollingPPM_rag", StringType(), True),
        StructField("RollingPPM_trendInd", StringType(), True),
        StructField("operatorCode", StringType(), True),
        StructField("name", StringType(), True),
        StructField("timestamp", StringType(), True)
    ]
)


# rtppmdata.oocpage.operator
oocpage_operator = StructType(
    [
        StructField("Total", StringType()),
        StructField("PPM_text", StringType()),
        StructField("PPM_rag", StringType()),
        StructField("RollingPPM_text", StringType()),
        StructField("RollingPPM_rag", StringType()),
        StructField("RollingPPM_trendInd", StringType()),
        StructField("operatorCode", StringType()),
        StructField("name", StringType()),
        StructField("timestamp", StringType(), True)
    ]
)


# rtppmdata.focpage.nationalppm
focpage_nationalppm = StructType(
    [
        StructField("Total", StringType(), True),
        StructField("OnTime", StringType(), True),
        StructField("Late", StringType(), True),
        StructField("PPM_text", StringType(), True),
        StructField("PPM_rag", StringType(), True),
        StructField("PPM_ragDisplayFlag", StringType(), True),
        StructField("RollingPPM_text", StringType(), True),
        StructField("RollingPPM_rag", StringType(), True),
        StructField("RollingPPM_trendInd", StringType(), True),
        StructField("timestamp", StringType(), True)
    ]
)


# rtppmdata.focpage.operator
focpage_operator = StructType(
    [
        StructField("Total", StringType()),
        StructField("PPM_text", StringType()),
        StructField("PPM_rag", StringType()),
        StructField("RollingPPM_text", StringType()),
        StructField("RollingPPM_rag", StringType()),
        StructField("RollingPPM_trendInd", StringType()),
        StructField("operatorCode", StringType()),
        StructField("name", StringType()),
        StructField("timestamp", StringType(), True)
    ]
)


# rtppmdata.operatorpage.operators
operatorpage_operators = StructType(
    [
        StructField("sectorCode", StringType()),
        StructField("sectorName", StringType()),
        StructField("total", StringType()),
        StructField("onTime", StringType()),
        StructField("late", StringType()),
        StructField("cancelVeryLate", StringType()),
        StructField("PPM_text", StringType()),
        StructField("PPM_rag", StringType()),
        StructField("RollingPPM_text", StringType()),
        StructField("RollingPPM_rag", StringType()),
        StructField("RollingPPM_trendInd", StringType()),
        StructField("timestamp", StringType(), True)
    ]
)


# rtppmdata.operatorpage.servicegroups
operatorpage_service_operators = StructType(
    [
        StructField("name", StringType(), True),
        StructField("Total", StringType(), True),
        StructField("OnTime", StringType(), True),
        StructField("Late", StringType(), True),
        StructField("CancelVeryLate", StringType(), True),
        StructField("PPM_text", StringType(), True),
        StructField("PPM_rag", StringType(), True),
        StructField("RollingPPM_text", StringType(), True),
        StructField("RollingPPM_rag", StringType(), True),
        StructField("RollingPPM_trendInd", StringType(), True),
        StructField("timestamp", StringType(), True)
    ]
)
