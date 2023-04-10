from pyspark.sql.types import (
    StructType,
    StringType,
    StructField,
)

# rtppmdata.nationalpage.nationalppm
rtppmdata_nationalpage_nationalppm = StructType(
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
rtppmdata_nationalpage_sector = StructType(
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
rtppmdata_nationalpage_operator = StructType(
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
rtppmdata_oocpage_operator = StructType(
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
rtppmdata_focpage_nationalppm = StructType(
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
rtppmdata_focpage_operator = StructType(
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
rtppmdata_operatorpage_operators = StructType(
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
rtppmdata_operatorpage_servicegroups = StructType(
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
