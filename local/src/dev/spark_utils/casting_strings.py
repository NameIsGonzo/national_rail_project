nationalpage_nationalppm: list = [
    "CAST(Total AS INT) AS Total",
    "CAST(OnTime AS INT) AS OnTime",
    "CAST(Late AS INT) AS Late",
    "CAST(CancelVeryLate AS INT) AS CancelVeryLate",
    "CAST(PPM_text AS INT) AS PPM_text",
    "CAST(PPM_rag AS STRING) AS PPM_rag",
    "CAST(PPM_ragDisplayFlag AS STRING) AS PPM_ragDisplayFlag",
    "CAST(RollingPPM_text AS INT) AS RollingPPM_text",
    "CAST(RollingPPM_rag AS STRING) AS RollingPPM_rag",
    "CAST(RollingPPM_trendInd AS STRING) AS RollingPPM_trendInd",
    "CAST(timestamp AS TIMESTAMP) AS timestamp",
]

nationalpage_nationalsector: list = [
    "CAST(sectorCode AS STRING) AS sectorCode",
    "CAST(sectorName AS STRING) AS sectorName",
    "CAST(Total AS INT) AS Total",
    "CAST(OnTime AS INT) AS OnTime",
    "CAST(Late AS INT) AS Late",
    "CAST(CancelVeryLate AS INT) AS CancelVeryLate",
    "CAST(PPM_text AS INT) AS PPM_text",
    "CAST(PPM_rag AS STRING) AS PPM_rag",
    "CAST(RollingPPM_text AS INT) AS RollingPPM_text",
    "CAST(RollingPPM_rag AS STRING) AS RollingPPM_rag",
    "CAST(RollingPPM_trendInd AS STRING) AS RollingPPM_trendInd",
    "CAST(timestamp AS TIMESTAMP) AS timestamp",
]

nationalpage_nationaloperator: list = [
    "CAST(Total AS INT) AS Total",
    "CAST(PPM_text AS INT) AS PPM_text",
    "CAST(PPM_rag AS STRING) AS PPM_rag",
    "CAST(RollingPPM_text AS INT) AS RollingPPM_text",
    "CAST(RollingPPM_rag AS STRING) AS RollingPPM_rag",
    "CAST(RollingPPM_trendInd AS STRING) AS RollingPPM_trendInd",
    "CAST(operatorCode AS STRING) AS operatorCode",
    "CAST(name AS STRING) AS operatorName",
    "CAST(timestamp AS TIMESTAMP) AS timestamp",
]

oocpage_operator: list = [
    "CAST(Total AS INT) AS Total",
    "CAST(PPM_text AS INT) AS PPM_text",
    "CAST(PPM_rag AS STRING) AS PPM_rag",
    "CAST(RollingPPM_text AS INT) AS RollingPPM_text",
    "CAST(RollingPPM_rag AS STRING) AS RollingPPM_rag",
    "CAST(RollingPPM_trendInd AS STRING) AS RollingPPM_trendInd",
    "CAST(operatorCode AS STRING) AS operatorCode",
    "CAST(name AS STRING) AS operatorName",
    "CAST(timestamp AS TIMESTAMP) AS timestamp",
]

focpage_nationalppm: list = [
    "CAST(Total AS INT) AS Total",
    "CAST(OnTime AS INT) AS OnTime",
    "CAST(Late AS INT) AS Late",
    "CAST(PPM_text AS INT) AS PPM_text",
    "CAST(PPM_rag AS STRING) AS PPM_rag",
    "CAST(PPM_ragDisplayFlag AS STRING) AS PPM_ragDisplayFlag",
    "CAST(RollingPPM_text AS INT) AS RollingPPM_text",
    "CAST(RollingPPM_rag AS STRING) AS RollingPPM_rag",
    "CAST(RollingPPM_trendInd AS STRING) AS RollingPPM_trendInd",
    "CAST(timestamp AS TIMESTAMP) AS timestamp",
]

focpage_operator: list = [
    "CAST(Total AS INT) AS Total",
    "CAST(PPM_text AS INT) AS PPM_text",
    "CAST(PPM_rag AS STRING) AS PPM_rag",
    "CAST(RollingPPM_text AS INT) AS RollingPPM_text",
    "CAST(RollingPPM_rag AS STRING) AS RollingPPM_rag",
    "CAST(RollingPPM_trendInd AS STRING) AS RollingPPM_trendInd",
    "CAST(operatorCode AS STRING) AS operatorCode",
    "CAST(name AS STRING) AS operatorName",
    "CAST(timestamp AS TIMESTAMP) AS timestamp",
]

operatorpage_operators: list = [
    "CAST(sectorCode AS STRING) AS sectorCode",
    "CAST(sectorName AS STRING) AS sectorName",
    "CAST(total AS INT) AS Total",
    "CAST(onTime AS INT) AS OnTime",
    "CAST(late AS INT) AS Late",
    "CAST(cancelVeryLate AS INT) AS CancelVeryLate",
    "CAST(PPM_text AS INT) AS PPM_text",
    "CAST(PPM_rag AS STRING) AS PPM_rag",
    "CAST(RollingPPM_text AS INT) AS RollingPPM_text",
    "CAST(RollingPPM_rag AS STRING) AS RollingPPM_rag",
    "CAST(RollingPPM_trendInd AS STRING) AS RollingPPM_trendInd",
    "CAST(timestamp AS TIMESTAMP) AS timestamp",
]

operatorpage_serviceoperators: list = [
    "CAST(name AS STRING) AS Name",
    "CAST(Total AS INT) AS Total",
    "CAST(OnTime AS INT) AS OnTime",
    "CAST(Late AS INT) AS Late",
    "CAST(CancelVeryLate AS INT) AS CancelVeryLate",
    "CAST(PPM_text AS INT) AS PPM_text",
    "CAST(PPM_rag AS STRING) AS PPM_rag",
    "CAST(RollingPPM_text AS INT) AS RollingPPM_text",
    "CAST(RollingPPM_rag AS STRING) AS RollingPPM_rag",
    "CAST(RollingPPM_trendInd AS STRING) AS RollingPPM_trendInd",
    "CAST(timestamp AS TIMESTAMP) AS timestamp",
]
