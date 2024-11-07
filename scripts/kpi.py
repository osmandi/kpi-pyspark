# Import
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import os
import json

# Print PySpark Version
spark = SparkSession.builder.appName("Actividad 2 - Gobierno de datos").getOrCreate()
print(f"Spark version: {spark.version}")

# Directory
DATA_DIRECTORY_RAW = "/opt/spark/work-dir/raw"
DATA_DIRECTORY_REPORTS = "/opt/spark/work-dir/reports"

# Function to UDF
def parse_json(json_string):
    """
    Parses a JSON-formatted string, evaluates it as a Python expression, 
    and returns the resulting object as a JSON-encoded string.

    This function takes a string containing a JSON-formatted object, evaluates 
    it using the `eval()` function to convert it into a Python object, and 
    then serializes it back into a JSON string using `json.dumps()`.

    Parameters:
        json_string (str): A JSON-formatted string representing a valid Python expression. 
                            It should be a valid JSON object, such as a dictionary, list, etc.

    Returns:
        str: A JSON-encoded string representing the evaluated Python object.

    Raises:
        SyntaxError: If the input string is not a valid Python expression or JSON format.
        ValueError: If the input string is invalid JSON or cannot be serialized.

    Warning:
        TODO: Remove eval function for the following updates. I used becasue there are simple quote as a contraction in English expression
        This function uses `eval()` to evaluate the string as Python code. 
        Using `eval()` on untrusted input can be a security risk, as it allows for 
        arbitrary code execution. Ensure that the input string comes from a trusted source.

    Example:
        >>> json_string = '{"name": "John", "age": 30, "city": "New York"}'
        >>> result = parse_json(json_string)
        >>> print(result)
        {"name": "John", "age": 30, "city": "New York"}

    Edge Cases:
        - If the input string is not a valid JSON or Python expression, a `SyntaxError` or `ValueError` 
          will be raised during the evaluation or serialization step.
        - Invalid JSON format (e.g., missing quotes, extra commas) will raise an error.
    """
    return json.dumps(eval(json_string))

parse_json_udf = F.udf(parse_json)

# Declare schemas
schema_visitas_lote = T.StructType([
    T.StructField("channelGrouping", T.StringType(), True),
    T.StructField("customDimensions", T.StringType(), True),
    T.StructField("date", T.IntegerType(), True),
    T.StructField("device", T.StringType(), True),
    T.StructField("fullVisitorId", T.StringType(), True),
    T.StructField("geoNetwork", T.StringType(), True),
    T.StructField("hits", T.StringType(), True),
    T.StructField("socialEngagementType", T.StringType(), True),
    T.StructField("totals", T.StringType(), True),
    T.StructField("trafficSource", T.StringType(), True),
    T.StructField("visitId", T.IntegerType(), True),
    T.StructField("visitNumber", T.IntegerType(), True),
    T.StructField("visitStartTime", T.IntegerType(), True)
])

device_schema = T.StructType([
    T.StructField("browser", T.StringType(), True),
    T.StructField("browserVersion", T.StringType(), True),
    T.StructField("browserSize", T.StringType(), True),
    T.StructField("operatingSystem", T.StringType(), True),
    T.StructField("operatingSystemVersion", T.StringType(), True),
    T.StructField("isMobile", T.BooleanType(), True),
    T.StructField("mobileDeviceBranding", T.StringType(), True),
    T.StructField("mobileDeviceModel", T.StringType(), True),
    T.StructField("mobileInputSelector", T.StringType(), True),
    T.StructField("mobileDeviceInfo", T.StringType(), True),
    T.StructField("mobileDeviceMarketingName", T.StringType(), True),
    T.StructField("flashVersion", T.StringType(), True),
    T.StructField("language", T.StringType(), True),
    T.StructField("screenColors", T.StringType(), True),
    T.StructField("screenResolution", T.StringType(), True),
    T.StructField("deviceCategory", T.StringType(), True)
])

geoNetwork_schema = T.StructType([
    T.StructField("continent", T.StringType(), True, metadata = {"desc": "Simple description"}),
    T.StructField("subContinent", T.StringType(), True),
    T.StructField("country", T.StringType(), True),
    T.StructField("region", T.StringType(), True),
    T.StructField("metro", T.StringType(), True),
    T.StructField("city", T.StringType(), True),
    T.StructField("cityId", T.StringType(), True),
    T.StructField("networkDomain", T.StringType(), True),
    T.StructField("latitude", T.StringType(), True),
    T.StructField("longitude", T.StringType(), True),
    T.StructField("networkLocation", T.StringType(), True)
])

trafficSource_schema = T.StructType([
    T.StructField("campaign", T.StringType(), True),
    T.StructField("source", T.StringType(), True),
    T.StructField("medium", T.StringType(), True),
    T.StructField("keyword", T.StringType(), True),
    T.StructField("adwordsClickInfo", T.StringType(), True)
])

customDimensions_schema = T.ArrayType(
    T.StructType([
        T.StructField("index", T.StringType(), True),
        T.StructField("value", T.StringType(),True)
    ])
)

totals_schema = T.StructType([
    T.StructField("visits", T.StringType(), True),
    T.StructField("hits", T.StringType(), True),
    T.StructField("pageviews", T.StringType(), True),
    T.StructField("bounces", T.StringType(), True),
    T.StructField("newVisits", T.StringType(), True),
    T.StructField("sessionQualityDim", T.StringType(), True),
    T.StructField("timeOnSite", T.StringType(), True),
    T.StructField("totalTransactionRevenue", T.StringType(), True),
    T.StructField("transactionRevenue", T.StringType(), True),
    T.StructField("transactions", T.StringType(), True)
])

hits_schema = T.ArrayType(
    T.StructType([
        T.StructField("appInfo", T.StructType([
            T.StructField("exitScreenName", T.StringType(), True),
            T.StructField("landingScreenName", T.StringType(), True),
            T.StructField("screenDepth", T.StringType(), True),
            T.StructField("screenName", T.StringType(), True)
        ]), True),
        T.StructField("contentGroup", T.StructType([
            T.StructField("contentGroup1", T.StringType(), True),
            T.StructField("contentGroup2", T.StringType(), True),
            T.StructField("contentGroup3", T.StringType(), True),
            T.StructField("contentGroup4", T.StringType(), True),
            T.StructField("contentGroup5", T.StringType(), True),
            T.StructField("contentGroupUniqueViews2", T.StringType(), True),
            T.StructField("previousContentGroup1", T.StringType(), True),
            T.StructField("previousContentGroup2", T.StringType(), True),
            T.StructField("previousContentGroup3", T.StringType(), True),
            T.StructField("previousContentGroup4", T.StringType(), True),
            T.StructField("previousContentGroup5", T.StringType(), True)
        ]), True),
        T.StructField("customDimensions", T.ArrayType(T.StringType(), True), True),
        T.StructField("customMetrics", T.ArrayType(T.StringType(), True), True),
        T.StructField("customVariables", T.ArrayType(T.StringType(), True), True),
        T.StructField("dataSource", T.StringType(), True),
        T.StructField("eCommerceAction", T.StructType([
            T.StructField("action_type", T.StringType(), True),
            T.StructField("step", T.StringType(), True)
        ]), True),
        T.StructField("exceptionInfo", T.StructType([
            T.StructField("isFatal", T.BooleanType(), True)
        ]), True),
        T.StructField("experiment", T.ArrayType(T.StringType(), True), True),
        T.StructField("hitNumber", T.StringType(), True),
        T.StructField("hour", T.StringType(), True),
        T.StructField("isEntrance", T.BooleanType(), True),
        T.StructField("isExit", T.BooleanType(), True),
        T.StructField("isInteraction", T.BooleanType(), True),
        T.StructField("item", T.StructType([
            T.StructField("currencyCode", T.StringType(), True)
        ]), True),
        T.StructField("minute", T.StringType(), True),
        T.StructField("page", T.StructType([
            T.StructField("hostname", T.StringType(), True),
            T.StructField("pagePath", T.StringType(), True),
            T.StructField("pagePathLevel1", T.StringType(), True),
            T.StructField("pagePathLevel2", T.StringType(), True),
            T.StructField("pagePathLevel3", T.StringType(), True),
            T.StructField("pagePathLevel4", T.StringType(), True),
            T.StructField("pageTitle", T.StringType(), True)
        ]), True),
        T.StructField("product", T.ArrayType(T.StructType([
            T.StructField("customDimensions", T.ArrayType(T.StringType(), True), True),
            T.StructField("customMetrics", T.ArrayType(T.StringType(), True), True),
            T.StructField("isImpression", T.BooleanType(), True),
            T.StructField("localProductPrice", T.StringType(), True),
            T.StructField("productBrand", T.StringType(), True),
            T.StructField("productListName", T.StringType(), True),
            T.StructField("productListPosition", T.StringType(), True),
            T.StructField("productPrice", T.StringType(), True),
            T.StructField("productSKU", T.StringType(), True),
            T.StructField("productVariant", T.StringType(), True),
            T.StructField("v2ProductCategory", T.StringType(), True),
            T.StructField("v2ProductName", T.StringType(), True)
        ]), True), True),
        T.StructField("promotion", T.ArrayType(T.StringType(), True), True),
        T.StructField("publisher_infos", T.ArrayType(T.StringType(), True), True),
        T.StructField("referer", T.StringType(), True),
        T.StructField("social", T.StructType([
            T.StructField("hasSocialSourceReferral", T.StringType(), True),
            T.StructField("socialInteractionNetworkAction", T.StringType(), True),
            T.StructField("socialNetwork", T.StringType(), True)
        ]), True),
        T.StructField("time", T.StringType(), True),
        T.StructField("transaction", T.StructType([
            T.StructField("currencyCode", T.StringType(), True)
        ]), True),
        T.StructField("type", T.StringType(), True)
    ])
)

# Load DataFrames
df_visitas_lote_01 = (spark.read.format("csv")
        .option("header", True)
        #.option("quote", "\'")
        .option("escape", "\"")
        #.option("inferSchema", True)
        .schema(schema_visitas_lote)
        .load(os.path.join(DATA_DIRECTORY_RAW, "Visitas_lote_01.csv")) 
     .withColumn("hits", parse_json_udf(F.col("hits")))
     .withColumn("hits", F.from_json(F.col("hits"), hits_schema))
     .withColumn("device", F.from_json(F.col("device"), device_schema))
     .withColumn("customDimensions", F.from_json(F.col("customDimensions"), customDimensions_schema))
     .withColumn("geoNetwork", F.from_json(F.col("geoNetwork"), geoNetwork_schema))
     .withColumn("trafficSource", F.from_json(F.col("trafficSource"), trafficSource_schema))
     .withColumn("totals", F.from_json(F.col("totals"), totals_schema))
)

df_visitas_lote_02 = (
    spark.read.format("csv")
    .option("header", True)
    .option("quote", "\"")
    .option("escape", "\"")
    #.option("inferSchema", True)
    .schema(schema_visitas_lote)
    .load(os.path.join(DATA_DIRECTORY_RAW, "Visitas_lote_02.csv")) 
    .withColumn("hits", parse_json_udf(F.col("hits")))
    .withColumn("hits", F.from_json(F.col("hits"), hits_schema))
    .withColumn("device", F.from_json(F.col("device"), device_schema))
    .withColumn("customDimensions", F.from_json(F.col("customDimensions"), customDimensions_schema))
    .withColumn("geoNetwork", F.from_json(F.col("geoNetwork"), geoNetwork_schema))
    .withColumn("trafficSource", F.from_json(F.col("trafficSource"), trafficSource_schema))
    .withColumn("totals", F.from_json(F.col("totals"), totals_schema))
)

# Union the two DataFrames
df_visitas_lote = (
    df_visitas_lote_01.union(df_visitas_lote_02)
)

# KPIs metrics

## Finance
### Ingresos totales por canal de Marketing
(
    df_visitas_lote.dropna(subset=["totals.transactionRevenue"])
    .groupBy(["channelGrouping"]).agg(
        F.sum(F.col("totals.transactionRevenue").cast(T.IntegerType())).alias("totalRevenue")
    )
).orderBy(["totalRevenue"], ascending=False).toPandas().to_csv(os.path.join(DATA_DIRECTORY_REPORTS, "ingresos_totales_por_marketing.csv"), index=False)

### Ingresos por dispositivo
(
    df_visitas_lote.dropna(subset=["totals.transactionRevenue"])
    .select(
        F.col("device.browser").alias("deviceName"),
        F.col("totals.transactionRevenue").cast(T.IntegerType()).alias("totalRevenue")
    )
    .groupBy(["deviceName"]).agg(
        F.sum(F.col("totalRevenue")).alias("totalRevenue")
    )
).orderBy(["totalRevenue"], ascending=False).toPandas().to_csv(os.path.join(DATA_DIRECTORY_REPORTS, "ingresos_totales_por_dispositivo.csv"), index=False)

### Ingresos totales por usuarios
(
    df_visitas_lote.dropna(subset=["totals.transactionRevenue"])
    .groupBy(["fullVisitorId"]).agg(
        F.sum(F.col("totals.transactionRevenue").cast(T.IntegerType())).alias("totalRevenue")
    )
).toPandas().to_csv(os.path.join(DATA_DIRECTORY_REPORTS, "ingresos_totales_usuario.csv"), index=False)

## Clientes
### Tasa de conversión por canal
df_visitas_lote.select(
        F.col("channelGrouping"),
        F.col("totals.transactions").cast(T.IntegerType()).alias("total_transactions")
    ).groupBy("channelGrouping").agg(
        F.count(F.lit(1)).alias("total_visits"),
        F.sum(F.col("total_transactions")).alias("total_conversions")
    ).select(F.col("channelGrouping"), F.col("total_visits"), F.col("total_conversions"), F.round((F.col("total_conversions")/F.col("total_visits"))  * 100, 2).alias("conversion_rate")
).toPandas().to_csv(os.path.join(DATA_DIRECTORY_REPORTS, "tasa_conversion_por_canal.csv"), index=False)

### Número de visitas por año
df_visitas_lote.select(F.substring(F.col("date"), 1, 4).cast(T.IntegerType()).alias("year")).groupBy("year").agg(
    F.count(F.lit(1)).alias("total_visits")
).toPandas().to_csv(os.path.join(DATA_DIRECTORY_REPORTS, "numero_visitas_por_anio.csv"), index=False)

## Procesos interos
### Tiempo medio de visita por año
(
    df_visitas_lote.select(
        F.substring(F.col("date"), 1, 4).cast(T.IntegerType()).alias("year"),
        F.col("totals.timeOnSite").cast(T.IntegerType()).alias("visitDuration")
    ).groupBy("year").agg(
        F.count(F.lit(1)).alias("total_visits"),
        F.sum(F.col("visitDuration")).alias("total_visitDuration")
    ).select(
        F.col("year"),
        F.col("total_visits"),
        F.col("total_visitDuration"),
        F.round((F.col("total_visitDuration")/F.col("total_visits")) * 100, 2).alias("avg_visit_time")
    )
).toPandas().to_csv(os.path.join(DATA_DIRECTORY_REPORTS, "tiempo_medio_visita_anio.csv"), index=False)

### Campañas con más visitas
(df_visitas_lote.where(F.col("trafficSource.campaign") != '(not set)')
    .select(F.col("trafficSource.campaign").alias("campaign"))
    .groupBy("campaign").count()
).toPandas().to_csv(os.path.join(DATA_DIRECTORY_REPORTS, "camapnas_mas_visitas.csv"), index=False)

## Aprendizaje y crecimiento
### Tasa de retención de usuarios
(
    df_visitas_lote.select(
        F.substring(F.col("date"), 1, 4).cast(T.IntegerType()).alias("year"),
        F.col("fullVisitorId"),
        F.row_number().over(Window.partitionBy("fullVisitorId").orderBy("date")).alias("row_number")
    ).groupBy("year").agg(
        F.count_distinct(F.col("fullVisitorId")).alias("total_unique_visitors"),
        F.count_distinct(F.when(F.col("row_number") > 1, F.col("fullVisitorId"))).alias("total_recurrent_visitors")
    ).select(
        F.col("year"),
        F.col("total_unique_visitors"),
        F.col("total_recurrent_visitors"),
        F.round((F.col("total_recurrent_visitors")/F.col("total_unique_visitors")),2).alias("retention_rate")
    )
).toPandas().to_csv(os.path.join(DATA_DIRECTORY_REPORTS, "tasa_retencion_usuarios_anio.csv"), index=False)

### Bounce rate por año
(df_visitas_lote.select(
        F.substring(F.col("date"), 1, 4).cast(T.IntegerType()).alias("year"),
        F.col("totals.bounces").alias("session_with_bounce")
    ).groupBy("year").agg(
        F.count(F.lit(1)).alias("total_sessions"),
        F.sum(F.col("session_with_bounce")).alias("total_sessions_with_bounce")
    ).select(
        F.col("year"),
        F.col("total_sessions"),
        F.col("total_sessions_with_bounce"),
        F.round((F.col("total_sessions_with_bounce")/F.col("total_sessions")) * 100, 2).alias("bounce_rate")
    )
).toPandas().to_csv(os.path.join(DATA_DIRECTORY_REPORTS, "bounce_rate_por_anio.csv"), index=False)