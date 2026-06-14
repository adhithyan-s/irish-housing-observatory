import sys
import boto3
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType

# --- init ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER = "s3://ireland-housing-silver"
GOLD = "s3://ireland-housing-gold"
DATABASE = "irish_housing_observatory"
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

glue_client = boto3.client("glue", region_name="us-east-1")

# --- helper: register Gold table in Glue Catalog ---
def register_table(table_name: str, s3_path: str, df):
    """
    Register a Gold Parquet table in the Glue Data Catalog.
    Drops and recreates the table so schema stays in sync on reruns.
    """
    try:
        glue_client.delete_table(DatabaseName=DATABASE, Name=table_name)
        print(f"Dropped existing table: {table_name}")
    except glue_client.exceptions.EntityNotFoundException:
        pass

    columns = [
        {"Name": field.name, "Type": str(field.dataType.simpleString())}
        for field in df.schema.fields
        if field.name != "year"   # partition key registered separately
    ]

    glue_client.create_table(
        DatabaseName=DATABASE,
        TableInput={
            "Name": table_name,
            "StorageDescriptor": {
                "Columns": columns,
                "Location": s3_path,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                },
            },
            "PartitionKeys": [{"Name": "year", "Type": "int"}],
            "TableType": "EXTERNAL_TABLE",
        }
    )
    print(f"Registered in Glue Catalog: {DATABASE}.{table_name}")
    
    # after registering the table, discover partitions automatically
    athena_client = boto3.client("athena", region_name="us-east-1")
    athena_client.start_query_execution(
        QueryString=f"MSCK REPAIR TABLE {DATABASE}.{table_name}",
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={
            "OutputLocation": "s3://ireland-housing-silver/athena-results/"
        }
    )
    print(f"Partition repair triggered for: {table_name}")


# ==============================================================
# TABLE 1 — gold_property_price_index
# Source: HPM06 Silver
# One row per (period, type_of_residential_property)
# Pivot the 4 statistics into columns instead of stacked rows
# ==============================================================
def build_price_index():
    print("\n-- Gold: property_price_index --")
    path = f"{SILVER}/cso/house_price_index/"

    df = spark.read.parquet(path)
    print(f"Silver rows: {df.count():,}")

    # filter to the four key property types for the dashboard
    # sub-regional breakdowns (Fingal, South Dublin etc) too granular for Gold
    key_types = [
        "National - all residential properties",
        "National - houses",
        "National - apartments",
        "Dublin - all residential properties",
        "Dublin - houses",
        "Dublin - apartments",
    ]
    df = df.filter(F.col("type_of_residential_property").isin(key_types))

    # shorten statistic values to pivot column names
    # "Residential Property Price Index" -> "price_index"
    stat_map = {
        "Residential Property Price Index": "price_index",
        "Percentage Change over 1 month for Residential Property Price Index": "pct_change_1m",
        "Percentage Change over 3 months for Residential Property Price Index": "pct_change_3m",
        "Percentage Change over 12 months for Residential Property Price Index": "pct_change_12m",
    }

    mapping_list = []
    for k, v in stat_map.items():
        mapping_list.append(F.lit(k))
        mapping_list.append(F.lit(v))

    mapping_expr = F.create_map(*mapping_list)

    df = df.withColumn("stat_key", mapping_expr[F.col("statistic")])

    # pivot: one row per period x property_type, columns = stat_key values
    df_pivot = df.groupBy(
        "period", "year", "type_of_residential_property"
    ).pivot(
        "stat_key", ["price_index", "pct_change_1m", "pct_change_3m", "pct_change_12m"]
    ).agg(F.round(F.first("metric_value"), 2))

    # extract month number from "2005 January" -> 1
    month_map = {
        "January": 1, 
        "February": 2, 
        "March": 3, 
        "April": 4,
        "May": 5, 
        "June": 6, 
        "July": 7, 
        "August": 8,
        "September": 9,
        "October": 10, 
        "November": 11, 
        "December": 12
    }
    month_expr = F.create_map(*[item for pair in
        [(F.lit(k), F.lit(v)) for k, v in month_map.items()]
        for item in pair])

    df_pivot = df_pivot.withColumn(
        "month_name",
        F.trim(F.regexp_extract(F.col("period"), r"^\d{4}\s+(.+)$", 1))
    ).withColumn(
        "month",
        month_expr[F.col("month_name")].cast(IntegerType())
    ).drop("month_name")

    # add metadata
    df_pivot = df_pivot.withColumn("gold_updated", F.lit(TODAY))

    # sort for readability
    df_pivot = df_pivot.orderBy("year", "month", "type_of_residential_property")

    gold_path = f"{GOLD}/property_price_index/"
    df_pivot.write.mode("overwrite").partitionBy("year").parquet(gold_path)
    count = df_pivot.count()
    print(f"Written {count:,} rows -> {gold_path}")
    register_table("gold_property_price_index", gold_path, df_pivot)
    return df_pivot


# ==============================================================
# TABLE 2 — gold_avg_rent_by_location_year
# Source: RTB Silver
# Filter to headline rent (all bedrooms, all property types)
# Add year-on-year change via window function
# ==============================================================
def build_rent_trends():
    print("\n-- Gold: avg_rent_by_location_year --")
    path = f"{SILVER}/rtb/rent_by_county/"

    df = spark.read.parquet(path)
    print(f"Silver rows: {df.count():,}")

    # filter to headline figures only
    df = df.filter(
        (F.col("number_of_bedrooms") == "All bedrooms") &
        (F.col("property_type") == "All property types")
    )

    # aggregate: one row per location × year
    df_agg = df.groupBy("location", "year").agg(
        F.round(F.avg("value"), 2).alias("avg_monthly_rent"),
        F.round(F.min("value"), 2).alias("min_monthly_rent"),
        F.round(F.max("value"), 2).alias("max_monthly_rent"),
        F.count("value").alias("sample_count")
    )

    # year-on-year change % using window function
    window_loc = Window.partitionBy("location").orderBy("year")
    df_agg = df_agg.withColumn(
        "prev_year_rent",
        F.lag("avg_monthly_rent", 1).over(window_loc)
    ).withColumn(
        "yoy_change_pct",
        F.round(
            (F.col("avg_monthly_rent") - F.col("prev_year_rent"))
            / F.col("prev_year_rent") * 100,
            2
        )
    ).drop("prev_year_rent")

    df_agg = df_agg.withColumn("gold_updated", F.lit(TODAY))
    df_agg = df_agg.orderBy("location", "year")

    gold_path = f"{GOLD}/avg_rent_by_location_year/"
    df_agg.write.mode("overwrite").partitionBy("year").parquet(gold_path)
    count = df_agg.count()
    print(f"Written {count:,} rows -> {gold_path}")
    register_table("gold_avg_rent_by_location_year", gold_path, df_agg)
    return df_agg


# ==============================================================
# TABLE 3 — gold_planning_permissions_trend
# Source: BHA04 Silver
# NOTE: This is planning permissions for communal dwellings,
#       NOT total dwelling completions — named honestly
# ==============================================================
def build_permissions_trend():
    print("\n-- Gold: planning_permissions_trend --")
    path = f"{SILVER}/cso/new_dwelling_completions/"

    df = spark.read.parquet(path)
    print(f"Silver rows: {df.count():,}")

    # filter to new construction planning permissions only
    df = df.filter(
        (F.col("type_of_construction") == "New construction") &
        (F.col("statistic") == "Planning Permissions Granted for Communal Dwellings")
    )

    df_agg = df.groupBy("year").agg(
        F.round(F.sum("metric_value"), 0).alias("new_communal_permissions"),
    )

    # year-on-year change
    window_yr = Window.orderBy("year")
    df_agg = df_agg.withColumn(
        "prev_year_permissions",
        F.lag("new_communal_permissions", 1).over(window_yr)
    ).withColumn(
        "yoy_change",
        F.round(
            F.col("new_communal_permissions") - F.col("prev_year_permissions"),
            0
        )
    ).withColumn(
        "yoy_change_pct",
        F.round(
            (F.col("new_communal_permissions") - F.col("prev_year_permissions"))
            / F.col("prev_year_permissions") * 100,
            2
        )
    ).drop("prev_year_permissions")

    # context column — estimated annual need for ALL housing in Ireland
    # 35,000/year per Housing Commission. This dataset is communal only,
    # so supply_gap is indicative not absolute.
    df_agg = df_agg.withColumn("estimated_annual_need", F.lit(35000))
    df_agg = df_agg.withColumn("gold_updated", F.lit(TODAY))
    df_agg = df_agg.orderBy("year")

    gold_path = f"{GOLD}/planning_permissions_trend/"
    df_agg.write.mode("overwrite").partitionBy("year").parquet(gold_path)
    count = df_agg.count()
    print(f"Written {count:,} rows -> {gold_path}")
    register_table("gold_planning_permissions_trend", gold_path, df_agg)
    return df_agg


# ==================================================================
# TABLE 4 — gold_property_sales_dublin
# Source: PPR Silver
# Two aggregations: annual headline + new vs second-hand breakdown
# ==================================================================
def build_ppr_sales():
    print("\n-- Gold: property_sales_dublin --")
    path = f"{SILVER}/ppr/sales_register/"

    df = spark.read.parquet(path)
    print(f"Silver rows: {df.count():,}")

    # simplify property_description to new vs second-hand
    df = df.withColumn("sale_type",
        F.when(
            F.lower(F.col("property_description")).contains("new"),
            F.lit("New")
        ).otherwise(F.lit("Second-Hand"))
    )

    # annual headline figures
    df_annual = df.groupBy("year").agg(
        F.count("price_eur").alias("total_sales"),
        F.round(F.avg("price_eur"), 0).alias("avg_price_eur"),
        F.round(F.expr("percentile_approx(price_eur, 0.5)"), 0).alias("median_price_eur"),
        F.round(F.min("price_eur"), 0).alias("min_price_eur"),
        F.round(F.max("price_eur"), 0).alias("max_price_eur"),
    )

    # year-on-year avg price change
    window_yr = Window.orderBy("year")
    df_annual = df_annual.withColumn(
        "prev_avg_price",
        F.lag("avg_price_eur", 1).over(window_yr)
    ).withColumn(
        "yoy_price_change_pct",
        F.round(
            (F.col("avg_price_eur") - F.col("prev_avg_price"))
            / F.col("prev_avg_price") * 100,
            2
        )
    ).drop("prev_avg_price")

    # new vs second-hand breakdown
    df_type = df.groupBy("year", "sale_type").agg(
        F.count("price_eur").alias("sales_count"),
        F.round(F.avg("price_eur"), 0).alias("avg_price_eur"),
        F.round(F.expr("percentile_approx(price_eur, 0.5)"), 0).alias("median_price_eur"),
    )

    # join breakdown onto annual
    df_new = df_type.filter(F.col("sale_type") == "New") \
        .select("year",
                F.col("sales_count").alias("new_build_sales"),
                F.col("avg_price_eur").alias("new_build_avg_price"))

    df_sh = df_type.filter(F.col("sale_type") == "Second-Hand") \
        .select("year",
                F.col("sales_count").alias("second_hand_sales"),
                F.col("avg_price_eur").alias("second_hand_avg_price"))

    df_final = df_annual \
        .join(df_new, on="year", how="left") \
        .join(df_sh, on="year", how="left")

    df_final = df_final.withColumn("county", F.lit("Dublin"))
    df_final = df_final.withColumn("gold_updated", F.lit(TODAY))
    df_final = df_final.orderBy("year")

    gold_path = f"{GOLD}/property_sales_dublin/"
    df_final.write.mode("overwrite").partitionBy("year").parquet(gold_path)
    count = df_final.count()
    print(f"Written {count:,} rows -> {gold_path}")
    register_table("gold_property_sales_dublin", gold_path, df_final)
    return df_final


# ==================================================
# TABLE 5 — gold_housing_crisis_summary
# One row per year joining all four Gold tables
# This is the master dashboard table
# ==================================================
def build_crisis_summary(df_price, df_rent, df_perms, df_sales):
    print("\n-- Gold: housing_crisis_summary --")

    # --- National price index (one value per year - December reading) ---
    df_national_price = df_price.filter(
        (F.col("type_of_residential_property") == "National - all residential properties") &
        (F.col("month") == 12)
    ).select(
        "year",
        F.col("price_index").alias("national_price_index"),
        F.col("pct_change_12m").alias("national_price_yoy_pct")
    )

    # --- Dublin price index (December reading) ---
    df_dublin_price = df_price.filter(
        (F.col("type_of_residential_property") == "Dublin - all residential properties") &
        (F.col("month") == 12)
    ).select(
        "year",
        F.col("price_index").alias("dublin_price_index")
    )

    # --- National avg rent (all locations average) ---
    df_national_rent = df_rent.groupBy("year").agg(
        F.round(F.avg("avg_monthly_rent"), 2).alias("avg_national_rent")
    )

    # --- Dublin rent (filter to Dublin locations only) ---
    df_dublin_rent = df_rent.filter(
        F.lower(F.col("location")).contains("dublin")
    ).groupBy("year").agg(
        F.round(F.avg("avg_monthly_rent"), 2).alias("avg_dublin_rent")
    )

    # --- Planning permissions ---
    df_perms_slim = df_perms.select(
        "year",
        "new_communal_permissions",
        "yoy_change_pct"
    ).withColumnRenamed("yoy_change_pct", "permissions_yoy_pct")

    # --- Dublin sales ---
    df_sales_slim = df_sales.select(
        "year",
        "total_sales",
        "avg_price_eur",
        "median_price_eur",
        "yoy_price_change_pct"
    ).withColumnRenamed("total_sales", "dublin_total_sales") \
     .withColumnRenamed("avg_price_eur", "dublin_avg_sale_price") \
     .withColumnRenamed("median_price_eur", "dublin_median_sale_price")

    # --- Build year spine from all available years ---
    all_years = (
        df_national_price.select("year")
        .union(df_national_rent.select("year"))
        .union(df_perms_slim.select("year"))
        .union(df_sales_slim.select("year"))
        .distinct()
        .orderBy("year")
    )

    # --- LEFT JOIN everything onto the year spine ---
    df_summary = all_years \
        .join(df_national_price, on="year", how="left") \
        .join(df_dublin_price, on="year", how="left") \
        .join(df_national_rent, on="year", how="left") \
        .join(df_dublin_rent, on="year", how="left") \
        .join(df_perms_slim, on="year", how="left") \
        .join(df_sales_slim, on="year", how="left")

    df_summary = df_summary.withColumn("gold_updated", F.lit(TODAY))
    df_summary = df_summary.orderBy("year")

    gold_path = f"{GOLD}/housing_crisis_summary/"
    df_summary.write.mode("overwrite").partitionBy("year").parquet(gold_path)
    count = df_summary.count()
    print(f"Written {count:,} rows -> {gold_path}")
    print(f"\nPreview (first 5 years):")
    df_summary.select(
        "year", "national_price_index", "avg_national_rent",
        "avg_dublin_rent", "dublin_avg_sale_price"
    ).orderBy("year").show(5, truncate=False)
    register_table("gold_housing_crisis_summary", gold_path, df_summary)
    return df_summary


# =============================
# Run all five Gold tables
# =============================
print("\n========================================")
print("  Irish Housing Observatory — Gold Layer")
print("========================================")

df_price = build_price_index()
df_rent = build_rent_trends()
df_perms = build_permissions_trend()
df_sales = build_ppr_sales()
df_crisis = build_crisis_summary(df_price, df_rent, df_perms, df_sales)

job.commit()
print("\nSilver -> Gold complete")
print(f"Gold tables registered in: {DATABASE}")
print(f"Query with Athena: SELECT * FROM {DATABASE}.gold_housing_crisis_summary ORDER BY year;")