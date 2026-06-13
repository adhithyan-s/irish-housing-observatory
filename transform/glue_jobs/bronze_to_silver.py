import sys
import json
import itertools
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType
import boto3

# --- init ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE = "s3://ireland-housing-bronze"
SILVER = "s3://ireland-housing-silver"
DATABASE = "irish_housing_observatory"

# --- helper: create Glue catalog database if not exists ---
def ensure_database(glue_client, db_name):
    try:
        glue_client.get_database(Name=db_name)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_database(DatabaseInput={"Name": db_name})
        print(f"Created Glue database: {db_name}")

import boto3
glue_client = boto3.client("glue", region_name="us-east-1")
ensure_database(glue_client, DATABASE)


# -----------------------------------------
# 1. CSO — unpack JSON-stat into flat rows
# -----------------------------------------
def parse_jsonstat(raw: dict) -> list:
    """
    Unpack a CSO JSON-RPC response (result is JSON-stat2 inside 'result' key).
    Returns a list of flat dicts ready for a Spark DataFrame.
    """
    # JSON-RPC wraps result
    data = raw.get("result", raw)

    # JSON-stat2 structure
    dimension_ids = data["id"]           # e.g. ["TLIST(M1)", "C02803V03373", "STATISTIC"]
    dimension_sizes = data["size"]       # e.g. [4, 255, 20]
    values = data["value"]               # flat array, row-major order

    # Build label lookups for each dimension
    dims = []
    dim_labels = {}
    for dim_id in dimension_ids:
        dim_info = data["dimension"][dim_id]
        category = dim_info["category"]
        labels = category.get("label", {})
        index = category.get("index", {})

        dim_labels[dim_id] = dim_info.get("label", dim_id)

        # handle all three forms of category.index
        if isinstance(index, dict) and len(index) > 0:
            # Most dimensions: {"CODE": 0, "CODE2": 1} — sort by position
            codes = [k for k, v in sorted(index.items(), key=lambda x: x[1])]
        elif isinstance(index, list) and len(index) > 0:
            # HPM06 STATISTIC: ["HPM06C01", "HPM06C02", ...] — already ordered
            codes = index
        else:
            # TLIST dimensions: index is {} or missing — codes are label keys
            codes = list(labels.keys())

        if not codes:
            print(f"WARNING: dimension {dim_id} has no codes. "
                  f"category keys: {list(category.keys())}")

        dims.append({"id": dim_id, "codes": codes, "labels": labels})

    # Walk the flat value array using dimension sizes
    rows = []
    total = len(values)
    idx = 0

    import itertools
    ranges = [range(s) for s in dimension_sizes]
    for combo in itertools.product(*ranges):
        if idx >= total:
            break
        row = {}
        for d, pos in zip(dims, combo):
            code = d["codes"][pos]
            row[d["id"]] = d["labels"].get(code, code)
        row["value"] = values[idx]
        rows.append(row)
        idx += 1
        
    return rows, dim_labels


def process_cso_source(source_name: str, dataset_code: str):
    print(f"\n-- CSO: {source_name} --")
    
    # Read all JSON files for this source from bronze
    bronze_path = f"{BRONZE}/cso/{source_name}/"
    try:
        raw_df = spark.read.text(bronze_path, recursiveFileLookup=True)
    except Exception as e:
        print(f"  No files found at {bronze_path}: {e}")
        return

    rows_all = []
    dim_labels_final = {}
    for row in raw_df.collect():
        try:
            raw = json.loads(row.value)
            rows, dim_labels = parse_jsonstat(raw)
            rows_all.extend(rows)
            dim_labels_final.update(dim_labels)
        except Exception as e:
            print(f"Parse error: {e}")
            continue

    if not rows_all:
        print(f"No rows parsed for {source_name}")
        return

    df = spark.createDataFrame(rows_all)
    print(f"Raw columns: {df.columns}")
    print(f"Dim labels: {dim_labels_final}")
    print(f"Row count: {len(rows_all):,}")

    # clean label helper
    def clean_label(label: str) -> str:
        return (label.lower().strip()
                .replace(" ", "_")
                .replace("-", "_")
                .replace("(", "")
                .replace(")", "")
                .replace("/", "_"))

    # Rename columns
    for col in df.columns:
        if "TLIST" in col:
            df = df.withColumnRenamed(col, "period")
        elif col == "value":
            df = df.withColumnRenamed(col, "metric_value")
        elif col in dim_labels_final:
            clean = clean_label(dim_labels_final[col])
            df = df.withColumnRenamed(col, clean)
            print(f"Renamed: {col} -> {clean}")
        else:
            # fallback - shouldnt happen
            fallback = col.lower().replace(" ", "_")
            df = df.withColumnRenamed(col, fallback)
            print(f"Fallback rename: {col} -> {fallback}")

    # Show sample period values so we can verify
    if "period" in df.columns:
        sample = [r[0] for r in df.select("period").limit(5).collect()]
        print(f"Sample period values: {sample}")

    # Cast value to double
    df = df.withColumn("metric_value", F.col("metric_value").cast(DoubleType()))
    
    # Add metadata columns
    df = df.withColumn("source_dataset", F.lit(dataset_code))
    df = df.withColumn("ingestion_date", F.lit(datetime.now(timezone.utc).strftime("%Y-%m-%d")))

    # robust year extraction — grabs first 4-digit number in period
    df = df.withColumn(
        "year",
        F.regexp_extract(
            F.col("period")
            .cast(StringType()), 
            r"(\d{4})", 
            1
        ).cast(IntegerType())
    )

    # Log how many rows have null year — should be 0
    null_years = df.filter(F.col("year").isNull()).count()
    if null_years > 0:
        print(f"WARNING: {null_years:,} rows have null year")
        df.filter(F.col("year").isNull()).select("period").limit(5).show()

    silver_path = f"{SILVER}/cso/{source_name}/"
    df.write.mode("overwrite").partitionBy("year").parquet(silver_path)
    print(f"Written {df.count():,} rows -> {silver_path}")
    return df


# -------------------------
# 2. RTB — clean flat CSV
# -------------------------
def process_rtb():
    print(f"\n-- RTB: rent by county --")
    bronze_path = f"{BRONZE}/rtb/rent_by_county/"

    try:
        df = spark.read.option("header", "true") \
                       .option("inferSchema", "true") \
                       .csv(bronze_path, recursiveFileLookup=True)
    except Exception as e:
        print(f"No files found: {e}")
        return

    print(f"Raw columns: {df.columns}")
    print(f"Raw row count: {df.count():,}")

    # CSO flat CSV includes both code columns (C02970V03592) and their
    # human label columns (Number of Bedrooms) side by side.
    # Drop the code columns — keep only the label columns.
    code_cols = [c for c in df.columns if c.startswith("C0") and "V" in c]
    stat_cols = [c for c in df.columns if c.startswith("STATISTIC") and c != "Statistic Label"]
    unit_cols = [c for c in df.columns if c == "UNIT"]
    tlist_cols = [c for c in df.columns if "TLIST" in c]

    drop = code_cols + stat_cols + unit_cols + tlist_cols
    print(f"Dropping code columns: {drop}")
    df = df.drop(*drop)
    print(f"Remaining columns: {df.columns}")

    # Standardise column names: lowercase, spaces to underscores
    for col in df.columns:
        clean = col.lower().strip().replace(" ", "_").replace("(", "").replace(")", "")
        df = df.withColumnRenamed(col, clean)
    
    print(f"Clean columns: {df.columns}")

    # Cast VALUE to double
    if "value" in df.columns:
        df = df.withColumn("value",
            F.regexp_replace(F.col("value").cast(StringType()), r"[€,$\s]", "")
             .cast(DoubleType()))

    # Extract year — "Year" column already exists in this CSV
    if "year" in df.columns:
        df = df.withColumn("year",
            F.regexp_extract(F.col("year").cast(StringType()), r"(\d{4})", 1)
             .cast(IntegerType()))
    else:
        df = df.withColumn("year", F.lit(datetime.now(timezone.utc).year))

    df = df.withColumn("source_dataset", F.lit("RIA02"))
    df = df.withColumn("ingestion_date",
                       F.lit(datetime.now(timezone.utc).strftime("%Y-%m-%d")))

    silver_path = f"{SILVER}/rtb/rent_by_county/"
    df.write.mode("overwrite").partitionBy("year").parquet(silver_path)
    print(f"  Written {df.count():,} rows -> {silver_path}")
    return df


# ---------------------------------------------------
# 3. PPR — latin-1 CSV, price cleaning, date parsing
# ---------------------------------------------------
def process_ppr():
    print(f"\n-- PPR: sales register --")

    import boto3
    from functools import reduce

    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket="ireland-housing-bronze", Prefix="ppr/sales_register/")
    csv_keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                csv_keys.append(f"s3://ireland-housing-bronze/{obj['Key']}")

    print(f"Found {len(csv_keys)} CSV files: {csv_keys}")
    if not csv_keys:
        print("No CSV files found")
        return

    STANDARD_COLS = {
        "Date of Sale (dd/mm/yyyy)": "date_of_sale_raw",
        "Address": "address",
        "County": "county",
        "Postal Code": "postal_code",
        "Eircode": "eircode",
        "Price (€)": "price_raw",
        "Price (\x80)": "price_raw",
        "Price (?)": "price_raw",
        "Not Full Market Price": "not_full_market_price",
        "VAT Exclusive": "vat_exclusive",
        "Description of Property": "property_description",
        "Property Size Description": "property_size",
    }

    dfs = []
    for csv_path in sorted(csv_keys):
        print(f"\nReading: {csv_path}")
        try:
            df_year = spark.read \
                .option("header", "true") \
                .option("encoding", "ISO-8859-1") \
                .option("inferSchema", "false") \
                .option("quote", '"') \
                .option("escape", '"') \
                .csv(csv_path)

            print(f"Columns: {df_year.columns}")
            print(f"Rows: {df_year.count():,}")

            for old, new in STANDARD_COLS.items():
                if old in df_year.columns:
                    df_year = df_year.withColumnRenamed(old, new)

            sample = df_year.select("date_of_sale_raw").limit(3).collect()
            print(f"  Sample dates: {[r[0] for r in sample]}")
            dfs.append(df_year)

        except Exception as e:
            print(f"ERROR reading {csv_path}: {e}")
            continue

    if not dfs:
        print("No DataFrames to union")
        return

    df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    df.cache()
    df.count()  # materialise cache immediately — single S3 read from here on
    print(f"\nTotal rows after union (cached): {df.count():,}")
    
    # Coalesce postal_code and eircode into single location_code column
    # 2015-2020 files have postal_code, 2021 file has eircode — same concept
    df = df.withColumn("location_code",
        F.coalesce(
            F.when(F.col("eircode").isNotNull() & (F.col("eircode") != ""), F.col("eircode")),
            F.when(F.col("postal_code").isNotNull() & (F.col("postal_code") != ""), F.col("postal_code"))
        )
    )
    df = df.drop("postal_code", "eircode")

    df = df.withColumn("price_eur",
        F.regexp_replace(F.col("price_raw"), r"[€\x80?,\s]", "")
         .cast(DoubleType()))

    df = df.withColumn("date_normalised",
        F.when(
            F.col("date_of_sale_raw").rlike(r"^\s*\d{1,2}\s+\d{1,2}\s+\d{4}\s*$"),
            F.concat(
                F.lpad(F.regexp_extract(F.trim(F.col("date_of_sale_raw")), r"^(\d{1,2})", 1), 2, "0"),
                F.lit("/"),
                F.lpad(F.regexp_extract(F.trim(F.col("date_of_sale_raw")), r"^\d{1,2}\s+(\d{1,2})", 1), 2, "0"),
                F.lit("/"),
                F.regexp_extract(F.trim(F.col("date_of_sale_raw")), r"(\d{4})$", 1)
            )
        ).otherwise(F.col("date_of_sale_raw"))
    )

    df = df.withColumn("date_of_sale",
        F.to_date(F.col("date_normalised"), "dd/MM/yyyy"))

    before = df.count()
    df = df.filter(F.col("date_of_sale").isNotNull())
    after = df.count()
    print(f"Dropped {before - after:,} unparseable rows ({(before-after)/before*100:.2f}%)")
    print(f"Remaining: {after:,} rows with valid dates")

    df = df.withColumn("year",  F.year(F.col("date_of_sale")))
    df = df.withColumn("month", F.month(F.col("date_of_sale")))

    df = df.filter(
        F.upper(F.col("not_full_market_price")).isin(["NO", ""])
        | F.col("not_full_market_price").isNull()
    )

    df = df.drop("price_raw", "date_of_sale_raw", "date_normalised")

    df = df.withColumn("source_dataset", F.lit("PSRA_PPR"))
    df = df.withColumn("ingestion_date",
                       F.lit(datetime.now(timezone.utc).strftime("%Y-%m-%d")))
    df = df.withColumn("coverage", F.lit("dublin"))

    null_years = df.filter(F.col("year").isNull()).count()
    if null_years > 0:
        print(f"BUG: {null_years:,} rows still have null year after date filter — investigate")
    else:
        print(f"Year partition check: all rows have valid year")

    silver_path = f"{SILVER}/ppr/sales_register/"
    df.write.mode("overwrite").partitionBy("year").parquet(silver_path)
    print(f"Written {df.count():,} rows -> {silver_path}")

    df.unpersist()
    return df


# --------------
# Run all three
# --------------
process_cso_source("house_price_index", "HPM06")
process_cso_source("new_dwelling_completions", "BHA04")
process_rtb()
process_ppr()

job.commit()
print("\nBronze -> Silver complete")