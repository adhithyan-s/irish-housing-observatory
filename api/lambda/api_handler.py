import json
import boto3
import time
import os
from datetime import datetime, timezone

# --- config ---
DATABASE = "irish_housing_observatory"
RESULTS_BUCKET  = "s3://ireland-housing-silver/api-query-results/"
REGION = "us-east-1"
MAX_POLL_SECS = 25   # Athena timeout before we return 504
POLL_INTERVAL = 0.5  # seconds between status checks

athena = boto3.client("athena", region_name=REGION)

# --- CORS headers — included on every response ---
CORS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Content-Type": "application/json",
}


# ===============
# Athena helpers
# ===============
def run_query(sql: str) -> list[dict]:
    """
    Execute a SQL query on Athena and return results as a list of dicts.
    Polls until complete or MAX_POLL_SECS is reached.
    """
    # start execution
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": RESULTS_BUCKET},
    )
    execution_id = response["QueryExecutionId"]

    # poll until finished
    start = time.time()
    while True:
        status = athena.get_query_execution(
            QueryExecutionId=execution_id
        )["QueryExecution"]["Status"]

        state = status["State"]

        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena query {state}: {reason}")
        if time.time() - start > MAX_POLL_SECS:
            raise TimeoutError(f"Athena query timed out after {MAX_POLL_SECS}s")

        time.sleep(POLL_INTERVAL)

    # fetch results
    paginator = athena.get_paginator("get_query_results")
    pages = paginator.paginate(QueryExecutionId=execution_id)

    rows = []
    headers = None
    for page in pages:
        result_rows = page["ResultSet"]["Rows"]
        if headers is None:
            # first row is always the column header
            headers = [col["VarCharValue"] for col in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            values = [col.get("VarCharValue", None) for col in row["Data"]]
            rows.append(dict(zip(headers, values)))

    return rows


def coerce_numbers(rows: list[dict]) -> list[dict]:
    """
    Athena returns everything as strings.
    Try to convert each value to int or float where possible.
    """
    coerced = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            if v is None or v == "":
                new_row[k] = None
                continue
            try:
                # try int first (no decimal point)
                if "." not in v:
                    new_row[k] = int(v)
                else:
                    new_row[k] = float(v)
            except (ValueError, TypeError):
                new_row[k] = v
        coerced.append(new_row)
    return coerced


# =================
# Response helpers
# =================
def ok(data, meta=None) -> dict:
    body = {"status": "ok", "data": data}
    if meta:
        body["meta"] = meta
    return {
        "statusCode": 200,
        "headers": CORS,
        "body": json.dumps(body, default=str)
    }

def error(status_code: int, message: str) -> dict:
    return {
        "statusCode": status_code,
        "headers": CORS,
        "body": json.dumps({"status": "error", "message": message})
    }


# ===============
# Route handlers
# ===============
def handle_health(_params: dict) -> dict:
    return ok({
        "service": "Irish Housing Observatory API",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database": DATABASE,
    })


def handle_summary(params: dict) -> dict:
    """
    GET /summary
    Optional: ?from_year=2010&to_year=2024
    Returns: one row per year with all crisis indicators
    """
    from_year = params.get("from_year", "2001")
    to_year = params.get("to_year",   "2025")

    sql = f"""
        SELECT
            year,
            national_price_index,
            national_price_yoy_pct,
            dublin_price_index,
            avg_national_rent,
            avg_dublin_rent,
            new_communal_permissions,
            permissions_yoy_pct,
            dublin_total_sales,
            dublin_avg_sale_price,
            dublin_median_sale_price,
            yoy_price_change_pct,
            gold_updated
        FROM gold_housing_crisis_summary
        WHERE year BETWEEN {int(from_year)} AND {int(to_year)}
        ORDER BY year
    """

    rows = coerce_numbers(run_query(sql))
    return ok(rows, meta={
        "table": "gold_housing_crisis_summary",
        "from_year": from_year,
        "to_year": to_year,
        "row_count": len(rows)
    })


def handle_rents(params: dict) -> dict:
    """
    GET /rents
    Optional: ?location=Dublin&from_year=2016&to_year=2024
    Returns: average monthly rent by location and year
    """
    location = params.get("location")
    from_year = params.get("from_year", "2016").strip().strip('"\'')
    to_year = params.get("to_year",   "2025").strip().strip('"\'')

    # build WHERE clause safely — location is a string filter
    location_clause = ""
    if location:
        # sanitise: allow only alphanumeric, spaces, commas, hyphens
        safe_loc = "".join(c for c in location if c.isalnum() or c in " ,-")
        location_clause = f"AND LOWER(location) LIKE LOWER('%{safe_loc}%')"

    sql = f"""
        SELECT
            location,
            year,
            avg_monthly_rent,
            min_monthly_rent,
            max_monthly_rent,
            yoy_change_pct,
            sample_count
        FROM gold_avg_rent_by_location_year
        WHERE year BETWEEN {int(from_year)} AND {int(to_year)}
        {location_clause}
        ORDER BY location, year
        LIMIT 500
    """

    rows = coerce_numbers(run_query(sql))
    return ok(rows, meta={
        "table": "gold_avg_rent_by_location_year",
        "location_filter": location,
        "from_year": from_year,
        "to_year": to_year,
        "row_count": len(rows)
    })


def handle_prices(params: dict) -> dict:
    """
    GET /prices
    Optional: ?type=National&from_year=2005&to_year=2025
    type matches against type_of_residential_property
    e.g. 'National', 'Dublin', 'houses', 'apartments'
    Returns: property price index by type and month
    """
    prop_type = params.get("type")
    from_year = params.get("from_year", "2005").strip().strip('"\'')
    to_year = params.get("to_year",   "2025").strip().strip('"\'')

    type_clause = ""
    if prop_type:
        safe_type = "".join(c for c in prop_type if c.isalnum() or c in " -")
        type_clause = f"AND LOWER(type_of_residential_property) LIKE LOWER('%{safe_type}%')"

    sql = f"""
        SELECT
            period,
            year,
            month,
            type_of_residential_property,
            price_index,
            pct_change_1m,
            pct_change_3m,
            pct_change_12m
        FROM gold_property_price_index
        WHERE year BETWEEN {int(from_year)} AND {int(to_year)}
        {type_clause}
        ORDER BY year, month, type_of_residential_property
        LIMIT 1000
    """

    rows = coerce_numbers(run_query(sql))
    return ok(rows, meta={
        "table": "gold_property_price_index",
        "type_filter": prop_type,
        "from_year": from_year,
        "to_year": to_year,
        "row_count": len(rows)
    })


def handle_sales(params: dict) -> dict:
    """
    GET /sales
    Optional: ?from_year=2015&to_year=2021
    Returns: Dublin property sales aggregated by year
    """
    from_year = params.get("from_year", "2015")
    to_year = params.get("to_year",   "2025")

    sql = f"""
        SELECT
            year,
            total_sales,
            avg_price_eur,
            median_price_eur,
            min_price_eur,
            max_price_eur,
            new_build_sales,
            new_build_avg_price,
            second_hand_sales,
            second_hand_avg_price,
            yoy_price_change_pct,
            county
        FROM gold_property_sales_dublin
        WHERE year BETWEEN {int(from_year)} AND {int(to_year)}
        ORDER BY year
    """

    rows = coerce_numbers(run_query(sql))
    return ok(rows, meta={
        "table": "gold_property_sales_dublin",
        "from_year": from_year,
        "to_year": to_year,
        "row_count": len(rows)
    })


# =========
# Router
# =========
ROUTES = {
    "GET /health": handle_health,
    "GET /summary": handle_summary,
    "GET /rents": handle_rents,
    "GET /prices": handle_prices,
    "GET /sales": handle_sales,
}

def lambda_handler(event, context):
    # handle CORS preflight
    if event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
        return {"statusCode": 200, "headers": CORS, "body": ""}

    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    path = event.get("rawPath", "/")
    params = event.get("queryStringParameters") or {}

    route_key = f"{method} {path}"
    print(f"Request: {route_key} params={params}")

    handler = ROUTES.get(route_key)
    if not handler:
        return error(404, f"Route not found: {route_key}. "
                          f"Available: {list(ROUTES.keys())}")

    try:
        return handler(params)
    except TimeoutError as e:
        print(f"Timeout: {e}")
        return error(504, "Query timed out — try adding filters to narrow the result set")
    except RuntimeError as e:
        print(f"Athena error: {e}")
        return error(500, f"Query failed: {str(e)}")
    except ValueError as e:
        print(f"Bad params: {e}")
        return error(400, f"Invalid parameter: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")
        return error(500, "Internal server error")