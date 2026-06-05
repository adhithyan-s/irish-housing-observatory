import json
import boto3
import urllib.request
import urllib.error
from datetime import datetime, timezone

# --- config ---
BUCKET = "ireland-housing-bronze"

# CSO uses JSON-RPC over POST
CSO_JSONRPC_URL = "https://ws.cso.ie/public/api.jsonrpc"

# Verified dataset codes (HPM06 = RPPI, BHA04 = dwelling completions)
DATASETS = {
    "house_price_index": "HPM06",
    "new_dwelling_completions": "BHA04",
}

s3 = boto3.client("s3")

# --- helpers ---
def fetch_cso_dataset(table_code: str) -> dict:
    """Fetch a CSO PxStat dataset via JSON-RPC POST."""
    payload = {
        "jsonrpc": "2.0",
        "method": "PxStat.Data.Cube_API.ReadDataset",
        "params": {
            "class": "query",
            "id": [],
            "dimension": {},
            "extension": {
                "pivot": None,
                "codes": False,
                "language": {"code": "en"},
                "format": {"type": "JSON-stat", "version": "2.0"},
                "matrix": table_code
            },
            "version": "2.0"
        }
    }

    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        CSO_JSONRPC_URL,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json"
        },
        method="POST"
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

def build_s3_key(dataset_name: str) -> str:
    """Hive-style partition key for easy Glue/Athena discovery."""
    now = datetime.now(timezone.utc)
    return (
        f"cso/{dataset_name}/"
        f"year={now.year}/month={now.month:02d}/"
        f"data_{now.strftime('%Y%m%d_%H%M%S')}.json"
    )

def write_to_s3(data: dict, key: str) -> None:
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False),
        ContentType="application/json",
        Metadata={
            "ingestion_time": datetime.now(timezone.utc).isoformat(),
            "source": "cso_pxstat"
        },
    )

# --- handler ---
def lambda_handler(event, context):
    results = []

    for dataset_name, code in DATASETS.items():
        try:
            print(f"Fetching CSO dataset: {code} ({dataset_name})")
            data = fetch_cso_dataset(code)

            # CSO JSON-RPC wraps data inside a "result" key — check for errors
            if "error" in data:
                raise ValueError(f"CSO API error: {data['error']}")

            key = build_s3_key(dataset_name)
            write_to_s3(data, key)

            print(f"Written to s3://{BUCKET}/{key}")
            results.append({
                "dataset": dataset_name,
                "code": code,
                "status": "success",
                "s3_key": key
            })

        except urllib.error.HTTPError as e:
            msg = f"HTTP {e.code}: {e.reason}"
            print(f"{msg}")
            results.append({"dataset": dataset_name, "status": "failed", "error": msg})
        except Exception as e:
            print(f"Unexpected error for {code}: {e}")
            results.append({"dataset": dataset_name, "status": "failed", "error": str(e)})

    success_count = sum(1 for r in results if r["status"] == "success")
    print(f"\nIngestion complete: {success_count}/{len(DATASETS)} datasets succeeded")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "results": results,
            "ingestion_time": datetime.now(timezone.utc).isoformat()
        })
    }