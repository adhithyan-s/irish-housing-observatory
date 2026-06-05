import json
import boto3
import urllib.request
import urllib.error
from datetime import datetime

BUCKET = "ireland-housing-bronze"
s3 = boto3.client("s3")

# --- PPR: per-year CSVs from data.smartdublin.ie ---
# We ingest from 2015 onward (pre-2015 is sparse and less relevant)
PPR_YEARLY_FILES = [
    ("2015", "https://data.smartdublin.ie/dataset/b0dd7d39-8eb5-4710-b46c-6a0db49e64af/resource/11d2f401-87d6-4252-bb1d-8f2af1ef19db/download/ppr-2015-dublin.csv"),
    ("2016", "https://data.smartdublin.ie/dataset/b0dd7d39-8eb5-4710-b46c-6a0db49e64af/resource/a4d52dde-6749-4d6f-87db-26c7ec87b205/download/ppr-2016-dublin.csv"),
    ("2020", "https://data.smartdublin.ie/dataset/b0dd7d39-8eb5-4710-b46c-6a0db49e64af/resource/1b3ccb47-ed22-460c-af96-f066ce2b3c35/download/ppr-2020.csv"),
    ("2021", "https://data.smartdublin.ie/dataset/b0dd7d39-8eb5-4710-b46c-6a0db49e64af/resource/43209239-0ee7-4c8b-9599-79a54b61dd01/download/ppr-2021.csv"),
]

def ingest_ppr() -> list:
    """
    Download per-year PPR CSVs from data.smartdublin.ie.
    Each file goes into its own year partition in Bronze.
    NOTE: these files are Dublin-only subsets. We'll add national data later.
    """
    results = []
    now = datetime.utcnow()

    for year, url in PPR_YEARLY_FILES:
        try:
            print(f"Fetching PPR {year}...")
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "IrishHousingObservatory/1.0"}
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw_bytes = resp.read()

            row_count = raw_bytes.decode("utf-8", errors="replace").count("\n") - 1
            print(f"  PPR {year}: {row_count:,} rows")

            key = (
                f"ppr/sales_register/"
                f"year={year}/month=full/"
                f"ppr_{year}_dublin_{now.strftime('%Y%m%d')}.csv"
            )

            s3.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=raw_bytes,
                ContentType="text/csv",
                Metadata={
                    "ingestion_time": now.isoformat(),
                    "source": "psra_smartdublin",
                    "data_year": year,
                    "row_count": str(row_count),
                    "coverage": "dublin_only"
                }
            )
            print(f"Written to s3://{BUCKET}/{key}")
            results.append({
                "dataset": f"ppr_{year}",
                "status": "success",
                "s3_key": key,
                "row_count": row_count
            })

        except Exception as e:
            print(f"PPR {year} failed: {e}")
            results.append({"dataset": f"ppr_{year}", "status": "failed", "error": str(e)})

    return results


# --- RTB ---
RTB_CSV_URLS = [
    {
        "name": "rtb_average_monthly_rent",
        "url": "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/RIA02/CSV/1.0/en",
        "filename": "rtb_average_monthly_rent.csv"
    }
]

def ingest_rtb() -> list:
    results = []
    now = datetime.utcnow()

    for dataset in RTB_CSV_URLS:
        try:
            print(f"Fetching RTB: {dataset['name']}")
            req = urllib.request.Request(
                dataset["url"],
                headers={"User-Agent": "IrishHousingObservatory/1.0"}
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw_bytes = resp.read()

            row_count = raw_bytes.decode("utf-8", errors="replace").count("\n") - 1
            print(f"  RTB: {row_count:,} rows")

            key = (
                f"rtb/rent_by_county/"
                f"year={now.year}/month={now.month:02d}/"
                f"{dataset['filename']}"
            )
            s3.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=raw_bytes,
                ContentType="text/csv",
                Metadata={
                    "ingestion_time": now.isoformat(),
                    "source": "rtb_data_gov_ie",
                    "row_count": str(row_count)
                }
            )
            print(f"Written to s3://{BUCKET}/{key}")
            results.append({
                "dataset": dataset["name"],
                "status": "success",
                "s3_key": key,
                "row_count": row_count
            })

        except Exception as e:
            print(f"RTB failed: {e}")
            results.append({"dataset": dataset["name"], "status": "failed", "error": str(e)})

    return results


# --- handler ---
def lambda_handler(event, context):
    all_results = []

    ppr_results = ingest_ppr()
    all_results.extend(ppr_results)

    rtb_results = ingest_rtb()
    all_results.extend(rtb_results)

    success = sum(1 for r in all_results if r["status"] == "success")
    print(f"\nIngestion complete: {success}/{len(all_results)} succeeded")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "results": all_results,
            "ingestion_time": datetime.utcnow().isoformat()
        })
    }