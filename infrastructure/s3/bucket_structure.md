# S3 Bucket Structure

## ireland-housing-bronze
Raw data exactly as received from sources. Never modified.
ireland-housing-bronze/
├── cso/
│   ├── house_price_index/year=YYYY/month=MM/data_YYYYMMDD_HHMMSS.json
│   └── new_dwelling_completions/year=YYYY/month=MM/data_YYYYMMDD_HHMMSS.json
├── rtb/
│   └── rent_by_county/year=YYYY/month=MM/rtb_average_monthly_rent.csv
├── ppr/
│   └── sales_register/year=YYYY/month=full/ppr_YYYY_dublin_YYYYMMDD.csv
└── dhlgh/
└── homelessness/   (Phase 3)

## ireland-housing-silver
Cleaned, typed, deduplicated Parquet. Partitioned by year.

## ireland-housing-gold
Analytics aggregations. Query-ready for Athena and Grafana.
EOF