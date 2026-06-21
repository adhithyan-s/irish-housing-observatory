# IAM Roles

## ireland-housing-lambda-role
Used by all Lambda functions in this project.

### Attached policies
| Policy | Purpose |
|--------|---------|
| `AWSLambdaBasicExecutionRole` | Write logs to CloudWatch |
| `AmazonS3FullAccess` | Read/write all project S3 buckets |
| `AmazonSSMReadOnlyAccess` | Read config/secrets |
| `AmazonAthenaFullAccess` | Run Athena queries (API Lambda) |

---

## ireland-housing-glue-role
Used by all AWS Glue jobs.

### Attached policies
| Policy | Purpose |
|--------|---------|
| `AWSGlueServiceRole` | Base Glue permissions |
| `AmazonS3FullAccess` | Read Bronze, write Silver and Gold |
| `CloudWatchLogsFullAccess` | Glue job logging |
| `AmazonAthenaFullAccess` | Run MSCK REPAIR TABLE after Gold writes |

---

## ireland-housing-stepfunctions-role
Used by the Step Functions state machine.

### Attached policies
| Policy | Purpose |
|--------|---------|
| `AWSLambdaRole` | Invoke Lambda functions |
| `AWSGlueServiceRole` | Start and monitor Glue jobs |
| `AmazonSNSFullAccess` | Publish success/failure notifications |
| `CloudWatchLogsFullAccess` | Write execution logs |

---

## grafana-athena-readonly
Used by Grafana Cloud to query Athena.

### Attached policies
| Policy | Purpose |
|--------|---------|
| `AmazonAthenaFullAccess` | Run Athena queries |
| `AmazonS3ReadOnlyAccess` | Read Gold Parquet files |
| `grafana-athena-results-write` (inline) | Write query results to S3 |