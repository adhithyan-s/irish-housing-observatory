# IAM Roles

## ireland-housing-lambda-role
Used by all Lambda functions in this project.

### Attached policies
- AWSLambdaBasicExecutionRole (CloudWatch logging)
- AmazonS3FullAccess (read/write all project S3 buckets)
- AmazonSSMReadOnlyAccess (config/secrets)

## ireland-housing-glue-role
Used by all AWS Glue jobs.

### Attached policies
- AWSGlueServiceRole (base Glue permissions)
- AmazonS3FullAccess (read Bronze, write Silver/Gold)
- CloudWatchLogsFullAccess (job logging)
