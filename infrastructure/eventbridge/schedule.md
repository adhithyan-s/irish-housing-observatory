# EventBridge Schedule

## Schedule name
`irish-housing-observatory-monthly`

## What it does
Triggers the full Step Functions pipeline automatically on the 1st of every month at 6am UTC - no manual intervention needed.

## Cron expression
```
0 6 1 * ? *
```
Breakdown:
- `0`  = minute 0
- `6`  = hour 6 (6am UTC)
- `1`  = day 1 of the month
- `*`  = every month
- `?`  = any day of week (required by AWS when day of month is specified)
- `*`  = every year

## Target
- Service: AWS Step Functions
- State machine: `irish-housing-observatory-pipeline`
- Input: `{}`

## Why the 1st of the month?
CSO publishes monthly price index updates typically in the first week of the following month. 
RTB publishes quarterly rent data with a similar lag.
Running on the 1st of each month ensures we capture the most recently published data as early as possible.

## Pipeline flow triggered
1. CSO Lambda + PPR/RTB Lambda run in parallel (~1 min)
2. Bronze -> Silver Glue job runs (~4 min)
3. Silver -> Gold Glue job runs (~3 min)
4. SNS email sent on success or failure (~8-10 min total)