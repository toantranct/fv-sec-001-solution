# FV-SEC-001 Test Solution

TypeScript CLI solution for the `FV-SEC001 - Software Engineer Challenge — Ad Performance Aggregator`.

## What This Does

- Stream-reads a CSV file line by line through a streaming parser
- Validates rows and skips malformed data safely
- Aggregates by `campaign_id`
- Computes `CTR = total_clicks / total_impressions`
- Computes `CPA = total_spend / total_conversions`
- Writes:
  - `results/top10_ctr.csv`
  - `results/top10_cpa.csv`

## Schema Summary

The real dataset inspected locally at `/home/toan/projects/work/fv-sec-001-solution/data/ad_data.csv` uses:

```csv
campaign_id,date,impressions,clicks,spend,conversions
```

Observed characteristics from local inspection:

- Header is present
- File uses comma delimiters and CRLF line endings
- Sampled rows are plain, unquoted CSV records
- File size is about 1.04 GB
- File contains about 26.8 million data rows plus a header
- Data currently contains 50 unique campaign IDs

## Risks And Handling

- Missing or invalid header: fail fast
- Malformed rows: skipped safely without terminating the run
- Invalid numeric values: skipped
- Invalid dates: skipped
- Zero impressions: excluded from CTR ranking because CTR is undefined
- Zero conversions: excluded from CPA ranking and rendered as `null` where applicable
- Deterministic ranking ties: broken by `campaign_id` ascending

## Project Structure

```text
src/cli.ts
src/csv.ts
src/aggregator.ts
src/output.ts
test/aggregator.test.ts
package.json
tsconfig.json
README.md
```

## Libraries Used

- `csv-parse`: streaming CSV parsing
- `typescript`: compilation
- `@types/node`: Node.js typings

## Setup

```bash
npm install
```

## Build

```bash
npm run build
```

## Run

```bash
node dist/src/cli.js --input /path/to/ad_data.csv --output ./results
```

Example using the local dataset already available in this workspace:

```bash
node dist/src/cli.js \
  --input /home/toan/projects/work/fv-sec-001-solution/data/ad_data.csv \
  --output /home/toan/projects/work/fv-sec-001-test-solution/results
```

## Test

```bash
npm test
```

## Output Format

Both output files use:

```csv
campaign_id,total_impressions,total_clicks,total_spend,total_conversions,CTR,CPA
```

Formatting rules:

- `total_spend`: 2 decimal places
- `CTR`: 4 decimal places
- `CPA`: 2 decimal places
- Undefined CPA values are written as `null`

## Performance Notes

- The CSV reader is fully streaming and does not load the source file into memory.
- Aggregation keeps only one totals object per campaign in memory.
- Top-10 ranking is maintained as bounded in-memory lists; it does not perform a full sort over the raw dataset.

## Processing Time And Memory

- Measured on the local dataset at `/home/toan/projects/work/fv-sec-001-solution/data/ad_data.csv`
- Rows processed: 26,843,544
- Valid rows: 26,843,544
- Skipped rows: 0
- Wall-clock processing time: `1:26.39`
- Peak memory usage: `90,568 KB` maximum resident set size

Measurement command:

```bash
/usr/bin/time -v node dist/src/cli.js \
  --input /home/toan/projects/work/fv-sec-001-solution/data/ad_data.csv \
  --output /home/toan/projects/work/fv-sec-001-test-solution/results
```
