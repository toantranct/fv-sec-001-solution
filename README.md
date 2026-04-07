# fv-sec-001-solution

TypeScript CLI solution for the `FV-SEC001 - Software Engineer Challenge — Ad Performance Aggregator`.

## Overview

This project processes a large ad-performance CSV using a streaming pipeline and produces two ranked result files:

- `results/top10_ctr.csv`: top 10 campaigns by highest CTR
- `results/top10_cpa.csv`: top 10 campaigns by lowest CPA

The implementation is designed for large inputs and does not load the full CSV into memory.

## Input Schema

The expected CSV schema is:

```csv
campaign_id,date,impressions,clicks,spend,conversions
```

Field definitions:

- `campaign_id`: string
- `date`: `YYYY-MM-DD`
- `impressions`: non-negative integer
- `clicks`: non-negative integer
- `spend`: non-negative decimal
- `conversions`: non-negative integer

## Behavior

- Reads the CSV as a stream
- Validates rows and safely skips malformed records
- Aggregates totals by `campaign_id`
- Computes:
  - `CTR = total_clicks / total_impressions`
  - `CPA = total_spend / total_conversions`
- Excludes campaigns with zero impressions from CTR ranking
- Excludes campaigns with zero conversions from CPA ranking
- Breaks metric ties by `campaign_id` ascending for deterministic output

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
node dist/src/cli.js --input ./ad_data.csv --output ./results
```

Optional parser selection:

```bash
node dist/src/cli.js --parser csv-parse --input ./ad_data.csv --output ./results
node dist/src/cli.js --parser readline --input ./ad_data.csv --output ./results
```

If you are using the challenge dataset from the original repository, first unzip `ad_data.csv.zip` and then pass the extracted `ad_data.csv` file to the CLI.

## Test

```bash
npm test
```

## Output Format

Both output files use this header:

```csv
campaign_id,total_impressions,total_clicks,total_spend,total_conversions,CTR,CPA
```

Formatting rules:

- `total_spend`: 2 decimal places
- `CTR`: 4 decimal places
- `CPA`: 2 decimal places
- Undefined CPA values are written as `null`

## Error Handling

- Missing or invalid header: fail fast
- Malformed rows: skipped safely without terminating the run
- Invalid numeric values: skipped
- Invalid dates: skipped

## Performance Notes

- The CSV reader is fully streaming and does not load the source file into memory.
- Aggregation keeps only one totals object per campaign in memory.
- Top-10 ranking is maintained as bounded in-memory lists instead of sorting the raw dataset.

## Benchmark

Measured on the full challenge dataset with both parser modes:

- Rows processed: 26,843,544
- Valid rows: 26,843,544
- Skipped rows: 0
- `csv-parse`: `1:18.52`, `90,076 KB` max RSS
- `readline`: `0:40.06`, `87,328 KB` max RSS
- Output comparison: `top10_ctr.csv` and `top10_cpa.csv` matched exactly across both parser modes

Example measurement command:

```bash
/usr/bin/time -v node dist/src/cli.js --parser readline --input ./ad_data.csv --output ./results
```

Raw benchmark artifacts:

- `benchmarks/csv-parse.txt`
- `benchmarks/readline.txt`

## Repository Contents

This repository includes generated result files:

- `results/top10_ctr.csv`
- `results/top10_cpa.csv`
