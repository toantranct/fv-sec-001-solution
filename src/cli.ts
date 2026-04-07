import { constants } from "node:fs";
import { access } from "node:fs/promises";
import { parseArgs as parseNodeArgs } from "node:util";
import { resolve } from "node:path";

import { aggregateCampaigns, rankTopCpa, rankTopCtr } from "./aggregator";
import type { ParserMode } from "./csv";
import { writeMetricsCsv } from "./output";

export interface CliOptions {
  inputPath: string;
  outputDir: string;
  parserMode: ParserMode;
}

export interface CliRunResult {
  totalRows: number;
  validRows: number;
  skippedRows: number;
  campaignCount: number;
  topCtrPath: string;
  topCpaPath: string;
}

export function parseArgs(argv: string[]): CliOptions {
  const { values } = parseNodeArgs({
    args: argv,
    options: {
      input: {
        type: "string",
      },
      output: {
        type: "string",
      },
      parser: {
        type: "string",
      },
    },
    allowPositionals: false,
  });

  if (!values.input || !values.output) {
    throw new Error(
      "Usage: node dist/src/cli.js --input <path/to/ad_data.csv> --output <dir> [--parser csv-parse|readline]",
    );
  }

  const parserMode = values.parser ?? "csv-parse";

  if (parserMode !== "csv-parse" && parserMode !== "readline") {
    throw new Error(`Invalid parser: "${parserMode}". Expected "csv-parse" or "readline".`);
  }

  return {
    inputPath: resolve(values.input),
    outputDir: resolve(values.output),
    parserMode,
  };
}

export async function run(options: CliOptions): Promise<CliRunResult> {
  await access(options.inputPath, constants.R_OK);

  const aggregation = await aggregateCampaigns(options.inputPath, options.parserMode);
  const topCtr = rankTopCtr(aggregation.campaigns.values(), 10);
  const topCpa = rankTopCpa(aggregation.campaigns.values(), 10);

  const [topCtrPath, topCpaPath] = await Promise.all([
    writeMetricsCsv(options.outputDir, "top10_ctr.csv", topCtr),
    writeMetricsCsv(options.outputDir, "top10_cpa.csv", topCpa),
  ]);

  return {
    totalRows: aggregation.totalRows,
    validRows: aggregation.validRows,
    skippedRows: aggregation.skippedRows,
    campaignCount: aggregation.campaignCount,
    topCtrPath,
    topCpaPath,
  };
}

async function main(): Promise<void> {
  try {
    const options = parseArgs(process.argv.slice(2));
    const result = await run(options);

    process.stdout.write(
      [
        `Processed rows: ${result.totalRows}`,
        `Valid rows: ${result.validRows}`,
        `Skipped rows: ${result.skippedRows}`,
        `Campaigns aggregated: ${result.campaignCount}`,
        `Parser: ${options.parserMode}`,
        `Wrote: ${result.topCtrPath}`,
        `Wrote: ${result.topCpaPath}`,
      ].join("\n") + "\n",
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`${message}\n`);
    process.exitCode = 1;
  }
}

if (require.main === module) {
  void main();
}
