import { createReadStream } from "node:fs";

import { parse } from "csv-parse";

export interface ValidatedAdRow {
  campaignId: string;
  date: string;
  impressions: number;
  clicks: number;
  spend: number;
  conversions: number;
}

export interface CsvProcessingStats {
  totalRows: number;
  validRows: number;
  skippedRows: number;
}

const EXPECTED_HEADER = [
  "campaign_id",
  "date",
  "impressions",
  "clicks",
  "spend",
  "conversions",
] as const;

function normalizeField(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function isValidDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false;
  }

  const [yearRaw, monthRaw, dayRaw] = value.split("-");
  const year = Number(yearRaw);
  const month = Number(monthRaw);
  const day = Number(dayRaw);
  const candidate = new Date(Date.UTC(year, month - 1, day));

  return (
    candidate.getUTCFullYear() === year &&
    candidate.getUTCMonth() === month - 1 &&
    candidate.getUTCDate() === day
  );
}

function parseNonNegativeInteger(value: string): number | null {
  if (!/^\d+$/.test(value)) {
    return null;
  }

  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function parseNonNegativeNumber(value: string): number | null {
  if (!/^\d+(?:\.\d+)?$/.test(value)) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function validateHeader(columns: string[]): void {
  if (columns.length !== EXPECTED_HEADER.length) {
    throw new Error(
      `Invalid header: expected ${EXPECTED_HEADER.length} columns, received ${columns.length}`,
    );
  }

  for (let index = 0; index < EXPECTED_HEADER.length; index += 1) {
    if (columns[index] !== EXPECTED_HEADER[index]) {
      throw new Error(
        `Invalid header: expected "${EXPECTED_HEADER.join(",")}", received "${columns.join(",")}"`,
      );
    }
  }
}

export function validateRecord(columns: string[]): ValidatedAdRow | null {
  if (columns.length !== EXPECTED_HEADER.length) {
    return null;
  }

  const campaignId = normalizeField(columns[0]);
  const date = normalizeField(columns[1]);
  const impressions = parseNonNegativeInteger(normalizeField(columns[2]));
  const clicks = parseNonNegativeInteger(normalizeField(columns[3]));
  const spend = parseNonNegativeNumber(normalizeField(columns[4]));
  const conversions = parseNonNegativeInteger(normalizeField(columns[5]));

  if (!campaignId || !isValidDate(date)) {
    return null;
  }

  if (impressions === null || clicks === null || spend === null || conversions === null) {
    return null;
  }

  return {
    campaignId,
    date,
    impressions,
    clicks,
    spend,
    conversions,
  };
}

export async function streamValidatedRows(
  inputPath: string,
  onRow: (row: ValidatedAdRow) => void | Promise<void>,
): Promise<CsvProcessingStats> {
  const parser = createReadStream(inputPath, { encoding: "utf8" }).pipe(
    parse({
      bom: true,
      trim: true,
      skip_empty_lines: true,
      relax_column_count: true,
      skip_records_with_error: true,
    }),
  );

  let sawHeader = false;
  const stats: CsvProcessingStats = {
    totalRows: 0,
    validRows: 0,
    skippedRows: 0,
  };

  for await (const record of parser as AsyncIterable<string[]>) {
    if (!sawHeader) {
      validateHeader(record.map((value) => normalizeField(value)));
      sawHeader = true;
      continue;
    }

    stats.totalRows += 1;
    const row = validateRecord(record);

    if (row === null) {
      stats.skippedRows += 1;
      continue;
    }

    stats.validRows += 1;
    await onRow(row);
  }

  if (!sawHeader) {
    throw new Error("Input file is empty");
  }

  return stats;
}
