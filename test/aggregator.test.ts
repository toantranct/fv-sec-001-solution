import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  accumulateCampaignRow,
  aggregateCampaigns,
  rankTopCpa,
  rankTopCtr,
  toCampaignMetrics,
  type CampaignTotals,
} from "../src/aggregator";
import { parseArgs, run } from "../src/cli";
import { streamValidatedRows, validateHeader, validateRecord } from "../src/csv";
import { formatMetricsRow } from "../src/output";

function createCampaign(campaignId: string, impressions: number, clicks: number, spend: number, conversions: number): CampaignTotals {
  return {
    campaignId,
    totalImpressions: impressions,
    totalClicks: clicks,
    totalSpend: spend,
    totalConversions: conversions,
  };
}

test("validateHeader accepts the expected schema", () => {
  assert.doesNotThrow(() =>
    validateHeader(["campaign_id", "date", "impressions", "clicks", "spend", "conversions"]),
  );
});

test("validateRecord rejects malformed rows", () => {
  assert.equal(
    validateRecord(["CMP001", "2025-01-01", "100", "bad", "10.5", "1"]),
    null,
  );
  assert.equal(
    validateRecord(["", "2025-01-01", "100", "10", "10.5", "1"]),
    null,
  );
  assert.equal(
    validateRecord(["CMP001", "2025-02-31", "100", "10", "10.5", "1"]),
    null,
  );
});

test("accumulateCampaignRow updates totals in place", () => {
  const campaigns = new Map<string, CampaignTotals>();
  accumulateCampaignRow(campaigns, {
    campaignId: "CMP001",
    date: "2025-01-01",
    impressions: 100,
    clicks: 10,
    spend: 12.5,
    conversions: 2,
  });
  accumulateCampaignRow(campaigns, {
    campaignId: "CMP001",
    date: "2025-01-02",
    impressions: 50,
    clicks: 5,
    spend: 7.5,
    conversions: 1,
  });

  assert.deepEqual(campaigns.get("CMP001"), {
    campaignId: "CMP001",
    totalImpressions: 150,
    totalClicks: 15,
    totalSpend: 20,
    totalConversions: 3,
  });
});

test("toCampaignMetrics computes ctr and null cpa", () => {
  assert.deepEqual(toCampaignMetrics(createCampaign("CMP001", 100, 20, 40, 0)), {
    campaignId: "CMP001",
    totalImpressions: 100,
    totalClicks: 20,
    totalSpend: 40,
    totalConversions: 0,
    ctr: 0.2,
    cpa: null,
  });
});

test("rankTopCtr uses descending ctr and campaign_id tie break", () => {
  const ranked = rankTopCtr(
    [
      createCampaign("CMP002", 100, 50, 10, 2),
      createCampaign("CMP001", 100, 50, 12, 2),
      createCampaign("CMP003", 100, 30, 8, 1),
      createCampaign("CMP004", 0, 0, 5, 1),
    ],
    2,
  );

  assert.deepEqual(
    ranked.map((campaign) => campaign.campaignId),
    ["CMP001", "CMP002"],
  );
});

test("rankTopCpa uses ascending cpa, excludes zero conversions, and applies tie break", () => {
  const ranked = rankTopCpa(
    [
      createCampaign("CMP003", 100, 20, 30, 3),
      createCampaign("CMP001", 100, 20, 20, 2),
      createCampaign("CMP002", 100, 20, 20, 0),
      createCampaign("CMP004", 100, 20, 20, 2),
    ],
    3,
  );

  assert.deepEqual(
    ranked.map((campaign) => campaign.campaignId),
    ["CMP001", "CMP003", "CMP004"],
  );
});

test("formatMetricsRow formats fixed precision and null metrics", () => {
  assert.equal(
    formatMetricsRow({
      campaignId: "CMP001",
      totalImpressions: 100,
      totalClicks: 5,
      totalSpend: 12,
      totalConversions: 0,
      ctr: 0.05,
      cpa: null,
    }),
    "CMP001,100,5,12.00,0,0.0500,null",
  );
});

test("aggregateCampaigns streams data and skips invalid rows safely", async () => {
  const tempDir = await mkdtemp(join(tmpdir(), "fv-sec-001-test-"));

  try {
    const inputPath = join(tempDir, "input.csv");
    await writeFile(
      inputPath,
      [
        "campaign_id,date,impressions,clicks,spend,conversions",
        "CMP001,2025-01-01,100,10,20.50,2",
        "CMP001,2025-01-02,150,15,29.50,3",
        "CMP002,2025-01-01,50,5,10.00,0",
        "CMP003,2025-01-01,broken,5,10.00,1",
        "CMP004,2025-02-30,10,1,1.00,1",
        "CMP005,2025-01-01,10,1,1.00",
      ].join("\n"),
      "utf8",
    );

    const result = await aggregateCampaigns(inputPath);

    assert.equal(result.totalRows, 6);
    assert.equal(result.validRows, 3);
    assert.equal(result.skippedRows, 3);
    assert.equal(result.campaignCount, 2);
    assert.deepEqual(result.campaigns.get("CMP001"), {
      campaignId: "CMP001",
      totalImpressions: 250,
      totalClicks: 25,
      totalSpend: 50,
      totalConversions: 5,
    });
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
});

test("csv-parse and readline parsers produce identical stats and rows", async () => {
  const tempDir = await mkdtemp(join(tmpdir(), "fv-sec-001-test-"));

  try {
    const inputPath = join(tempDir, "input.csv");
    await writeFile(
      inputPath,
      [
        "campaign_id,date,impressions,clicks,spend,conversions",
        "CMP001,2025-01-01,100,10,20.50,2",
        "CMP001,2025-01-02,150,15,29.50,3",
        "CMP002,2025-01-01,50,5,10.00,0",
        "CMP003,2025-01-01,broken,5,10.00,1",
        "CMP004,2025-02-30,10,1,1.00,1",
        "CMP005,2025-01-01,10,1,1.00",
      ].join("\n"),
      "utf8",
    );

    const csvParseRows: string[] = [];
    const readlineRows: string[] = [];

    const csvParseStats = await streamValidatedRows(
      inputPath,
      (row) => {
        csvParseRows.push(JSON.stringify(row));
      },
      "csv-parse",
    );
    const readlineStats = await streamValidatedRows(
      inputPath,
      (row) => {
        readlineRows.push(JSON.stringify(row));
      },
      "readline",
    );

    assert.deepEqual(readlineStats, csvParseStats);
    assert.deepEqual(readlineRows, csvParseRows);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
});

test("parseArgs validates required flags", () => {
  assert.throws(() => parseArgs([]), /Usage:/);

  assert.deepEqual(parseArgs(["--input", "data.csv", "--output", "results"]), {
    inputPath: join(process.cwd(), "data.csv"),
    outputDir: join(process.cwd(), "results"),
    parserMode: "csv-parse",
  });
});

test("parseArgs accepts parser override", () => {
  assert.deepEqual(parseArgs(["--input", "data.csv", "--output", "results", "--parser", "readline"]), {
    inputPath: join(process.cwd(), "data.csv"),
    outputDir: join(process.cwd(), "results"),
    parserMode: "readline",
  });
});

test("run writes both result files", async () => {
  const tempDir = await mkdtemp(join(tmpdir(), "fv-sec-001-test-"));

  try {
    const inputPath = join(tempDir, "input.csv");
    const outputDir = join(tempDir, "results");

    await writeFile(
      inputPath,
      [
        "campaign_id,date,impressions,clicks,spend,conversions",
        "CMP002,2025-01-01,100,20,30.00,3",
        "CMP001,2025-01-01,100,20,20.00,2",
        "CMP003,2025-01-01,100,10,5.00,0",
      ].join("\n"),
      "utf8",
    );

    const result = await run({ inputPath, outputDir, parserMode: "csv-parse" });
    const topCtr = await readFile(result.topCtrPath, "utf8");
    const topCpa = await readFile(result.topCpaPath, "utf8");

    assert.match(topCtr, /^campaign_id,total_impressions,total_clicks,total_spend,total_conversions,CTR,CPA/m);
    assert.match(topCtr, /CMP001,100,20,20.00,2,0.2000,10.00/);
    assert.match(topCtr, /CMP003,100,10,5.00,0,0.1000,null/);
    assert.match(topCpa, /CMP001,100,20,20.00,2,0.2000,10.00/);
    assert.doesNotMatch(topCpa, /CMP003/);
    assert.equal(result.totalRows, 3);
    assert.equal(result.validRows, 3);
    assert.equal(result.skippedRows, 0);
    assert.equal(result.campaignCount, 3);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
});

test("run writes identical results for readline parser", async () => {
  const tempDir = await mkdtemp(join(tmpdir(), "fv-sec-001-test-"));

  try {
    const inputPath = join(tempDir, "input.csv");
    const csvParseDir = join(tempDir, "csv-parse-results");
    const readlineDir = join(tempDir, "readline-results");

    await writeFile(
      inputPath,
      [
        "campaign_id,date,impressions,clicks,spend,conversions",
        "CMP002,2025-01-01,100,20,30.00,3",
        "CMP001,2025-01-01,100,20,20.00,2",
        "CMP003,2025-01-01,100,10,5.00,0",
      ].join("\n"),
      "utf8",
    );

    const csvParseResult = await run({ inputPath, outputDir: csvParseDir, parserMode: "csv-parse" });
    const readlineResult = await run({ inputPath, outputDir: readlineDir, parserMode: "readline" });

    assert.equal(await readFile(csvParseResult.topCtrPath, "utf8"), await readFile(readlineResult.topCtrPath, "utf8"));
    assert.equal(await readFile(csvParseResult.topCpaPath, "utf8"), await readFile(readlineResult.topCpaPath, "utf8"));
    assert.deepEqual(
      {
        totalRows: readlineResult.totalRows,
        validRows: readlineResult.validRows,
        skippedRows: readlineResult.skippedRows,
        campaignCount: readlineResult.campaignCount,
      },
      {
        totalRows: csvParseResult.totalRows,
        validRows: csvParseResult.validRows,
        skippedRows: csvParseResult.skippedRows,
        campaignCount: csvParseResult.campaignCount,
      },
    );
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
});
