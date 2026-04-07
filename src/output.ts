import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

import type { CampaignMetrics } from "./aggregator";

export const OUTPUT_HEADER = [
  "campaign_id",
  "total_impressions",
  "total_clicks",
  "total_spend",
  "total_conversions",
  "CTR",
  "CPA",
].join(",");

export function formatMetricsRow(campaign: CampaignMetrics): string {
  return [
    campaign.campaignId,
    String(campaign.totalImpressions),
    String(campaign.totalClicks),
    campaign.totalSpend.toFixed(2),
    String(campaign.totalConversions),
    campaign.ctr === null ? "null" : campaign.ctr.toFixed(4),
    campaign.cpa === null ? "null" : campaign.cpa.toFixed(2),
  ].join(",");
}

export async function writeMetricsCsv(
  outputDir: string,
  fileName: string,
  campaigns: CampaignMetrics[],
): Promise<string> {
  await mkdir(outputDir, { recursive: true });

  const filePath = join(outputDir, fileName);
  const content = [OUTPUT_HEADER, ...campaigns.map((campaign) => formatMetricsRow(campaign))]
    .join("\n")
    .concat("\n");

  await writeFile(filePath, content, "utf8");
  return filePath;
}
