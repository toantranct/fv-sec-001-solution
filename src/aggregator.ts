import { streamValidatedRows, type CsvProcessingStats, type ValidatedAdRow } from "./csv";

export interface CampaignTotals {
  campaignId: string;
  totalImpressions: number;
  totalClicks: number;
  totalSpend: number;
  totalConversions: number;
}

export interface CampaignMetrics extends CampaignTotals {
  ctr: number | null;
  cpa: number | null;
}

export interface AggregationResult extends CsvProcessingStats {
  campaignCount: number;
  campaigns: Map<string, CampaignTotals>;
}

function createCampaignTotals(campaignId: string): CampaignTotals {
  return {
    campaignId,
    totalImpressions: 0,
    totalClicks: 0,
    totalSpend: 0,
    totalConversions: 0,
  };
}

export function accumulateCampaignRow(
  campaigns: Map<string, CampaignTotals>,
  row: ValidatedAdRow,
): void {
  const totals = campaigns.get(row.campaignId) ?? createCampaignTotals(row.campaignId);

  totals.totalImpressions += row.impressions;
  totals.totalClicks += row.clicks;
  totals.totalSpend += row.spend;
  totals.totalConversions += row.conversions;

  campaigns.set(row.campaignId, totals);
}

export function toCampaignMetrics(totals: CampaignTotals): CampaignMetrics {
  const ctr =
    totals.totalImpressions === 0 ? null : totals.totalClicks / totals.totalImpressions;
  const cpa =
    totals.totalConversions === 0 ? null : totals.totalSpend / totals.totalConversions;

  return {
    ...totals,
    ctr,
    cpa,
  };
}

function isBetterCtr(candidate: CampaignMetrics, current: CampaignMetrics): boolean {
  const candidateCtr = candidate.ctr ?? Number.NEGATIVE_INFINITY;
  const currentCtr = current.ctr ?? Number.NEGATIVE_INFINITY;

  if (candidateCtr !== currentCtr) {
    return candidateCtr > currentCtr;
  }

  return candidate.campaignId.localeCompare(current.campaignId) < 0;
}

function isBetterCpa(candidate: CampaignMetrics, current: CampaignMetrics): boolean {
  const candidateCpa = candidate.cpa ?? Number.POSITIVE_INFINITY;
  const currentCpa = current.cpa ?? Number.POSITIVE_INFINITY;

  if (candidateCpa !== currentCpa) {
    return candidateCpa < currentCpa;
  }

  return candidate.campaignId.localeCompare(current.campaignId) < 0;
}

function insertTopCampaign(
  ranked: CampaignMetrics[],
  candidate: CampaignMetrics,
  limit: number,
  isBetter: (candidate: CampaignMetrics, current: CampaignMetrics) => boolean,
): void {
  for (let index = 0; index < ranked.length; index += 1) {
    const current = ranked[index];

    if (current && isBetter(candidate, current)) {
      ranked.splice(index, 0, candidate);

      if (ranked.length > limit) {
        ranked.length = limit;
      }

      return;
    }
  }

  if (ranked.length < limit) {
    ranked.push(candidate);
  }
}

export async function aggregateCampaigns(inputPath: string): Promise<AggregationResult> {
  const campaigns = new Map<string, CampaignTotals>();
  const stats = await streamValidatedRows(inputPath, (row) => {
    accumulateCampaignRow(campaigns, row);
  });

  return {
    ...stats,
    campaignCount: campaigns.size,
    campaigns,
  };
}

export function rankTopCtr(
  campaigns: Iterable<CampaignTotals>,
  limit = 10,
): CampaignMetrics[] {
  const ranked: CampaignMetrics[] = [];

  for (const campaign of campaigns) {
    const metrics = toCampaignMetrics(campaign);

    if (metrics.ctr === null) {
      continue;
    }

    insertTopCampaign(ranked, metrics, limit, isBetterCtr);
  }

  return ranked;
}

export function rankTopCpa(
  campaigns: Iterable<CampaignTotals>,
  limit = 10,
): CampaignMetrics[] {
  const ranked: CampaignMetrics[] = [];

  for (const campaign of campaigns) {
    const metrics = toCampaignMetrics(campaign);

    if (metrics.cpa === null) {
      continue;
    }

    insertTopCampaign(ranked, metrics, limit, isBetterCpa);
  }

  return ranked;
}
