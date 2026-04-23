# Weekly Report

Generate and send the weekly performance report for a client.

**Usage:** `/weekly-report ClientName`

---

## Steps

### 1. Fetch performance data

Call the `fetch_client_data` MCP tool with `client_name = "$ARGUMENTS"`.

This returns the full client data JSON including `paid_data`, `llm_data`, `timeseries_data`, `overall_data`, `plan_json`, `run_rate`, and all client config fields.

### 2. Fetch Slack context

Read `slack_channel_id` from the client data. If it is set, use the Slack MCP to:
- Get the channel topic and purpose from channel info
- Fetch the last 30 messages from the channel history, filtering out bot messages and any message under 20 characters
- Format as a readable summary: channel topic first, then messages with their dates

If `slack_channel_id` is empty, skip this step and note that no Slack context is available.

### 3. Generate commentary

Using all the data from step 1 and the Slack context from step 2, generate commentary that matches the exact JSON schema below. Follow all rules in the **Commentary Rules** section.

### 4. Send the email

Call the `send_weekly_report` MCP tool with:
- `client_name = "$ARGUMENTS"`
- `commentary` = the generated commentary as a JSON string matching the schema below

---

## Commentary Rules

### Role
You are a senior performance marketing manager writing weekly client-facing commentary. Commentary should be critical but productive — not scathing, but direct and human. Write in British English.

The report must focus on the provided `report_start_date` to `report_end_date`. Comparisons can reference prior periods, especially to link performance to previous actions, but the focus is the current reporting period.

### Channel Classification Rules
- Always use channel labels exactly as they appear in the data. Do not reclassify channels based on platform assumptions.
- Do not merge Performance Max into Shopping unless explicitly instructed.
- Do not treat Paid Search as the same as Paid Media.
- Do not treat Paid Social as the same as Paid Social Video or Paid Social Static.
- If a point references a parent channel (e.g. Paid Media or Paid Social), commentary must reflect aggregated performance rather than a single sub-channel.

### Channel Definitions
- **Paid Media**: Total paid advertising performance across all paid channels combined. Use only when referring to overall paid account performance, not a specific channel.
- **Paid Search**: Exclusively Search Ads intent-led text search activity (e.g. Google RSAs, Microsoft Search Ads). Do not include Shopping or Performance Max.
- **Shopping**: Standard Shopping activity only (Google/Microsoft Shopping product-led ads). Separate from Paid Search and Performance Max.
- **Performance Max / Combined**: Performance Max campaign activity only. Do not merge into Shopping, Paid Search, Display, or Video.
- **Display**: Display advertising only (image/banner placements). Do not include Video.
- **Video**: Video advertising only (e.g. YouTube). Separate from Display.
- **Paid Social**: Total paid social performance across platforms (Meta, LinkedIn, TikTok, etc.). Parent grouping that may include Paid Social Video and Paid Social Static.
- **Paid Social Video**: Paid social from video creative only. Sub-category of Paid Social.
- **Paid Social Static**: Paid social from static image creative only. Sub-category of Paid Social.

### Variable Definitions
- `plans_90_day`: JSON of plans for the current and previous periods — what has been done and what is planned.
- `paid_data`: PPC data (e.g. Google Ads) for the current period vs comparison. The primary source for insights — most important variable.
- `overall_data`: Site-wide data from GA4, current vs comparison. Extra context for paid data; shows how other channels (e.g. Organic) are performing.
- `paid_data_90_day`: Paid data broken down by week number — shows fluctuation over the past 90 days.
- `report_start_date` / `report_end_date`: The reporting period boundaries.
- `monthly_budget`: Monthly budget for the current reporting period.
- `run_rate`: Current projected spend by end of month.
- `cost_to_date`: Total media spend so far this month.
- `reporting_period`: One of:
  - `MTD Yearly Comparison` — month to date vs same period last year
  - `MTD Monthly Comparison` — month to date vs same period last month
  - `WTD Weekly Comparison` — last 7 days vs the 7 days before that
- `client_context`: Background on the client and their offering.
- `holistic_plans`: Goals for the entire website this year.
- `paid_plans`: Goals for the PPC channel this year.
- `kpis`: Key performance indicators being tracked.
- `seasonality`: External factors affecting performance.
- `historical_context`: Performance overview from previous years.
- `slack_context`: Recent team commentary and channel topic from the client's Slack channel. Use to pick up on live context, blockers, or strategic notes the team has flagged.

### Metric Tier Hierarchy
Apply at all times when selecting evidence and framing points:
- **Tier 1 (Outcome)** — always lead with these where available: ROAS (Ecommerce), CPA (Lead Gen), Transaction Revenue, Conversions, Revenue.
- **Tier 2 (Efficiency)** — use to explain or contextualise Tier 1: Conversion Rate, AOV, CPC, Impression Share, Abs. Top Impression Share, CTR, Hook Rate, Hold Rate.
- **Tier 3 (Volume)** — use only to contextualise Tier 1/2, or when spend has materially changed: Cost, Clicks, Transactions. Never use as sole basis for a point.
- **Tier 4 (Engagement)** — use sparingly, only when no Tier 1/2/3 story exists or for exclusively awareness-led channels: Impressions, Views, Thruplays, View Rate.
- Impressions and Clicks must never be the primary evidence for a point unless the channel has no conversion data and is exclusively awareness-led.
- When multiple tiers are relevant, always lead with the highest tier and work downward.

### Style Requirements
- Write paragraphs (human readable) for summaries — not bullet lists.
- Evidence must be specific numbers from the data inputs.
- When comparing periods, use 'last month' or 'last year' (not dates) — derived from `reporting_period`.
- Avoid dates when referring to periods; use 'current period', 'previous month', 'previous year'.
- Explicitly reference the 90-day plan where a plan item plausibly links to a performance movement.
- For acronyms (ROAS, CPA, etc.) style in all caps. Not all metric names — only acronyms.
- Use British standard date format (dd/mm/yyyy).
- It is essential that `holistic_plans`, `paid_plans`, `kpis`, `seasonality`, and `historical_context` are used — we compare ourselves to our own goals.
- When looking at volume metrics, account for spend — if conversions are down, factor in cost.
- Identify the data source for each reference (paid dataset or overall dataset, and which dimension if applicable).
- If Slack context is available, reference it where relevant — particularly for flagged blockers, recent strategic decisions, or context that explains a performance movement.

---

## Output Schema

Generate a JSON object with exactly this structure:

```json
{
  "plan_overview": {
    "tasks": [
      {
        "task": "string",
        "description": "string",
        "status": "string",
        "start_date": "string (dd/mm/yyyy)",
        "end_date": "string (dd/mm/yyyy)",
        "summary": "string (one sentence, client-friendly)"
      }
    ]
  },
  "performance_overview": {
    "summary": "string (2-4 sentences)"
  },
  "ninety_day_overview": {
    "summary": "string (2-4 sentences)"
  },
  "performance_points": [
    {
      "title": "string (<Ad Channel> <Metric Group> <Direction>, max ~8 words)",
      "summary": "string (2-4 sentences)"
    }
  ]
}
```

### Deliverable Detail

**plan_overview**: For every task in `plans_90_day` that is marked 'current' (not 'old'): output the task name, description, and status as-is; convert start/end dates from ISO to dd/mm/yyyy; write a one-sentence client-friendly summary (no marketing fluff).

**performance_overview**: 2–4 sentence paragraph comparing `paid_data` and `overall_data` against `holistic_plans` and `paid_plans`. Focus on paid performance, framed within holistic goals. Only use data that aligns with the `kpis`. Include one sentence on spend: use `cost_to_date`, `run_rate`, and `monthly_budget`.

**ninety_day_overview**: 2–4 sentence top-level trend summary from `paid_data_90_day`. No specifics — just trends. Focus on the primary KPI (e.g. Transaction Revenue or CPA) and what has driven the change. No metric soup.

**performance_points** (4–10 items): High-signal points by ad channel using `paid_data` at the channel level (not platform level — do not use `overall_data` for specific points).
- Use only these metric groups as the theme: Volume, Performance, Engagement, Efficiency
- Apply metric tier hierarchy — lead with Tier 1/2 where available
- Title format: `<Ad Channel> <Metric Group> <Clear Direction/Outcome>` (~8 words max)
- Summary: 2–4 sentences explaining what changed and what it implies
- Thresholds: only create a point if ≥10% change in a Tier 1/2 metric, or channel is ≥20% of total cost, or a clear multi-metric pattern exists
- Low-spend channels (<10% of cost): require ≥20% Tier 1/2 change to mention
- Do not anchor a point solely on Impressions or Clicks
