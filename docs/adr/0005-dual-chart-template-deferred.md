# ADR 0005 — Dual-chart slide template deferred

**Status:** Deferred  
**Date:** 2026-06-01

## Context

The monthly report template bank includes a `dual_chart` template (two charts side by side on one slide). Every other template in the bank maps cleanly to the existing trend slide spec shape, which has a single `graph_spec` object. A dual-chart slide requires two independent graph specs on one slide.

## Decision

Defer the `dual_chart` template until the rest of the template bank is shipped and proven in production.

## Alternatives considered

**Option A — `graph_specs` array on dual_chart only:** When `template: "dual_chart"`, the spec carries `graph_specs: [spec1, spec2]`. All other templates keep the singular `graph_spec`. The renderer branches on template name.

**Option B — always an array:** Replace `graph_spec` with `graph_specs` everywhere. Single-chart templates use a one-element array. Consistent schema at the cost of verbosity and a breaking change to existing slide content JSON.

## Reason for deferral

Dual chart is the only template that changes the schema shape of a trend slide. Implementing it alongside the rest of the bank introduces unnecessary complexity before the template selection mechanism is validated. The two options above remain open — choose when implementing.
