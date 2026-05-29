# ADR 0004 — Native python-pptx Shapes for Gantt Charts

**Status:** Accepted  
**Date:** 2026-05-29

## Context

The Gantt slide (`slide_planning_gantt`) was originally rendered by `_render_gantt_chart`, which used matplotlib `barh` to produce a PNG image that was then embedded in the PowerPoint slide. The output looked visually out of place — the PNG rendering produced a chart-style figure with axis ticks, gridlines, and a legend box that felt inconsistent with the rest of the deck's native shape-based design.

The company's established pattern for Gantt charts (carried over from the Google Sheets version) is a clean grid of native shapes: dark navy header cells per month, dark navy label boxes on the left, and coloured bars positioned proportionally within the grid. This is how the chart appeared in client-facing decks before the monthly report system was built.

## Decision

Replace the matplotlib PNG approach with a native python-pptx shapes implementation. `slide_planning_gantt` now draws all Gantt elements directly onto the slide using `add_shape` (rectangles), `add_textbox`, and text frame styling. No intermediate image file is written.

The layout:
- Month column headers derived from the actual date span of the tasks (no hardcoded quarter)
- Left label panel (`2.5"`) showing `"{platform}: {name}"` in dark navy
- Chart area bars coloured by status (Complete=teal, In Progress=gold, Scheduled=grey, Blocked=orange)
- Row height computed dynamically from available slide height
- Status legend centred below the rows

## Consequences

- **Crisp output:** Shapes render as resolution-independent vectors inside the PPTX — no PNG scaling artefacts.
- **No filesystem side-effect:** The old approach wrote `charts/{title}_gantt.png`. The new approach writes nothing to disk.
- **matplotlib still required:** `generate_visualisation.py` uses matplotlib for all other graph types. The import was removed from `generate_ppt.py` only.
- **Harder to add non-rectangular elements:** Features like curved bars or gradient fills would require lower-level XML manipulation. This trade-off is acceptable — the design is deliberately simple.
