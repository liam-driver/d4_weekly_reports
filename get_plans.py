import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import re
from datetime import datetime, timedelta, date
import numpy as np

pd.options.mode.chained_assignment = None  # default='warn'
np.seterr(divide='ignore', invalid='ignore')


def slugify(text: str) -> str:
    """Simple slug for stable task/channel IDs."""
    if not isinstance(text, str):
        return ""
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _parse_iso(d: str):
    return date.fromisoformat(d) if d else None


def _task_in_month(task: dict, month_start: date, month_end: date) -> bool:
    """
    Include tasks where:
      - start_date is in the month, OR
      - end_date is in the month, OR
      - task spans across the whole month
    """
    ts = _parse_iso(task.get("start_date"))
    te = _parse_iso(task.get("end_date"))
    if ts is None or te is None:
        return False  # safest default for undated tasks

    starts_in_month = month_start <= ts <= month_end
    ends_in_month = month_start <= te <= month_end
    spans_month = ts < month_start and te > month_end

    return starts_in_month or ends_in_month or spans_month


def _filter_plans_to_report_window(plans_by_tab: dict, report_start: str, report_end: str) -> dict:
    """
    Filters plans (keyed by tab) down to tasks relevant to the *current month*.
    "Current month" is derived from report_end (so weekly reports still map to the correct month).

    A task is included if it starts in the month, ends in the month, or spans the whole month.
    """
    def _parse_report_date(d: str) -> date:
        """
        Accepts either ISO (YYYY-MM-DD) or UK (DD/MM/YYYY).
        """
        if not d:
            raise ValueError("Empty report date provided.")
        d = str(d).strip()
        # ISO
        if "-" in d:
            return date.fromisoformat(d)
        # UK
        if "/" in d:
            return datetime.strptime(d, "%d/%m/%Y").date()
        raise ValueError(f"Unrecognised date format: {d}")

    # We only need report_end to determine the month we’re filtering to
    re_ = _parse_report_date(report_end)

    # Month boundaries for the month containing report_end
    month_start = date(re_.year, re_.month, 1)
    if re_.month == 12:
        next_month_start = date(re_.year + 1, 1, 1)
    else:
        next_month_start = date(re_.year, re_.month + 1, 1)
    month_end = next_month_start - timedelta(days=1)

    filtered_by_tab = {}

    for tab_name, plan in plans_by_tab.items():
        # Filter channels
        filtered_channels = []
        for ch in plan.get("channels", []):
            tasks = [t for t in ch.get("tasks", []) if _task_in_month(t, month_start, month_end)]
            if tasks:
                filtered_channels.append({**ch, "tasks": tasks})

        # Filter reporting/tracking
        reporting_items = [
            t for t in plan.get("reporting_and_analysis", {}).get("items", [])
            if _task_in_month(t, month_start, month_end)
        ]
        tracking_items = [
            t for t in plan.get("tracking_and_infra", {}).get("items", [])
            if _task_in_month(t, month_start, month_end)
        ]

        # Skip empty tabs
        if not filtered_channels and not reporting_items and not tracking_items:
            continue

        # Rebuild definitions based on what’s left
        all_tasks = []
        for ch in filtered_channels:
            all_tasks.extend(ch.get("tasks", []))
        all_tasks.extend(reporting_items)
        all_tasks.extend(tracking_items)

        unique_categories = sorted({t.get("category") for t in all_tasks if t.get("category")})
        unique_statuses = sorted({t.get("status") for t in all_tasks if t.get("status")})

        category_desc_map = {
            "BAU": "Business-as-usual activity that runs continuously within the plan period.",
            "Active Workstream": "Higher-focus initiative currently being worked on.",
            "Awaiting Client Approval": "Ready to go but blocked pending client sign-off.",
        }
        status_desc_map = {
            "Scheduled": "Planned work that is queued and not yet in progress (or not yet completed).",
            "Blocked": "Work that cannot proceed due to a dependency or issue.",
            "Completed": "Work finished within the reporting period or recently.",
        }

        filtered_plan = {
            **plan,
            "channels": filtered_channels,
            "reporting_and_analysis": {"items": reporting_items},
            "tracking_and_infra": {"items": tracking_items},
            "category_definitions": [
                {"category": c, "description": category_desc_map.get(c, "")} for c in unique_categories
            ],
            "status_definitions": [
                {"status": s, "description": status_desc_map.get(s, "")} for s in unique_statuses
            ],
        }

        filtered_by_tab[tab_name] = filtered_plan

    return filtered_by_tab



def build_plan_json_from_sheet(client: dict) -> dict:
    """
    Build structured 90-day plan JSON from each tab of the plan spreadsheet,
    left-to-right, keyed by the sheet tab name.

    Sheet columns expected:
      - Task
      - Description
      - Category  (old "status": BAU / Active Workstream / Awaiting Client Approval)
      - Status    (new execution status: Scheduled / Blocked / Completed)
      - Start Date
      - End Date

    Returns:
      {
        "full": { "<tab>": <plan_json>, ... },
        "report_window": { "<tab>": <filtered_plan_json>, ... }
      }
    """
    # --- 0. Load sheet ---
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
    sa = gspread.authorize(creds)

    sh = sa.open_by_url(client["plan"])

    plans_by_tab = {}

    for cfg in sh.worksheets()[:2]:  # left-to-right tab order
        try:
            values = cfg.get_all_values()
            df = pd.DataFrame(values)
            df.replace("", np.nan, inplace=True)

            # --- 1. Find header row with Task / Description / Category / Status / Start Date / End Date ---
            header_row_candidates = df.index[df[1] == "Task"]
            if len(header_row_candidates) == 0:
                raise ValueError("Could not find a 'Task' header in column 1.")
            header_row_idx = int(header_row_candidates[0])
            header_row = df.loc[header_row_idx]

            # --- 2. Map columns by header values ---
            col_map: dict = {}
            for col in df.columns:
                val = header_row[col]
                if isinstance(val, str):
                    key = val.strip().lower()
                    if key == "task":
                        col_map["task"] = col
                    elif key == "description":
                        col_map["description"] = col
                    elif key == "category":
                        col_map["category"] = col
                    elif key == "status":
                        col_map["status"] = col
                    elif key == "start date":
                        col_map["start_date"] = col
                    elif key == "end date":
                        col_map["end_date"] = col
                    elif key.startswith("month"):
                        col_map.setdefault("months", []).append((col, val))

            required_keys = ["task", "description", "category", "status", "start_date", "end_date"]
            missing = [k for k in required_keys if k not in col_map]
            if missing:
                raise ValueError(f"Missing expected columns in header row: {missing}")

            task_col = col_map["task"]
            desc_col = col_map["description"]
            category_col = col_map["category"]
            status_col = col_map["status"]
            start_col = col_map["start_date"]
            end_col = col_map["end_date"]
            month_headers = sorted(col_map.get("months", []), key=lambda x: x[0])

            # --- 3. Timeframe: derive weeks from the row after header ---
            week_row_idx = header_row_idx + 1
            if week_row_idx not in df.index:
                raise ValueError("Sheet does not contain a week header row.")
            week_row = df.loc[week_row_idx]

            def is_date_like(v) -> bool:
                if isinstance(v, (pd.Timestamp, datetime, np.datetime64)) and not pd.isna(v):
                    return True
                if isinstance(v, str) and v.strip():
                    try:
                        pd.to_datetime(v, dayfirst=True)
                        return True
                    except Exception:
                        return False
                return False

            week_cols_raw = []
            for col in df.columns:
                if col <= end_col:
                    continue
                val = week_row[col]
                if is_date_like(val):
                    week_cols_raw.append(col)

            if not week_cols_raw:
                raise ValueError(
                    "Could not find any week date columns in the row after the header "
                    "(to the right of 'End Date')."
                )

            month_ranges = []
            if month_headers:
                for i, (col, label) in enumerate(month_headers):
                    start_c = col
                    if i + 1 < len(month_headers):
                        end_c = month_headers[i + 1][0] - 1
                    else:
                        end_c = max(week_cols_raw)
                    month_id = f"m{i+1}"
                    month_ranges.append((month_id, label, start_c, end_c))
            else:
                month_ranges.append(("m1", "Month 1", min(week_cols_raw), max(week_cols_raw)))

            def month_for_col(c):
                for month_id, label, start_c, end_c in month_ranges:
                    if start_c <= c <= end_c:
                        return month_id
                return None

            week_info = []
            for col in sorted(week_cols_raw):
                raw_val = week_row[col]
                w_start_ts = pd.to_datetime(raw_val, dayfirst=True, errors="coerce")
                if pd.isna(w_start_ts):
                    continue
                week_info.append((w_start_ts, col))

            if not week_info:
                raise ValueError("No valid week dates could be parsed from the week header row.")

            week_info.sort(key=lambda x: x[0])

            weeks = []
            for i, (w_start_ts, col) in enumerate(week_info, start=1):
                w_end_ts = w_start_ts + timedelta(days=6)
                month_id = month_for_col(col)
                weeks.append(
                    {
                        "id": f"w{i}",
                        "label": f"Week of {w_start_ts.date().isoformat()}",
                        "start_date": w_start_ts.date().isoformat(),
                        "end_date": w_end_ts.date().isoformat(),
                        "month_id": month_id,
                        "_start_ts": w_start_ts,
                        "_end_ts": w_end_ts,
                        "_col": col,
                    }
                )

            months = []
            for month_id, label, start_c, end_c in month_ranges:
                month_weeks = [w for w in weeks if w["month_id"] == month_id]
                if month_weeks:
                    start_date = min(pd.to_datetime(w["start_date"]) for w in month_weeks).date().isoformat()
                    end_date = max(pd.to_datetime(w["end_date"]) for w in month_weeks).date().isoformat()
                else:
                    start_date = None
                    end_date = None
                months.append({"id": month_id, "label": label, "start_date": start_date, "end_date": end_date})

            plan_start = min(w["_start_ts"] for w in weeks).date().isoformat()
            plan_end = max(w["_end_ts"] for w in weeks).date().isoformat()

            # --- 4. Parse sections and tasks ---
            sections: dict = {}
            current_section = None

            for idx in range(week_row_idx, int(df.index.max()) + 1):
                if idx not in df.index:
                    continue
                row = df.loc[idx]

                task_name_cell = row[task_col]
                desc_cell = row[desc_col]
                category_cell = row[category_col]
                status_cell = row[status_col]
                start_val = row[start_col]
                end_val = row[end_col]

                # Skip header row
                if idx == header_row_idx:
                    continue

                # Skip fully empty rows
                if (
                    pd.isna(task_name_cell)
                    and pd.isna(desc_cell)
                    and pd.isna(category_cell)
                    and pd.isna(status_cell)
                    and pd.isna(start_val)
                    and pd.isna(end_val)
                ):
                    continue

                # Section header: value in Task col, everything else empty
                if (
                    pd.notna(task_name_cell)
                    and pd.isna(desc_cell)
                    and pd.isna(category_cell)
                    and pd.isna(status_cell)
                    and pd.isna(start_val)
                    and pd.isna(end_val)
                ):
                    section_name = str(task_name_cell).strip()
                    current_section = sections.setdefault(section_name, {"name": section_name, "tasks": []})
                    continue

                # Task row
                if pd.notna(task_name_cell) and (
                    pd.notna(desc_cell)
                    or pd.notna(category_cell)
                    or pd.notna(status_cell)
                    or pd.notna(start_val)
                    or pd.notna(end_val)
                ):
                    if current_section is None:
                        continue

                    task_name = str(task_name_cell).strip()
                    description = str(desc_cell).strip() if pd.notna(desc_cell) else None

                    # Category (old status): bau/active workstream/awaiting client approval
                    category = str(category_cell).strip() if pd.notna(category_cell) else None

                    # Status (new execution): scheduled/blocked/completed
                    status = str(status_cell).strip() if pd.notna(status_cell) else None

                    # Keep only Active Workstream tasks (based on CATEGORY)
                    if (category or "").strip().lower() != "active workstream":
                        continue

                    start_ts = pd.to_datetime(start_val, dayfirst=True, errors="coerce") if pd.notna(start_val) else None
                    end_ts = pd.to_datetime(end_val, dayfirst=True, errors="coerce") if pd.notna(end_val) else None

                    if start_ts is not None and pd.isna(start_ts):
                        start_ts = None
                    if end_ts is not None and pd.isna(end_ts):
                        end_ts = None

                    start_date_iso = start_ts.date().isoformat() if start_ts is not None else None
                    end_date_iso = end_ts.date().isoformat() if end_ts is not None else None

                    applies_weeks = []
                    if start_ts is not None and end_ts is not None:
                        for w in weeks:
                            if w["_end_ts"] >= start_ts and w["_start_ts"] <= end_ts:
                                applies_weeks.append(w["id"])

                    section_slug = slugify(current_section["name"])
                    task_slug = slugify(task_name)
                    task_id = (
                        f"{section_slug}_{task_slug}"
                        if section_slug and task_slug
                        else task_slug or section_slug
                    )

                    task_obj = {
                        "id": task_id,
                        "name": task_name,
                        "description": description,
                        "status": status,       # Scheduled / Blocked / Completed
                        "category": category,   # BAU / Active Workstream / Awaiting Client Approval
                        "start_date": start_date_iso,
                        "end_date": end_date_iso,
                        "applies_weeks": applies_weeks,
                        "applies_during_plan": bool(applies_weeks),
                        "notes": None,
                        "tags": [],
                    }
                    current_section["tasks"].append(task_obj)

            # --- 5. Split into channels vs reporting/tracking ---
            channel_type_map = {
                "Google Ads": "Paid Search",
                "Microsoft Ads (Bing)": "Paid Search",
                "Meta Ads (Facebook/Instagram)": "Paid Social",
                "TikTok Ads": "Paid Social",
                "YouTube Ads": "Video",
            }

            channels = []
            reporting_items = []
            tracking_items = []

            for name, section in sections.items():
                if name in channel_type_map:
                    channels.append({"name": name, "category": channel_type_map[name], "tasks": section["tasks"]})
                elif name == "Reporting / Analysis":
                    for t in section["tasks"]:
                        n = (t["name"] or "").lower()
                        if "tracking" in n or "audit" in n:
                            tracking_items.append(t)
                        else:
                            reporting_items.append(t)
                else:
                    reporting_items.extend(section["tasks"])

            # --- 6. Definitions ---
            all_tasks = []
            for ch in channels:
                all_tasks.extend(ch["tasks"])
            all_tasks.extend(reporting_items)
            all_tasks.extend(tracking_items)

            unique_categories = sorted({t.get("category") for t in all_tasks if t.get("category")})
            unique_statuses = sorted({t.get("status") for t in all_tasks if t.get("status")})

            category_desc_map = {
                "BAU": "Business-as-usual activity that runs continuously within the plan period.",
                "Active Workstream": "Higher-focus initiative currently being worked on.",
                "Awaiting Client Approval": "Ready to go but blocked pending client sign-off.",
            }
            status_desc_map = {
                "Scheduled": "Planned work that is queued and not yet in progress (or not yet completed).",
                "Blocked": "Work that cannot proceed due to a dependency or issue.",
                "Completed": "Work finished within the reporting period or recently.",
            }

            category_definitions = [
                {"category": c, "description": category_desc_map.get(c, "")} for c in unique_categories
            ]
            status_definitions = [
                {"status": s, "description": status_desc_map.get(s, "")} for s in unique_statuses
            ]

            # --- 7. Final assembly ---
            weeks_clean = [
                {"id": w["id"], "label": w["label"], "start_date": w["start_date"], "end_date": w["end_date"], "month_id": w["month_id"]}
                for w in weeks
            ]

            start_dt = datetime.fromisoformat(plan_start)
            end_dt = datetime.fromisoformat(plan_end)
            period_label = f"{start_dt.strftime('%b %Y')} – {end_dt.strftime('%b %Y')}"

            plan_meta = {
                "client_name": client["name"],
                "plan_name": "90 Day Plan",
                "period_label": period_label,
                "plan_start": plan_start,
                "plan_end": plan_end,
                "generated_at": datetime.utcnow().isoformat() + "Z",
            }

            final_json = {
                "plan_meta": plan_meta,
                "timeframe": {"months": months, "weeks": weeks_clean},
                "category_definitions": category_definitions,
                "status_definitions": status_definitions,
                "channels": channels,
                "reporting_and_analysis": {"items": reporting_items},
                "tracking_and_infra": {"items": tracking_items},
            }

            plans_by_tab[cfg.title] = final_json

        except Exception as e:
            raise ValueError(f"Tab '{cfg.title}' failed to parse: {e}") from e

    # Build the filtered “report window” version (odd ranges supported)
    report_start = client.get("start_date_string")
    report_end = client.get("end_date_string")
    if not report_start or not report_end:
        raise ValueError("Client is missing start_date_string / end_date_string for report window filtering.")

    plans_report_window = _filter_plans_to_report_window(plans_by_tab, report_start, report_end)

    return {
        "full": plans_by_tab,
        "report_window": plans_report_window,
    }
