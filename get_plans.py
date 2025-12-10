import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import re
from datetime import datetime, timedelta
import numpy as np

pd.options.mode.chained_assignment = None  # default='warn'
np.seterr(divide='ignore', invalid='ignore')


def build_plan_json_from_sheet(client):
    """
    Build structured 90-day plan JSON from the 'Current' tab of the plan
    spreadsheet (same layout as your Q4 90-day plan).
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
    cfg = sh.worksheet("Current")
    values = cfg.get_all_values()
    df = pd.DataFrame(values)
    df.replace("", np.nan, inplace=True)

    # --- 1. Find header row with Task / Description / Status / Start Date / End Date ---
    header_row_candidates = df.index[df[1] == "Task"]
    if len(header_row_candidates) == 0:
        raise ValueError("Could not find a 'Task' header in column 1.")
    header_row_idx = int(header_row_candidates[0])
    header_row = df.loc[header_row_idx]

    # 2. Map core columns by header value
    col_map: dict = {}
    for col in df.columns:
        val = header_row[col]
        if isinstance(val, str):
            key = val.strip().lower()
            if key == "task":
                col_map["task"] = col
            elif key == "description":
                col_map["description"] = col
            elif key == "status":
                col_map["status"] = col
            elif key == "start date":
                col_map["start_date"] = col
            elif key == "end date":
                col_map["end_date"] = col
            elif key.startswith("month"):
                col_map.setdefault("months", []).append((col, val))

    required_keys = ["task", "description", "status", "start_date", "end_date"]
    missing = [k for k in required_keys if k not in col_map]
    if missing:
        raise ValueError(f"Missing expected columns in header row: {missing}")

    task_col = col_map["task"]
    desc_col = col_map["description"]
    status_col = col_map["status"]
    start_col = col_map["start_date"]
    end_col = col_map["end_date"]
    month_headers = sorted(col_map.get("months", []), key=lambda x: x[0])

    # --- 3. Timeframe: derive weeks from the "week dates" row (header row + 1) ---
    week_row_idx = header_row_idx + 1
    if week_row_idx not in df.index:
        raise ValueError("Sheet does not contain a week header row.")

    week_row = df.loc[week_row_idx]

    def is_date_like(v) -> bool:
        # Handle existing Timestamp / datetime, or strings
        if isinstance(v, (pd.Timestamp, datetime, np.datetime64)) and not pd.isna(v):
            return True
        if isinstance(v, str) and v.strip():
            try:
                # IMPORTANT: UK style – dayfirst=True
                pd.to_datetime(v, dayfirst=True)
                return True
            except Exception:
                return False
        return False

    # Only consider columns *after* the End Date column as potential weeks
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

    # Map columns to months via Month 1 / Month 2 / Month 3 header cells
    month_ranges = []  # (month_id, label, start_col, end_col)
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
        # Fallback: single month covering all weeks
        month_ranges.append(("m1", "Month 1", min(week_cols_raw), max(week_cols_raw)))

    def month_for_col(c):
        for month_id, label, start_c, end_c in month_ranges:
            if start_c <= c <= end_c:
                return month_id
        return None

    # Build week list, sorted by actual date (using dayfirst parsing)
    week_info = []
    for col in sorted(week_cols_raw):
        raw_val = week_row[col]
        w_start_ts = pd.to_datetime(raw_val, dayfirst=True, errors="coerce")
        if pd.isna(w_start_ts):
            continue
        week_info.append((w_start_ts, col))

    if not week_info:
        raise ValueError("No valid week dates could be parsed from the week header row.")

    week_info.sort(key=lambda x: x[0])  # chronological

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

    # Build months
    months = []
    for month_id, label, start_c, end_c in month_ranges:
        month_weeks = [w for w in weeks if w["month_id"] == month_id]
        if month_weeks:
            start_date = (
                min(pd.to_datetime(w["start_date"]) for w in month_weeks)
                .date()
                .isoformat()
            )
            end_date = (
                max(pd.to_datetime(w["end_date"]) for w in month_weeks)
                .date()
                .isoformat()
            )
        else:
            start_date = None
            end_date = None
        months.append(
            {
                "id": month_id,
                "label": label,
                "start_date": start_date,
                "end_date": end_date,
            }
        )

    plan_start = min(w["_start_ts"] for w in weeks).date().isoformat()
    plan_end = max(w["_end_ts"] for w in weeks).date().isoformat()

    # --- 4. Parse sections (channels / Reporting / etc.) and tasks ---
    sections: dict = {}  # name -> {"name": ..., "tasks": [...]}
    current_section = None

    for idx in range(week_row_idx, int(df.index.max()) + 1):
        if idx not in df.index:
            continue
        row = df.loc[idx]

        task_name_cell = row[task_col]
        desc_cell = row[desc_col]
        status_cell = row[status_col]
        start_val = row[start_col]
        end_val = row[end_col]

        # Skip the header row itself
        if idx == header_row_idx:
            continue

        # Skip fully empty rows
        if (
            pd.isna(task_name_cell)
            and pd.isna(desc_cell)
            and pd.isna(status_cell)
            and pd.isna(start_val)
            and pd.isna(end_val)
        ):
            continue

        # Section header: value in Task col, everything else empty
        if (
            pd.notna(task_name_cell)
            and pd.isna(desc_cell)
            and pd.isna(status_cell)
            and pd.isna(start_val)
            and pd.isna(end_val)
        ):
            section_name = str(task_name_cell).strip()
            current_section = sections.setdefault(
                section_name, {"name": section_name, "tasks": []}
            )
            continue

        # Task row
        if pd.notna(task_name_cell) and (
            pd.notna(desc_cell)
            or pd.notna(status_cell)
            or pd.notna(start_val)
            or pd.notna(end_val)
        ):
            if current_section is None:
                # No section header yet – ignore for now
                continue

            task_name = str(task_name_cell).strip()
            description = str(desc_cell).strip() if pd.notna(desc_cell) else None
            status = str(status_cell).strip() if pd.notna(status_cell) else None

            # IMPORTANT: dates in sheet are UK style – parse dayfirst
            start_ts = (
                pd.to_datetime(start_val, dayfirst=True, errors="coerce")
                if pd.notna(start_val)
                else None
            )
            end_ts = (
                pd.to_datetime(end_val, dayfirst=True, errors="coerce")
                if pd.notna(end_val)
                else None
            )

            if start_ts is not None and pd.isna(start_ts):
                start_ts = None
            if end_ts is not None and pd.isna(end_ts):
                end_ts = None

            start_date_iso = start_ts.date().isoformat() if start_ts is not None else None
            end_date_iso = end_ts.date().isoformat() if end_ts is not None else None

            # Which weeks does this task apply to?
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
                "status": status,
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
            channels.append(
                {
                    "name": name,
                    "category": channel_type_map[name],
                    "tasks": section["tasks"],
                }
            )
        elif name == "Reporting / Analysis":
            for t in section["tasks"]:
                n = (t["name"] or "").lower()
                if "tracking" in n or "audit" in n:
                    tracking_items.append(t)
                else:
                    reporting_items.append(t)
        else:
            # Any other sections can be folded into reporting for now
            reporting_items.extend(section["tasks"])

    # --- 6. Status definitions ---
    all_tasks = []
    for ch in channels:
        all_tasks.extend(ch["tasks"])
    all_tasks.extend(reporting_items)
    all_tasks.extend(tracking_items)

    unique_statuses = sorted({t["status"] for t in all_tasks if t.get("status")})
    status_desc_map = {
        "BAU": "Business-as-usual activity that runs continuously within the plan period.",
        "Active Workstream": "Higher-focus initiative currently being worked on.",
        "Awaiting Client Approval": "Ready to go but blocked pending client sign-off.",
    }
    status_definitions = [
        {
            "status": s,
            "description": status_desc_map.get(s, ""),
        }
        for s in unique_statuses
    ]

    # --- 7. Final assembly ---
    weeks_clean = [
        {
            "id": w["id"],
            "label": w["label"],
            "start_date": w["start_date"],
            "end_date": w["end_date"],
            "month_id": w["month_id"],
        }
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
        "timeframe": {
            "months": months,
            "weeks": weeks_clean,
        },
        "status_definitions": status_definitions,
        "channels": channels,
        "reporting_and_analysis": {
            "items": reporting_items,
        },
        "tracking_and_infra": {
            "items": tracking_items,
        },
    }
    return final_json


def slugify(text: str) -> str:
    """Simple slug for stable task/channel IDs."""
    if not isinstance(text, str):
        return ""
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text
