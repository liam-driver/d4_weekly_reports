import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
import json


def build_plan_json_from_sheet():
    # 0. Initialise the sheets
    with open("storage/secrets.json","r") as f:
        secrets = json.load(f)
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        secrets["google_service_account"], 
        scope
    )
    sa = gspread.authorize(creds)
    with open("storage/config.json", "r") as config_json:
        clients = json.load(config_json)
    plans = {}
    for client in clients:
        sh = sa.open_by_url(client["plan"])
        client_plans = {}
        # 1. Loop through each sheet
        for i, sheet in enumerate(sh.worksheets()):
            # 2. Initialise sheet
            values = sheet.get_all_values()
            df = pd.DataFrame(values)
            df.replace("", np.nan, inplace=True)
            weeks = get_weeks(df)
            # 3. 
            if i == 0:
                plan_type = "current"
            else:
                plan_type = "old"
            plan = {
                    "client_name": client["name"],
                    "plan_start": weeks[0].strftime("%d/%m/%y"),
                    "plan_end": (weeks[-1] + pd.Timedelta(days=5)).strftime("%d/%m/%y"),
                    "plan_status": plan_type,
                    "tasks": (get_tasks(df, plan_type))
                }
            client_plans[sheet.title] = plan
        plans[client["name"]]= client_plans
    with open("storage/plans.json", "w", encoding="utf-8") as f:
        json.dump(plans, f, ensure_ascii=False, indent=2)
    return 0

# Create a list of all the dates that are used in the sheet to get a start and end date for the overall plan
def get_weeks(df):
    date_row_candidates = df.iloc[3].tolist()
    dates_cleaned = (
        pd.to_datetime(date_row_candidates, dayfirst=True, errors="coerce")
            .dropna()
            .tolist() 
        )
    return(dates_cleaned)        

# Convert the google sheet tasks into a json object that can be added to the plans json
def get_tasks(df, plan_type):
    # Check for misconfigured headers
    header_row_candidates = df.index[df[1] == "Task"]
    if len(header_row_candidates) == 0:
        raise ValueError("Could not find a 'Task' header in column 1.")
    
    # Get the right range of DF
    df = df.iloc[2:, 1:7]
    df.columns = df.iloc[0].astype(str).str.strip()
    df = df.iloc[1:].reset_index(drop=True) 
    cat = df["Category"].fillna("").astype(str).str.strip()
    mask = (cat == "Active Workstream") | (cat == "")
    df = df.loc[mask]

    # Get the mask for current
    now = pd.Timestamp.now()
    now = now.replace(day=1)
    if now.day <= 5:
        now = now.replace(day=1) - pd.Timedelta(days=1)
    first_of_current_month = pd.to_datetime(now.replace(day=1).normalize(), dayfirst=True, errors="coerce", utc=True)
    df["Start Date"] = pd.to_datetime(df["Start Date"], dayfirst=True, errors="coerce", utc=True)
    df["End Date"] = pd.to_datetime(df["End Date"], dayfirst=True, errors="coerce", utc=True)
    if plan_type == 'current':
        end = df["End Date"]
        mask = (end >= first_of_current_month) | end.isna()
        df = df.loc[mask]

    # Create a list of tasks, each task is a json objected appended to the 'task' list
    tasks=[]
    for idx, row in df.iterrows():
        if pd.isna(row["Description"]):
            ad_platform = row["Task"]
            continue
        tasks.append(
            {
                "name": row["Task"],
                "desc": row["Description"],
                "category": row["Category"],
                "status": row["Status"],
                "start_date": row["Start Date"].strftime("%d/%m/%y"),
                "end_date": row["End Date"].strftime("%d/%m/%y"),
                "platform": ad_platform,
            }
        )
    return tasks

build_plan_json_from_sheet()