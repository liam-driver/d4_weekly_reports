import pandas as pd
import numpy as np
import json
import locale
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from safe_div import safe_div
from pandas.tseries.offsets import MonthEnd


# Main workflow
def get_funnel_data(client):
    df = initialise_df(client)
    df = apply_filters(df, client)
    df = add_secondary_metrics(df,client)
    df = pivot_df(df,client)
    final_data = df_to_json(df,client)
    # with open("storage/funnel_data.json", "w", encoding="utf-8") as f:
    #     json.dump(final_data, f, ensure_ascii=False, indent=2)
    client['funnel_data'] = final_data
    client['run_rate'] = get_run_rate(client)
    return client

def get_run_rate(client):
    now = pd.Timestamp.now()
    yday = (now - pd.DateOffset(days=2)).normalize()
    spend = float(client['funnel_data']['Total']['Cost']['curr'].replace('£', '').replace(',', '').strip())
    day_diff_yday = ((yday - client['start_date']).days)
    day_diff_month = ((client['end_date'] - client['start_date']).days) + 1
    run_rate = (spend / day_diff_yday) * day_diff_month
    run_rate = locale.currency(run_rate, grouping=True)
    return(run_rate)

# Initialise the dataframe
def initialise_df(client):
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('storage/creds.json', scope)
    sa = gspread.authorize(creds)
    # Initial Config -- Declare Global Variables and initialise datasets (filename=serene-lotus-379510-b3f9b3b23758)
    sh = sa.open('Weekly Reports')
    locale.setlocale(locale.LC_ALL, 'en_GB.UTF-8')
    client['report_type'] = 'normal'
    ws = sh.worksheet(f"{client['name']} Funnel Import")
    df = pd.DataFrame(ws.get_all_records())
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    return df

# Mask the dataframes so that they are within the correct date range
def apply_filters(df, client):
    # Initialise dates
    now = pd.Timestamp.now()
    yday = (now - pd.DateOffset(days=2)).normalize()
    
    # Create masks for date periods
    if client['comparison_dates'] == 'MTD Yearly Comparison':
        start_date = client['start_date']
        end_date = yday
        compare_start_date = (start_date - pd.DateOffset(years=1)).normalize()
        compare_end_date = (end_date - pd.DateOffset(years=1)).normalize()
        if now.day <= 5:
            end_date = start_date - MonthEnd(0)
            compare_start_date = (start_date - pd.DateOffset(years=1)).normalize()
            compare_end_date = (end_date - pd.DateOffset(years=1)).normalize()
    
    if client['comparison_dates'] == 'MTD Monthly Comparison':
        start_date = client['start_date']
        end_date = yday
        compare_start_date = (start_date - pd.DateOffset(months=1)).normalize()
        compare_end_date = (end_date - pd.DateOffset(months=1)).normalize()
        if now.day <= 5:
            end_date = start_date - MonthEnd(0)
            compare_start_date = (start_date - pd.DateOffset(months=1)).normalize()
            compare_end_date = (end_date - pd.DateOffset(months=1)).normalize()

    if client['comparison_dates'] == 'WTD Weekly Comparison':
        end_date = yday
        compare_start_date = (start_date - pd.DateOffset(days=7)).normalize()
        compare_end_date = (end_date - pd.DateOffset(days=7)).normalize()
    
    # Apply Date Mask
    mask = ((df[client['dimension']]!='') & (((df['Date'] >= start_date) & (df['Date'] <= end_date)) | (df['Date'] >= compare_start_date) & (df['Date'] <= compare_end_date)))
    df = df.loc[mask]

    # Add in helper columns to categorise date periods
    df.loc[df['Date'] >= start_date, 'Period'] = 'Current'
    df.loc[df['Date'] <= start_date, 'Period'] = 'Previous'
    return (df)

# Add in secondary metrics to the dataframe, such as ROAS
def add_secondary_metrics(df, client):
    headers = list(df.columns.values)
    if client['account_type'] == 'Ecommerce':
        # Group, filter and clean Dataframes
        headers = ['Period', client['dimension'], headers[9], headers[10], headers[11], headers[12], headers[13]]
        numeric_headers = headers[2:]
        df_grouped = df[headers].copy()
        df_grouped[numeric_headers] = df_grouped[numeric_headers].apply(pd.to_numeric, errors="coerce")
        df_grouped = df_grouped.groupby(['Period', client['dimension']], as_index=False).sum()
        df_grouped = df_grouped.rename(columns={"Cost (GBP)": 'Cost','Transaction Revenue (GBP)': 'Transaction Revenue'})
        
        # Add a total row for each period
        curr_df = get_total_row(df_grouped[df_grouped["Period"].eq("Current")].copy(), "Current")
        prev_df = get_total_row(df_grouped[df_grouped["Period"].eq("Previous")].copy(), "Previous")
        df_grouped = pd.concat([curr_df, prev_df], ignore_index=True)
        
        # Standardise Column Names
        df_grouped = df_grouped.rename(columns={
            numeric_headers[0]: 'Impressions',
            numeric_headers[1]: 'Clicks',
            numeric_headers[2]: 'Cost',
            numeric_headers[3]: 'Transactions',
            numeric_headers[4]: 'Transaction Revenue'
            })

        # Get Secondary Metrics
        df_grouped['CTR'] = safe_div(
            df_grouped['Clicks'],
            df_grouped['Impressions'],
            multiplier = 100
        )
        df_grouped['CPC'] = safe_div(
            df_grouped['Cost'],
            df_grouped['Clicks'],
            multiplier = 1
        ) 
        df_grouped['Conversion Rate'] = safe_div(
            df_grouped['Transactions'],
            df_grouped['Clicks'],
            multiplier = 100
        )
        df_grouped['CPA'] = safe_div(
            df_grouped['Cost'],
            df_grouped['Transactions'],
            multiplier = 1
        )
        df_grouped['ROAS'] = safe_div(
            df_grouped['Transaction Revenue'],
            df_grouped['Cost'],
            multiplier = 100
        )

    if client['account_type'] == 'Lead Gen':
        # Group, filter and clean Dataframes
        headers = ['Period', client['dimension'], headers[9], headers[10], headers[11], headers[12]]
        numeric_headers = headers[2:]
        df_grouped = df[headers].copy()
        df_grouped[numeric_headers] = df_grouped[numeric_headers].apply(pd.to_numeric, errors="coerce")
        df_grouped = df_grouped.groupby(['Period', client['dimension']], as_index=False).sum()
        df_grouped = df_grouped.rename(columns={"Cost (GBP)": 'Cost'})

        # Add a total row for each period
        curr_df = get_total_row(df_grouped[df_grouped["Period"].eq("Current")].copy(), "Current")
        prev_df = get_total_row(df_grouped[df_grouped["Period"].eq("Previous")].copy(), "Previous")
        df_grouped = pd.concat([curr_df, prev_df], ignore_index=True)   
        
        # Standardise Column Names
        df_grouped = df_grouped.rename(columns={
            numeric_headers[0]: 'Impressions',
            numeric_headers[1]: 'Clicks',
            numeric_headers[2]: 'Cost',
            numeric_headers[3]: 'Conversions'
            })

        # Get Secondary Metrics
        df_grouped['CTR'] = safe_div(
            df_grouped['Clicks'],
            df_grouped['Impressions'],
            multiplier = 100
        )
        df_grouped['CPC'] = safe_div(
            df_grouped['Cost'],
            df_grouped['Clicks'],
            multiplier = 1
        ) 
        df_grouped['Conversion Rate'] = safe_div(
            df_grouped['Conversions'],
            df_grouped['Clicks'],
            multiplier = 100
        )
        df_grouped['CPA'] = safe_div(
            df_grouped['Cost'],
            df_grouped['Conversions'],
            multiplier = 1
        )
    return(df_grouped)

# Create total rows for the filtered down data
def get_total_row(df, period_label, dim_cols=2):
    # Initialise the total row as a dictionary
    total_row = {
        df.columns[0]: period_label,
        df.columns[1]: "Total",
        **df.iloc[:, dim_cols:].sum(numeric_only=True).to_dict()
    }
    # Return the dicitonary concated on the end of the dataframe
    return pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)

# Create a pivoted version of the data
def pivot_df(df_grouped, client):
    # Set Headers
    metrics = list(df_grouped.columns.values)
    metrics = metrics[2:]

    df_pivot = (
        df_grouped.pivot(index=client['dimension'], columns="Period", values=metrics)
        .reindex(columns=pd.MultiIndex.from_product([metrics, ["Current", "Previous"]]))  # ensures both exist
    )
    
    df_pivot.columns = [f"{metric}__{period.lower()}" for metric, period in df_pivot.columns]
    df_pivot = df_pivot.reset_index()

    for metric in metrics:
        df_pivot[f"{metric}__delta"] = df_pivot[f"{metric}__current"] - df_pivot[f"{metric}__previous"] 
        df_pivot[f"{metric}__pct"] = np.where(
            df_pivot[f"{metric}__previous"].fillna(0).eq(0),
            np.nan,
            df_pivot[f"{metric}__delta"] / df_pivot[f"{metric}__previous"]
        )

    return(df_pivot)

# Convert datafram to json format
def df_to_json(df_pivot,client):
    if client['account_type'] == "Lead Gen":
        metrics = ["Impressions", "Clicks", "Cost", "Conversions", "CTR", "CPC", "Conversion Rate", "CPA"]
    elif client['account_type'] == "Ecommerce":
        metrics = ["Impressions", "Clicks", "Cost", "Transactions", "Transaction Revenue", "CTR", "CPC", "Conversion Rate", "CPA", "ROAS"]
    
    int_metrics = ["Impressions", "Clicks", "Transactions", "Conversions"]
    pct_metrics = ["CTR", "Conversion Rate", "ROAS"]
    gbp_metrics = ["Cost", "Transaction Revenue", "CPA", "CPC"]

    output = {}

    for _, row in df_pivot.iterrows():
        platform = row[client['dimension']]
        output[platform] = {}

        for metric in metrics: 
            if metric in int_metrics:
                output[platform][metric] = {
                    "curr":  fmt_int(row[f"{metric}__current"]),
                    "prev":  fmt_int(row[f"{metric}__previous"]),
                    "delta": fmt_int(row[f"{metric}__delta"]),
                    "pct":   pct_diff(row[f"{metric}__pct"]),
            }
            elif metric in pct_metrics:
                output[platform][metric] = {
                    "curr":  fmt_pct(row[f"{metric}__current"]),
                    "prev":  fmt_pct(row[f"{metric}__previous"]),
                    "delta": fmt_pct(row[f"{metric}__delta"]),
                    "pct":   pct_diff(row[f"{metric}__pct"]),
            }
            elif metric in gbp_metrics:
                output[platform][metric] = {
                    "curr":  fmt_gbp(row[f"{metric}__current"]),
                    "prev":  fmt_gbp(row[f"{metric}__previous"]),
                    "delta": fmt_gbp(row[f"{metric}__delta"]),
                    "pct":   pct_diff(row[f"{metric}__pct"]),
            }
    return output

# Ensure no numpy types are returned
def to_py(v):
    if pd.isna(v):
        return None
    if isinstance(v, np.generic):
        return v.item()
    return v

# Convert df values to ints that are human readable
def fmt_int(v):
    v = to_py(v)
    if v is None:
        return "0"
    return f"{int(round(v)):,}"

# Convert df values to percentages that are human readable
def fmt_pct(v):
    v = to_py(v)
    if v is None:
        return "0.00%"
    return f"{float(v):.2f}%"

# Convert df values to percentages that include +/-
def pct_diff(v):
    v = to_py(v)
    if v is None:
        return "-"
    v = v*100
    if v > 0:
        return f"+{float(v):.2f}%"
    elif v <= 0:
        return f"{float(v):.2f}%"

# Convert df values to monetary values that are human readable
def fmt_gbp(v):
    v = to_py(v)
    if v is None:
        return "£0.00"  # e.g. "" or "—"
    return f"£{float(v):,.2f}"