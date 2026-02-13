import pandas as pd
import numpy as np
import json
import locale
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from safe_div import safe_div
from pandas.tseries.offsets import MonthEnd
from generate_df import *


# Main workflow
def get_funnel_data(client, breakdown_dimension, table_type):
    df = initialise_df(client)
    df = apply_filters(df, client, breakdown_dimension)
    df = add_secondary_metrics(df, breakdown_dimension, table_type)
    metrics = [col for col in df.columns.values if col not in ['Period', breakdown_dimension]]
    df = pivot_df(df, breakdown_dimension, metrics)
    final_data = df_to_json(df, breakdown_dimension, metrics)
    return final_data

def get_llm_data(client, breakdown_dimension, table_type):
    df = initialise_df(client)
    df = apply_filters(df, client, breakdown_dimension)
    ad_channels = df['Ad Channel'].dropna().unique().tolist()
    final_data = {}
    for channel in ad_channels:
        if channel not in ['Combined', 'Dispaly','Shopping', 'Paid Search', 'Paid Social', 'Paid Social Static', 'Paid Social Video', 'Video']:
            continue
        df_llm = df.copy()
        mask = (df['Ad Channel'] == channel)
        df_llm = df_llm.loc[mask]
        headers = list(df_llm.columns.values)
        if channel == 'Paid Search':
            if client['account_type'] == 'Lead Gen':
                df_llm = paid_search_lead_gen(df_llm, 'Ad Platform' ,  headers)
            else:
                df_llm = paid_search_ecommerce(df_llm, 'Ad Platform' ,  headers)
        if channel in ('Shopping', 'Combined', 'Performance Max'):
            if client['account_type'] == 'Lead Gen':
                df_llm = paid_shopping_lead_gen(df_llm, 'Ad Platform' ,  headers)
            else:
                df_llm = paid_shopping_ecommerce(df_llm, 'Ad Platform' ,  headers)
        if channel == 'Display':
            if client['account_type'] == 'Lead Gen':
                df_llm = paid_display_lead_gen(df_llm, 'Ad Platform' ,  headers)
            else:
                df_llm = paid_display_ecommerce(df_llm, 'Ad Platform' ,  headers)
        if channel == 'Video':
            if client['account_type'] == 'Lead Gen':
                df_llm = paid_video_lead_gen(df_llm, 'Ad Platform' ,  headers)
            else:
                df_llm = paid_video_ecommerce(df_llm, 'Ad Platform' ,  headers)
        if channel == 'Paid Social Video':
            if client['account_type'] == 'Lead Gen':
                df_llm = paid_social_video_lead_gen(df_llm, 'Ad Platform' ,  headers)
            else:
                df_llm = paid_social_video_ecommerce(df_llm, 'Ad Platform' ,  headers)
        if channel in ('Paid Social Static', 'Paid Social'):
            if client['account_type'] == 'Lead Gen':
                df_llm = paid_social_static_lead_gen(df_llm, 'Ad Platform' ,  headers)
            else:
                df_llm = paid_social_static_ecommerce(df_llm, 'Ad Platform' ,  headers)            
        metrics = [col for col in df_llm.columns.values if col not in ['Period', 'Ad Platform']]
        df_llm = pivot_df(df_llm, 'Ad Platform', metrics)
        final_data[channel] = df_to_json(df_llm, 'Ad Platform', metrics)
    return(final_data)

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
def apply_filters(df, client, breakdown_dimension):
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
    mask = ((df[breakdown_dimension]!='') & (((df['Date'] >= start_date) & (df['Date'] <= end_date)) | (df['Date'] >= compare_start_date) & (df['Date'] <= compare_end_date)))
    df = df.loc[mask]
    # Add in helper columns to categorise date periods
    df.loc[df['Date'] >= start_date, 'Period'] = 'Current'
    df.loc[df['Date'] < start_date, 'Period'] = 'Previous'
    return (df)

# Add in secondary metrics to the dataframe, such as ROAS
def add_secondary_metrics(df, breakdown_dimension, table_type):
    if table_type == 'paid_ecommerce':
        df_grouped = paid_ecommerce(df, breakdown_dimension)

    if table_type == 'paid_lead_gen':
        df_grouped = paid_lead_gen(df, breakdown_dimension)

    if table_type == 'overall_ecommerce':
        df_grouped = overall_ecommerce(df, breakdown_dimension)

    if table_type == 'overall_lead_gen':
        df_grouped = overall_lead_gen(df, breakdown_dimension)

    return(df_grouped)

# Create a pivoted version of the data
def pivot_df(df_grouped, breakdown_dimension,metrics):
    df_pivot = (
        df_grouped.pivot(index=breakdown_dimension, columns="Period", values=metrics)
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
def df_to_json(df_pivot, breakdown_dimension, metrics):    
    int_metrics = ["Impressions", "Clicks", "Transactions", "Conversions", "Sessions", "Thruplays", "3-Second Video Plays","Views"]
    pct_metrics = ["CTR", "Conversion Rate", "ROAS", "Impression Share", "Abs. Top Impression Share", "View Rate", "Hook Rate", "Hold Rate"]
    gbp_metrics = ["Cost", "Transaction Revenue", "CPA", "CPC", "AOV"]

    output = {}

    for _, row in df_pivot.iterrows():
        platform = row[breakdown_dimension]
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