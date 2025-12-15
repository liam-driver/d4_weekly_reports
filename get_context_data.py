import pandas as pd
import numpy as np
import locale
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from safe_div import safe_div

pd.options.mode.chained_assignment = None  # default='warn'
np.seterr(divide='ignore', invalid='ignore')

pd.options.mode.chained_assignment = None  # default='warn'
np.seterr(divide='ignore', invalid='ignore')

scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
sa = gspread.authorize(creds)

# Initial Config -- Declare Global Variables and initialise datasets (filename=serene-lotus-379510-b3f9b3b23758)
sh = sa.open('Weekly Reports')
locale.setlocale(locale.LC_ALL, 'en_GB.UTF-8')

# Time Variables
now = pd.Timestamp.now()
yday = (now - pd.DateOffset(days=2)).normalize()
if now.day <= 5:
    now = now - MonthEnd(1)
    yday = now
first_of_current_month = now.replace(day=1).normalize()
end_of_current_month = now + pd.offsets.MonthEnd(0)

def get_context_data(client):
    client['report_type'] = 'normal'
    ws = sh.worksheet(f"{client['name']} Funnel Import")
    df = pd.DataFrame(ws.get_all_records())
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    yoy_date_check = (client['start_date'] - pd.DateOffset(years=1)).normalize()   
    # Minimum date in the dataset
    min_date = df['Date'].min()
    # If YoY data
    if yoy_date_check >= min_date:
        first_compare = (client['start_date'] - pd.DateOffset(years=1)).normalize()
        yday_compare = (yday - pd.DateOffset(years=1)).normalize()
        client['report_dates'] = 'YoY'
        # Needs ad platform filter
        mask = ((df['Date'] >= client['start_date']) & (df['Date'] <= yday)) | ((df['Date'] >= first_compare) & (df['Date'] <= yday_compare))
        df = df.loc[mask]
        group = df.groupby(['Year'])
    else: 
        first_compare = (client['start_date'] - pd.DateOffset(months=1)).normalize()
        yday_compare = (yday - pd.DateOffset(months=1)).normalize()
        client['report_dates'] = 'MoM'
        mask = ((df['Date'] >= client['start_date']) & (df['Date'] <= yday)) | ((df['Date'] >= first_compare) & (df['Date'] <= yday_compare))
        df = df.loc[mask]
        group = df.groupby(['Month'])

    # Sum of the main columns
    df[df.columns[8]] = pd.to_numeric(df[df.columns[8]])
    sessions = group[df.columns[8]].sum()
    df[df.columns[12]] = pd.to_numeric(df[df.columns[12]])
    transactions = group[df.columns[12]].sum()
    df[df.columns[13]] = pd.to_numeric(df[df.columns[13]])
    transaction_revenue = group[df.columns[13]].sum()

    # Concatenate the columns
    new_df = pd.concat([sessions, transactions, transaction_revenue], axis='columns', sort=False)
    # Get the calculated columns
    new_df['Conversion Rate'] = safe_div(
        new_df[new_df.columns[1]],
        new_df[new_df.columns[0]],
        multiplier = 100
    )
    new_df['AOV'] = safe_div(
        new_df[new_df.columns[2]],
        new_df[new_df.columns[1]],
        multiplier = 1
    ) 
    new_df = new_df.astype('float64')
    new_df = new_df.round({new_df.columns[0]: 0, new_df.columns[1]: 0, new_df.columns[2]: 2, new_df.columns[3]: 2, new_df.columns[4]: 2})

    new_df = new_df.rename(columns={new_df.columns[0]: 'Sessions'})
    new_df = new_df.rename(columns={new_df.columns[1]: 'Transactions'})
    new_df = new_df.rename(columns={new_df.columns[2]: 'Transaction Revenue'})
    return new_df