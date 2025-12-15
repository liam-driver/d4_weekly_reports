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

def get_funnel_data(client):
    # Initialise the client list
    if client["dimension"] == '':
        # Create dataset (not dimension)
        dataset = create_dataset(client)
        report_data = get_report_data(dataset, client)
        client['report_data'] = report_data
    else:
        dataset = create_dataset(client)
        report_data = get_report_data(dataset, client)
        client['report_data'] = report_data

        dataset_dim = create_dataset_dim(client)
        curr_df = dataset_dim[0]
        prev_df = dataset_dim[1]
        report_data_dim = get_report_data_dim(curr_df, prev_df)
        client['report_data_dim'] = report_data_dim
    client['run_rate'] = get_run_rate(client)
    return client

# Return dictionary of data points
def create_dataset(client):
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
        mask = ((df['Ad Platform']!='') & (df['Date'] >= client['start_date']) & (df['Date'] <= yday)) | ((df['Date'] >= first_compare) & (df['Date'] <= yday_compare))
        df = df.loc[mask]
        group = df.groupby(['Year'])
    else: 
        first_compare = (client['start_date'] - pd.DateOffset(months=1)).normalize()
        yday_compare = (yday - pd.DateOffset(months=1)).normalize()
        client['report_dates'] = 'MoM'
        mask = ((df['Ad Platform']!='') & (df['Date'] >= client['start_date']) & (df['Date'] <= yday)) | ((df['Date'] >= first_compare) & (df['Date'] <= yday_compare))
        df = df.loc[mask]
        group = df.groupby(['Month'])
    # Transform data into data set
    if client['account_type'] == 'Lead Gen':
        df = transform_dataset_leadgen(df, group)
    else: 
        df = transform_dataset_ecomm(df, group)
    return(df)

def create_dataset_dim(client):
    client['report_type'] = 'normal'
    ws = sh.worksheet(f"{client['name']} Funnel Import")
    df = pd.DataFrame(ws.get_all_records())
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    yoy_date_check = (client['start_date'] - pd.DateOffset(years=1)).normalize()
    min_date_check = df[(df[df.columns[12]] != 0) & (df[df.columns[12]] != '')]
    
    # Minimum date in the dataset
    min_date = min_date_check['Date'].min()
    if (now - pd.DateOffset(days=2)).normalize() != yday:
        yday_f = client['end_date']
    else: 
        yday_f = yday
    # If YoY data
    if yoy_date_check >= min_date:
        first_compare = (client['start_date'] - pd.DateOffset(years=1)).normalize()
        yday_compare = (yday_f - pd.DateOffset(years=1)).normalize()
    else: 
        first_compare = (client['start_date'] - pd.DateOffset(months=1)).normalize()
        yday_compare = (yday_f - pd.DateOffset(months=1)).normalize()
    # Transform data into data set 
    if client['account_type'] == 'Lead Gen': 
        mask = ((df['Ad Platform']!='') & (df['Date'] >= client['start_date']) & (df['Date'] <= yday_f))
        curr_df = df.loc[mask]
        group = curr_df.groupby(client['dimension'])
        curr_df = transform_dataset_leadgen(curr_df, group)
        curr_df= add_overall_row(curr_df, client)
        curr_df = curr_df.iloc[:, 3:4].join(curr_df.loc[:, ['CPA']])

        mask = ((df['Ad Platform']!='') & (df['Date'] >= first_compare) & (df['Date'] <= yday_compare))
        prev_df = df.loc[mask]
        group = prev_df.groupby(client['dimension'])
        prev_df = transform_dataset_leadgen(prev_df, group)
        prev_df = add_overall_row(prev_df, client)
        prev_df = prev_df.iloc[:, 3:4].join(prev_df.loc[:, ['CPA']])
    else: 
        mask = ((df['Ad Platform']!='') & (df['Date'] >= client['start_date']) & (df['Date'] <= yday_f))
        curr_df = df.loc[mask]
        group = curr_df.groupby(client['dimension'])
        curr_df = transform_dataset_ecomm(curr_df, group)
        curr_df= add_overall_row(curr_df, client)
        curr_df = curr_df.iloc[:, 4:5].join(curr_df.loc[:, ['ROAS']])

        mask = ((df['Ad Platform']!='') & (df['Date'] >= first_compare) & (df['Date'] <= yday_compare))
        prev_df = df.loc[mask]
        group = prev_df.groupby(client['dimension'])
        prev_df = transform_dataset_ecomm(prev_df, group)
        prev_df = add_overall_row(prev_df, client)
        prev_df = prev_df.iloc[:, 4:5].join(prev_df.loc[:, ['ROAS']])
    curr_df.replace([np.nan, np.inf], '-', inplace=True)
    prev_df.replace([np.nan, np.inf], '-', inplace=True)
    df = [curr_df, prev_df]
    return(curr_df, prev_df)

    
# Transform dataset into ecomm template
def transform_dataset_ecomm(df, group):
    # Sum of the main columns
    df[df.columns[9]] = pd.to_numeric(df[df.columns[9]])
    impressions = group[df.columns[9]].sum()
    df[df.columns[10]] = pd.to_numeric(df[df.columns[10]])
    clicks = group[df.columns[10]].sum()
    df[df.columns[11]] = pd.to_numeric(df[df.columns[11]])
    cost = group[df.columns[11]].sum()
    df[df.columns[12]] = pd.to_numeric(df[df.columns[12]])
    transactions = group[df.columns[12]].sum()
    df[df.columns[13]] = pd.to_numeric(df[df.columns[13]])
    transaction_revenue = group[df.columns[13]].sum()

    # Concatenate the columns
    new_df = pd.concat([impressions, clicks, cost, transactions, transaction_revenue], axis='columns', sort=False)

    # Get the calculated columns
    new_df['CTR'] = safe_div(
        new_df[new_df.columns[1]],
        new_df[new_df.columns[0]],
        multiplier = 100
    )
    new_df['CPC'] = safe_div(
        new_df[new_df.columns[2]],
        new_df[new_df.columns[1]],
        multiplier = 1
    ) 
    new_df['Conversion Rate'] = safe_div(
        new_df[new_df.columns[3]],
        new_df[new_df.columns[1]],
        multiplier = 100
    )
    new_df['CPA'] = safe_div(
        new_df[new_df.columns[2]],
        new_df[new_df.columns[3]],
        multiplier = 1
    )
    new_df['ROAS'] = safe_div(
        new_df[new_df.columns[4]],
        new_df[new_df.columns[2]],
        multiplier = 100
    )
    new_df = new_df.astype('float64')
    new_df = new_df.round({new_df.columns[2]: 2, new_df.columns[4]: 2, new_df.columns[5]: 2, new_df.columns[6]: 2, new_df.columns[7]: 2, new_df.columns[8]: 2, new_df.columns[9]: 2})

    new_df = new_df.rename(columns={new_df.columns[0]: 'Impressions'})
    new_df = new_df.rename(columns={new_df.columns[1]: 'Clicks'})
    new_df = new_df.rename(columns={new_df.columns[2]: 'Cost'})
    new_df = new_df.rename(columns={new_df.columns[3]: 'Transactions'})
    new_df = new_df.rename(columns={new_df.columns[4]: 'Transaction Revenue'})
    return new_df

def add_overall_row(curr_df: pd.DataFrame, client: dict) -> pd.DataFrame:
    df = curr_df.copy()

    overall = {}

    if client['account_type'] == 'Lead Gen':
        # Totals
        total_impr = df['Impressions'].sum()
        total_clicks = df['Clicks'].sum()
        total_cost = df['Cost'].sum()
        total_conversions = df['Conversions'].sum()

        # Base totals
        overall['Impressions'] = total_impr
        overall['Clicks'] = total_clicks
        overall['Cost'] = total_cost
        overall['Conversions'] = total_conversions

        # Derived metrics using safe_div
        overall['CTR'] = round(safe_div(total_clicks, total_impr, multiplier=100.0),2)
        overall['CPC'] = round(safe_div(total_cost, total_clicks),2)
        overall['Conversion Rate'] = round(safe_div(total_conversions, total_clicks, multiplier=100.0),2)
        overall['CPA'] = round(safe_div(total_cost, total_conversions),2)

    else:
        # Treat anything else as Ecommerce
        total_impr = df['Impressions'].sum()
        total_clicks = df['Clicks'].sum()
        total_cost = df['Cost'].sum()
        total_txns = df['Transactions'].sum()
        total_revenue = df['Transaction Revenue'].sum()

        # Base totals
        overall['Impressions'] = total_impr
        overall['Clicks'] = total_clicks
        overall['Cost'] = total_cost
        overall['Transactions'] = total_txns
        overall['Transaction Revenue'] = total_revenue

        # Derived metrics using safe_div
        overall['CTR'] = round(safe_div(total_clicks, total_impr, multiplier=100.0),2)
        overall['CPC'] = round(safe_div(total_cost, total_clicks),2)
        overall['Conversion Rate'] = round(safe_div(total_txns, total_clicks, multiplier=100.0),2)
        overall['CPA'] = round(safe_div(total_cost, total_txns),2)
        # ROAS as a percentage (e.g. 316.30)
        overall['ROAS'] = round(safe_div(total_revenue, total_cost, multiplier=100.0),2)

    # Ensure all existing columns are represented in the overall row
    for col in df.columns:
        if col not in overall:
            overall[col] = np.nan

    # Append the overall row at the bottom
    df.loc['Overall'] = pd.Series(overall)

    return df

# Transform dataset into lead gen template
def transform_dataset_leadgen(df, group):
    # Sum of the main columns
    df[df.columns[9]] = pd.to_numeric(df[df.columns[9]])
    impressions = group[df.columns[9]].sum()
    df[df.columns[10]] = pd.to_numeric(df[df.columns[10]])
    clicks = group[df.columns[10]].sum()
    df[df.columns[11]] = pd.to_numeric(df[df.columns[11]])
    cost = group[df.columns[11]].sum()
    df[df.columns[12]] = pd.to_numeric(df[df.columns[12]])
    transactions = group[df.columns[12]].sum()

    new_df = pd.concat([impressions, clicks, cost, transactions], axis='columns', sort=False)

    new_df['CTR'] = safe_div(
        new_df[new_df.columns[1]],
        new_df[new_df.columns[0]],
        multiplier = 100
    )
    new_df['CPC'] = safe_div(
        new_df[new_df.columns[2]],
        new_df[new_df.columns[1]],
        multiplier = 1
    ) 
    new_df['Conversion Rate'] = safe_div(
        new_df[new_df.columns[3]],
        new_df[new_df.columns[1]],
        multiplier = 100
    )
    new_df['CPA'] = safe_div(
        new_df[new_df.columns[2]],
        new_df[new_df.columns[3]],
        multiplier = 1
    )
    new_df = new_df.astype('float64')
    new_df = new_df.round({new_df.columns[2]: 2, new_df.columns[4]: 2, new_df.columns[5]: 2, new_df.columns[6]: 2, new_df.columns[7]: 2})
    new_df = new_df.rename(columns={new_df.columns[0]: 'Impressions'})
    new_df = new_df.rename(columns={new_df.columns[1]: 'Clicks'})
    new_df = new_df.rename(columns={new_df.columns[2]: 'Cost'})
    new_df = new_df.rename(columns={new_df.columns[3]: 'Conversions'})

    return new_df

# Convert Data set into object dictionary
def get_report_data(df,client):
    report_data = []

    for column in df:
        report_data_tmp = {}
        report_data_tmp['field'] = column
        if client['report_dates'] == 'MoM':
            cmp_curr = client['start_date'].strftime('%B')
            cmp_prev = (client['start_date'] - pd.DateOffset(months=1)).normalize().strftime('%B')
        else:
            cmp_curr = pd.to_datetime(client['start_date']).year
            cmp_prev = cmp_curr-1
        report_data_tmp['prev'] = df.at[cmp_prev, column]
        report_data_tmp['current'] = df.at[cmp_curr, column]
        report_data_tmp['compare'] = round(safe_div(
            report_data_tmp['current'] - report_data_tmp['prev'],
            report_data_tmp['prev'],
            multiplier = 100
        ),2)
        if report_data_tmp['compare'] == float('nan') or report_data_tmp['compare'] == float('inf'):
            report_data_tmp['compare'] = '-'
        # Format Data to be readable 
        if report_data_tmp['field'] == 'Impressions' or report_data_tmp['field'] == 'Clicks' or report_data_tmp['field'] == 'Transactions' or report_data_tmp['field'] == 'Conversions':
            report_data_tmp['prev'] = int(report_data_tmp['prev'])
            report_data_tmp['current'] = int(report_data_tmp['current'])
            report_data_tmp['prev'] = f"{report_data_tmp['prev']:,d}"
            report_data_tmp['current'] = f"{report_data_tmp['current']:,d}"
        elif report_data_tmp['field'] == 'Transaction Revenue' or report_data_tmp['field'] == 'CPC' or report_data_tmp['field'] == 'CPA':
            report_data_tmp['prev'] = locale.currency(report_data_tmp['prev'], grouping=True)
            report_data_tmp['current'] = locale.currency(report_data_tmp['current'], grouping=True)
        elif report_data_tmp['field'] == 'Cost' or report_data_tmp['field'] == 'Cost (*)':
            pass
        else:
            report_data_tmp['prev'] = str(report_data_tmp['prev']) + "%"
            report_data_tmp['current'] = str(report_data_tmp['current']) + "%"
        if isinstance(report_data_tmp['compare'], (str, np.str_)):
            report_data_tmp['compare'] = str(report_data_tmp['compare']) + "%"
        elif report_data_tmp['compare'] > 0:
            report_data_tmp['compare'] = str(report_data_tmp['compare']) + "%"
            report_data_tmp['compare'] = "+" + report_data_tmp['compare']
        else:
            report_data_tmp['compare'] = str(report_data_tmp['compare']) + "%"
        report_data.append(report_data_tmp)
    return report_data


def get_report_data_dim(curr_df, prev_df):
    dim_data = []
    # loop through dimensions
        # loop through metrics
    for index, row in curr_df.iterrows():
        dim_data_tmp = {}
        dim_data_tmp['dim_name'] = index
        report_data = []
        for column in curr_df:
            report_data_tmp = {}
            report_data_tmp['field'] = column
            try:
                report_data_tmp['prev'] = prev_df.at[index ,column]
            except KeyError:
                report_data_tmp['prev'] = '-'
            report_data_tmp['current'] = curr_df.at[index ,column]
            if report_data_tmp['prev'] == '-' or report_data_tmp['current'] == '-':
                report_data_tmp['compare'] = '-'
            else:
                report_data_tmp['compare'] = round(safe_div(
                    report_data_tmp['current'] - report_data_tmp['prev'],
                    report_data_tmp['prev'],
                    multiplier = 100
            ),2)       
            if report_data_tmp['compare'] == 0.0:
                report_data_tmp['compare'] = '-'                      
            # Format Data to be readable 
            if report_data_tmp['field'] == 'ROAS':
                if report_data_tmp['prev'] != '-':
                    report_data_tmp['prev'] = str(report_data_tmp['prev']) + "%"
                if report_data_tmp['current'] != '-':
                    report_data_tmp['current'] = str(report_data_tmp['current']) + "%"
            elif report_data_tmp['field'] in ('CPA', 'Transaction Revenue'):
                if report_data_tmp['prev'] != '-':
                    report_data_tmp['prev'] = locale.currency(report_data_tmp['prev'], grouping=True)
                if report_data_tmp['current'] != '-':
                    report_data_tmp['current'] = locale.currency(report_data_tmp['current'], grouping=True)
            else:
                if report_data_tmp['prev'] != '-':
                    report_data_tmp['prev'] = int(report_data_tmp['prev'])
                    report_data_tmp['prev'] = f"{report_data_tmp['prev']:,d}"
                if report_data_tmp['current'] != '-':    
                    report_data_tmp['current'] = int(report_data_tmp['current'])
                    report_data_tmp['current'] = f"{report_data_tmp['current']:,d}"
            if report_data_tmp['compare'] != '-':
                if report_data_tmp['compare'] > 0:
                    report_data_tmp['compare'] = str(report_data_tmp['compare']) + "%"
                    report_data_tmp['compare'] = "+" + report_data_tmp['compare']
                else:
                    report_data_tmp['compare'] = str(report_data_tmp['compare']) + "%"
            report_data.append(report_data_tmp)
        dim_data_tmp['report_data'] = report_data
        dim_data.append(dim_data_tmp)
    print(dim_data)
    return dim_data

def get_run_rate(client):
    spend = float(client['report_data'][2]['current'])
    day_diff_yday = ((yday - client['start_date']).days)
    day_diff_month = ((client['end_date'] - client['start_date']).days) + 1
    run_rate = (spend / day_diff_yday) * day_diff_month
    run_rate = locale.currency(run_rate, grouping=True)
    client['report_data'][2]['current'] = locale.currency(client['report_data'][2]['current'], grouping=True)
    return(run_rate)