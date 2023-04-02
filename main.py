import gspread
import smtplib
import os
import pandas as pd
import datetime
import calendar
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from jinja2 import Environment, FileSystemLoader
from datetime import date
import locale
from pandas.tseries.offsets import MonthEnd
pd.options.mode.chained_assignment = None  # default='warn'

# API call
sa = gspread.service_account() # (filename=serene-lotus-379510-b3f9b3b23758)
# Sheet
sh = sa.open('Weekly Reports')
# Config sheet
cfg = sh.worksheet("Config")
ws_config = pd.DataFrame(cfg.get_all_records())
# Action & Insights Sheet
ais = sh.worksheet("Actions & Insights")
ws_ais = pd.DataFrame(ais.get_all_records())
locale.setlocale(locale.LC_ALL, 'en_GB.UTF-8')
# Initialise Time variables
now = pd.Timestamp.now()
if now.day <= 3:
    now = now - MonthEnd(1)
first = now.replace(day=1).normalize()
yday = (now - pd.DateOffset(days=1)).normalize()
current_year = date.today().year
previous_year = current_year - 1
current_month = calendar.month_name[now.month]
previous_month = calendar.month_name[(now - pd.offsets.MonthEnd(1)).month]
compare = ''


def main():
    # Get account manager emails and client lists with type
    clients = config()
    em = emails()
    # Loop through the list and execute all the functions
    for client, client_type in clients.items():
        if client_type == 'Lead Gen':
            data = data_lead(client)
            kpist = ["Conversions", "CPA"]
            metricst = ["Impressions", "Clicks", "Cost", "Conversions", "CTR", "CPC", "Conversion Rate", "CPA"]

        else:
            data = data_ecom(client)
            kpist = ["Transactions", "Transaction Revenue", "CPA", "ROAS"]
            metricst = ["Impressions", "Clicks", "Cost", "Transactions", "Transaction Revenue", "CTR", "CPC", "Conversion Rate", "CPA", "ROAS"]

        # Initialise the KPI lists
        kpisd = kpis(data, client_type)
        kpisl = list(kpisd.keys())
        kpisy = list(kpisd.values())
        for i in range(len(kpisy)):
            kpisy[i] = str(kpisy[i]) + "%"

        for i in range(len(kpist)):
            if kpist[i] == 'Impressions' or kpist[i] == 'Clicks' or kpist[i] == 'Conversions' or kpist[i] == 'Transactions':
                kpisl[i] = int(kpisl[i])
            elif kpist[i] == 'Cost' or kpist[i] == 'Transaction Revenue' or kpist[i] == 'CPC' or kpist[i] == 'CPA':
                kpisl[i] = locale.currency(kpisl[i], grouping=True)
            else:
                kpisl[i] = str(kpisl[i]) + "%"
        # Initialise the Spend list
        spend_block = costs(data)
        budget = budgets(client)

        # Initialise the Metrics Lists
        metricsd = metrics(data)
        metricsl = list(metricsd.keys())
        metricsy = list(metricsd.values())
        for i in range(len(metricsy)):
            metricsy[i] = str(metricsy[i]) + "%"

        # Format the values to % and $z
        for i in range(len(metricst)):
            if metricst[i] == 'Impressions' or metricst[i] == 'Clicks' or metricst[i] =='Transactions' or metricst[i] =='Conversions':
                metricsl[i] = int(metricsl[i])
            elif metricst[i] == 'Cost' or metricst[i] == 'Transaction Revenue' or metricst[i] =='CPC' or metricst[i] =='CPA':
                metricsl[i] = locale.currency(metricsl[i], grouping= True)
            else:
                metricsl[i] = str(metricsl[i]) + "%"

        # Initialise the Actions and Insights Lists
        actions_list = actions(client)
        action = actions_list[2].pop()
        insights = actions_list[1].pop()
        wwd = actions_list[0].pop()

        # Initialise the email
        env = Environment(loader=FileSystemLoader('templates'))
        template = env.get_template('email_template.html')

        # Get the date
        month_start = yday - datetime.timedelta(yday.day - 1)
        day_check = pd.Timestamp.now()
        if day_check.day <= 3:
            period_end = now
        else:
            period_end =  yday
        period_end = period_end.normalize().strftime("%d/%m/%Y")
        period_start = first.strftime("%d/%m/%Y")

        # Write and send the email
        html = template.render(client=client, period_end=period_end, period_start=period_start, kpist_kpisl_kpisy=zip(kpist,kpisl,kpisy), spend_block=spend_block, budget=budget,
                               metricst_metricsl_metricsy=zip(metricst,metricsl,metricsy), wwd=wwd, insights=insights, actions=action, compare=compare)
        email(html, client, em)
    # Successfully Run
    return 0


# Initialise the Client list w/ their ecommerce / lead gen status
def config():
    clients = {}
    for column in ws_config:
        clients[column] = ws_config.at[0, column]
    return clients

# Initialise a list of all the email addresses


def emails():
    email = {}
    for column in ws_config:
        email[column] = (ws_config.at[1, column])
    return email


def budgets(client):
    budget = ws_config.at[2, client]
    return budget


# Get the data from the sheet and format it into dataset for the email // Ecommerce
def data_ecom(client):

    # Initialise raw datasheet through Google Sheets worksheet
    ws = sh.worksheet(f"{client} Funnel Import")
    df_raw = pd.DataFrame(ws.get_all_records())
    filt = df_raw['Paid / Organic'] == 'Paid'
    df = df_raw[filt]
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')

    # Test whether to do MoM or YoY
    min_date = df['Date'].min()
    prev = (first - pd.DateOffset(years=1)).normalize()
    if prev >= min_date:
        # YoY Dataset
        first_yoy = (first - pd.DateOffset(years=1)).normalize()
        yday_yoy = (yday - pd.DateOffset(years=1)).normalize()
        # Create the filter for the data
        mask = ((df['Date'] >= first) & (df['Date'] <= yday)) | (df['Date'] <= yday_yoy)
        df = df.loc[mask]
        year_grp = df.groupby(['Year'])

        # Sum of the main columns
        df[df.columns[9]] = pd.to_numeric(df[df.columns[9]])
        impressions = year_grp[df.columns[9]].sum()
        df[df.columns[10]] = pd.to_numeric(df[df.columns[10]])
        clicks = year_grp[df.columns[10]].sum()
        df[df.columns[11]] = pd.to_numeric(df[df.columns[11]])
        cost = year_grp[df.columns[11]].sum()
        df[df.columns[12]] = pd.to_numeric(df[df.columns[12]])
        transactions = year_grp[df.columns[12]].sum()
        df[df.columns[13]] = pd.to_numeric(df[df.columns[13]])
        transaction_revenue = year_grp[df.columns[13]].sum()

        # Concatenate the columns
        new_df = pd.concat([impressions, clicks, cost, transactions, transaction_revenue], axis='columns', sort=False)

        # Get the calculated columns
        new_df['CTR'] = ((new_df[new_df.columns[1]] / new_df[new_df.columns[0]]) * 100)
        new_df['CPC'] = (new_df[new_df.columns[2]] / new_df[new_df.columns[1]])
        new_df['Conversion Rate'] = ((new_df[new_df.columns[3]] / new_df[new_df.columns[2]]) * 100)
        new_df['CPA'] = (new_df[new_df.columns[2]] / new_df[new_df.columns[3]])
        new_df['ROAS'] = ((new_df[new_df.columns[4]] / new_df[new_df.columns[2]]) * 100)
        new_df = new_df.astype('float64')
        new_df = new_df.round({new_df.columns[2]: 2, new_df.columns[4]: 2, new_df.columns[5]: 2, new_df.columns[6]: 2, new_df.columns[7]: 2, new_df.columns[8]: 2, new_df.columns[9]: 2})

        # Create YoY Comparison Row
        yoy = {}
        current_year = date.today().year
        previous_year = current_year - 1

        for column in new_df:
            cy = new_df.at[current_year, column]
            py = new_df.at[previous_year, column]
            yoy[column] = ((cy - py) / py) * 100

        tmp_df = pd.DataFrame(yoy, index=['Compare'])
        tmp_df = tmp_df.round(2)
        concat_df = pd.concat([new_df, tmp_df])
        global compare
        compare = 'YoY'

    else:
        # MoM Dataset
        current_year = date.today().year
        mask = ((df['Year'] == current_year) & ((df['Month'] == current_month) | (df['Month'] == previous_month)))
        df = df.loc[mask]

        year_grp = df.groupby(['Month'])

        df[df.columns[9]] = pd.to_numeric(df[df.columns[9]])
        impressions = year_grp[df.columns[9]].sum()
        df[df.columns[10]] = pd.to_numeric(df[df.columns[10]])
        clicks = year_grp[df.columns[10]].sum()
        df[df.columns[11]] = pd.to_numeric(df[df.columns[11]])
        cost = year_grp[df.columns[11]].sum()
        df[df.columns[12]] = pd.to_numeric(df[df.columns[12]])
        transactions = year_grp[df.columns[12]].sum()
        df[df.columns[13]] = pd.to_numeric(df[df.columns[13]])
        transaction_revenue = year_grp[df.columns[13]].sum()

        new_df = pd.concat([impressions, clicks, cost, transactions, transaction_revenue], axis='columns', sort=False)

        new_df['CTR'] = ((new_df[new_df.columns[1]] / new_df[new_df.columns[0]]) * 100)
        new_df['CPC'] = (new_df[new_df.columns[2]] / new_df[new_df.columns[1]])
        new_df['Conversion Rate'] = ((new_df[new_df.columns[3]] / new_df[new_df.columns[2]]) * 100)
        new_df['CPA'] = (new_df[new_df.columns[2]] / new_df[new_df.columns[3]])
        new_df['ROAS'] = ((new_df[new_df.columns[4]] / new_df[new_df.columns[2]]) * 100)

        new_df = new_df.astype('float64')
        new_df = new_df.round({new_df.columns[2]: 2, new_df.columns[4]: 2, new_df.columns[5]: 2, new_df.columns[6]: 2, new_df.columns[7]: 2, new_df.columns[8]: 2, new_df.columns[9]: 2})

        mom = {}
        for column in new_df:
            cy = new_df.at[current_month, column]
            py = new_df.at[previous_month, column]
            mom[column] = ((cy - py) / py) * 100

        tmp_df = pd.DataFrame(mom, index=['Compare'])
        tmp_df = tmp_df.round(2)
        concat_df = pd.concat([new_df, tmp_df])
        compare = 'MoM'

    return concat_df


def data_lead(client):
    ws = sh.worksheet(f"{client} Funnel Import")
    df_raw = pd.DataFrame(ws.get_all_records())
    filt = df_raw['Paid / Organic'] == 'Paid'
    df = df_raw[filt]
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')

    min_date = df['Date'].min()
    prev = (first - pd.DateOffset(years=1)).normalize()
    if prev >= min_date:
        # YoY Dataset
        first_yoy = (first - pd.DateOffset(years=1)).normalize()
        yday_yoy = (yday - pd.DateOffset(years=1)).normalize()

        mask = ((df['Date'] >= first) & (df['Date'] <= yday)) | (df['Date'] <= yday_yoy)
        df = df.loc[mask]

        year_grp = df.groupby(['Year'])

        df[df.columns[9]] = pd.to_numeric(df[df.columns[9]])
        impressions = year_grp[df.columns[9]].sum()
        df[df.columns[10]] = pd.to_numeric(df[df.columns[10]])
        clicks = year_grp[df.columns[10]].sum()
        df[df.columns[11]] = pd.to_numeric(df[df.columns[11]])
        cost = year_grp[df.columns[11]].sum()
        df[df.columns[12]] = pd.to_numeric(df[df.columns[12]])
        transactions = year_grp[df.columns[12]].sum()

        new_df = pd.concat([impressions, clicks, cost, transactions], axis='columns', sort=False)

        new_df['CTR'] = (new_df[new_df.columns[1]] / new_df[new_df.columns[0]] * 100)
        new_df['CPC'] = (new_df[new_df.columns[2]] / new_df[new_df.columns[1]])
        new_df['Conversion Rate'] = (new_df[new_df.columns[3]] / new_df[new_df.columns[2]] * 100)
        new_df['CPA'] = (new_df[new_df.columns[2]] / new_df[new_df.columns[3]])
        new_df = new_df.astype('float64')
        new_df = new_df.round({new_df.columns[2]: 2, new_df.columns[4]: 2, new_df.columns[5]: 2, new_df.columns[6]: 2, new_df.columns[7]: 2})

        yoy = {}
        current_year = date.today().year
        previous_year = current_year - 1

        for column in new_df:
            cy = new_df.at[current_year, column]
            py = new_df.at[previous_year, column]
            yoy[column] = ((cy - py) / py) * 100

        tmp_df = pd.DataFrame(yoy, index=['Compare'])
        tmp_df = tmp_df.round(2)
        concat_df = pd.concat([new_df, tmp_df])
        global compare
        compare = 'YoY'
    else:
        # MoM Dataset
        current_year = date.today().year
        mask = ((df['Year'] == current_year) & ((df['Month'] == current_month) | (df['Month'] == previous_month)))
        df = df.loc[mask]

        year_grp = df.groupby(['Month'])

        df[df.columns[9]] = pd.to_numeric(df[df.columns[9]])
        impressions = year_grp[df.columns[9]].sum()
        df[df.columns[10]] = pd.to_numeric(df[df.columns[10]])
        clicks = year_grp[df.columns[10]].sum()
        df[df.columns[11]] = pd.to_numeric(df[df.columns[11]])
        cost = year_grp[df.columns[11]].sum()
        df[df.columns[12]] = pd.to_numeric(df[df.columns[12]])
        transactions = year_grp[df.columns[12]].sum()

        new_df = pd.concat([impressions, clicks, cost, transactions], axis='columns', sort=False)

        new_df['CTR'] = (new_df[new_df.columns[1]] / new_df[new_df.columns[0]] * 100)
        new_df['CPC'] = (new_df[new_df.columns[2]] / new_df[new_df.columns[1]])
        new_df['Conversion Rate'] = (new_df[new_df.columns[3]] / new_df[new_df.columns[2]] * 100)
        new_df['CPA'] = (new_df[new_df.columns[2]] / new_df[new_df.columns[3]])

        new_df = new_df.astype('float64')
        new_df = new_df.round({new_df.columns[2]: 2, new_df.columns[4]: 2, new_df.columns[5]: 2, new_df.columns[6]: 2, new_df.columns[7]: 2})

        mom = {}
        for column in new_df:
            cy = new_df.at[current_month, column]
            py = new_df.at[previous_month, column]
            mom[column] = ((cy - py) / py) * 100

        tmp_df = pd.DataFrame(mom, index=['Compare'])
        tmp_df = tmp_df.round(2)
        concat_df = pd.concat([new_df, tmp_df])
        compare = 'MoM'
    return concat_df


def kpis(data, client_type):
    if compare == "YoY":
        if client_type == "Lead Gen":
            kpis_results = {}
            current_year = date.today().year
            kpis_results[data.at[current_year, data.columns[3]]] = data.at["Compare", data.columns[3]]
            kpis_results[data.at[current_year, "CPA"]] = data.at["Compare", "CPA"]
        else:
            kpis_results = {}
            current_year = date.today().year
            kpis_results[data.at[current_year, data.columns[3]]] = data.at["Compare", data.columns[3]]
            kpis_results[data.at[current_year, data.columns[4]]] = data.at["Compare", data.columns[4]]
            kpis_results[data.at[current_year, "CPA"]] = data.at["Compare", "CPA"]
            kpis_results[data.at[current_year, "ROAS"]] = data.at["Compare", "ROAS"]
    else:
        if client_type == "Lead Gen":
            kpis_results = {}
            kpis_results[data.at[current_month, data.columns[3]]] = data.at["Compare", data.columns[3]]
            kpis_results[data.at[current_month, "CPA"]] = data.at["Compare", "CPA"]
        else:
            kpis_results = {}
            kpis_results[data.at[current_month, data.columns[3]]] = data.at["Compare", data.columns[3]]
            kpis_results[data.at[current_month, data.columns[4]]] = data.at["Compare", data.columns[4]]
            kpis_results[data.at[current_month, "CPA"]] = data.at["Compare", "CPA"]
            kpis_results[data.at[current_month, "ROAS"]] = data.at["Compare", "ROAS"]
    return kpis_results


def costs(data):
    c_year = date.today().year
    c_month = date.today().month
    yday = (date.today().day) - 1
    total_days = calendar.monthrange(c_year, c_month)[1]
    if compare == "YoY":
        spend = data.at[current_year, data.columns[2]]
    else:
        spend = data.at[current_month, data.columns[2]]
    run_rate = (spend / yday) * total_days

    spend = locale.currency(spend, grouping=True)
    run_rate = locale.currency(run_rate, grouping=True)

    costs_results = [spend, run_rate]
    return costs_results


def metrics(data):
    metrics_results = {}
    current_year = date.today().year
    if compare == "YoY":
        for columns in data:
            metrics_results[data.at[current_year, columns]] = data.at["Compare", columns]
    else:
        for columns in data:
            metrics_results[data.at[current_month, columns]] = data.at["Compare", columns]
    return metrics_results


def actions(client):
    ws = sh.worksheet("Actions & Insights")
    df_raw = pd.DataFrame(ws.get_all_records())
    wwd = []
    insights = []
    actions = []

    for i in range(15):
        wwd.append(df_raw.at[i, client])
        insights.append(df_raw.at[(i + 15), client])
        actions.append(df_raw.at[(i + 30), client])
    while '' in wwd:
        wwd.remove('')
    while '' in insights:
        insights.remove('')
    while '' in actions:
        actions.remove('')
    zip_list = [[wwd], [insights], [actions]]

    return zip_list


def email(html, client, email_address):
    # Define email & password for sender email
    wr_email = os.environ.get('WEEKLY_REPORTS_EMAIL')
    wr_password = os.environ.get('WEEKLY_REPORTS_PASSWORD')

    # Create a message object
    msg = MIMEMultipart()
    msg['From'] = wr_email
    msg['To'] = email_address[client]
    msg['Subject'] = f"{client} Weekly Report"

    # Add the HTML body to the message
    body = MIMEText(html, 'html')
    msg.attach(body)

    # Send the message
    with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(wr_email, wr_password)
        smtp.sendmail(wr_email, 'liam.driver@door4.com', msg.as_string())


main()
