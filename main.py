import gspread
import smtplib
import os
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from jinja2 import Environment, FileSystemLoader
import datetime
import calendar
from datetime import date
import numpy
import re
import locale

# API call
sa = gspread.service_account() #(filename=serene-lotus-379510-b3f9b3b23758)
# Sheet
sh = sa.open('Weekly Reports')
# Config sheet
cfg = sh.worksheet("Config")
ws_config = pd.DataFrame(cfg.get_all_records())
ais = sh.worksheet("Actions & Insights")
ws_ais = pd.DataFrame(ais.get_all_records())
locale.setlocale(locale.LC_ALL, 'en_GB.UTF-8')
# act = sh.worksheet("Actions & Insights")
# ws_act = pd.DataFrame(act.get_all_records())

def main():
    # Get account manager emails and client lists with type
    clients = config()
    em = emails()
    # Loop through the list and execute all the functions
    for client, client_type in clients.items():
        # b1 = wwd(client)
        # b2 = insights(client)

        if client_type == 'Lead Gen':
            data = data_lead(client)
            kpist = ["Conversions", "CPA"]
            metricst = ["Impressions", "Clicks", "Cost", "Conversions", "CTR", "CPC", "Conversion Rate", "CPA"]

        else:
            data = data_ecom(client)
            kpist = ["Transactions", "Transaction Revenue", "CPA", "ROAS"]
            metricst = ["Impressions", "Clicks", "Cost", "Transactions", "Transaction Revenue", "CTR", "CPC", "Conversion Rate", "CPA", "ROAS"]

        # b5 = actions(client)
        kpisd = kpis(data, client_type)
        kpisl = list(kpisd.keys())
        kpisy = list(kpisd.values())
        for i in range(len(kpisy)):
            kpisy[i] = str(kpisy[i]) + "%"

        for i in range(len(kpist)):
            if kpist[i] == 'Impressions' or kpist[i] == 'Clicks' or kpist[i] =='Conversions' or kpist[i] =='Transactions':
                kpisl[i] = int(kpisl[i])
            elif kpist[i] == 'Cost' or kpist[i] == 'Transaction Revenue' or kpist[i] =='CPC' or kpist[i] =='CPA':
                kpisl[i] = locale.currency(kpisl[i], grouping= True)
            else:
                kpisl[i] = str(kpisl[i]) + "%"

        spend_block = costs(data)
        budget = str(ws_config.at[2, client])

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

        actions_list = actions(client)
        action = actions_list[2].pop()
        insights = actions_list[1].pop()
        wwd = actions_list[0].pop()

        print(wwd, insights, action)

        # Initialise the email
        env = Environment(loader=FileSystemLoader('templates'))
        template = env.get_template('email_template.html')
        # Get the date
        yday = (datetime.date.today() - datetime.timedelta(days=1))
        month_start = yday - datetime.timedelta(yday.day - 1)
        #Write and send the email
        html = template.render(client=client, yday=yday, month_start=month_start, kpist_kpisl_kpisy=zip(kpist,kpisl,kpisy), spend_block=spend_block,
                               metricst_metricsl_metricsy=zip(metricst,metricsl,metricsy), wwd=wwd, insights= insights, actions=action)
        email(html, client, em)
    return 0

# def wwd(client):
#     results = []
#     cell = act.find(client)
#     for i in range(3, 13, 1):
#         if act.cell(i, cell.col).value is None:
#             break
#         else:
#             results.append(act.cell(i, cell.col).value)
#     return results

# def insights(client):
#     results = {}
def config():
    clients = {}
    for column in ws_config:
        clients[column] = ws_config.at[0, column]
    return clients

def emails():
    email = {}
    for column in ws_config:
        email[column] = (ws_config.at[1, column])
    return email

def data_ecom(client):
    ws = sh.worksheet(f"{client} Funnel Import")
    df_raw = pd.DataFrame(ws.get_all_records())
    filt = df_raw['Paid / Organic'] == 'Paid'
    df = df_raw[filt]

    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')

    now = pd.Timestamp.now()
    first = now.replace(day=1).normalize()
    yday = (now - pd.DateOffset(days=1)).normalize()

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

    YoY = {}
    current_year = date.today().year
    previous_year = current_year - 1

    for column in new_df:
        cy = new_df.at[current_year, column]
        if df_raw['Year'].min() < previous_year:
            py = new_df.at[previous_year, column]
        else:
            py = 0
        YoY[column] = ((cy - py) / py) * 100


    tmp_df = pd.DataFrame(YoY, index=['YoY'])
    tmp_df = tmp_df.round(2)
    concat_df = pd.concat([new_df, tmp_df])
    return concat_df

def data_lead(client):
    ws = sh.worksheet(f"{client} Funnel Import")
    df_raw = pd.DataFrame(ws.get_all_records())
    filt = df_raw['Paid / Organic'] == 'Paid'
    df = df_raw[filt]

    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')

    now = pd.Timestamp.now()
    first = now.replace(day=1).normalize()
    yday = (now - pd.DateOffset(days=1)).normalize()

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

    YoY = {}
    current_year = date.today().year
    previous_year = current_year - 1

    for column in new_df:
        cy = new_df.at[current_year, column]
        py = new_df.at[previous_year, column]
        YoY[column] = ((cy - py) / py) * 100

    tmp_df = pd.DataFrame(YoY, index=['YoY'])
    tmp_df = tmp_df.round(2)
    concat_df = pd.concat([new_df, tmp_df])
    return concat_df

def kpis(data, client_type):
    if client_type == "Lead Gen":
        kpis_results = {}
        current_year = date.today().year
        kpis_results[data.at[current_year, data.columns[3]]] = data.at["YoY", data.columns[3]] # Conversions
        kpis_results[data.at[current_year, "CPA"]] = data.at["YoY", "CPA"] # CPA
    else:
        kpis_results = {}
        current_year = date.today().year
        kpis_results[data.at[current_year, data.columns[3]]] = data.at["YoY", data.columns[3]] # Conversion
        kpis_results[data.at[current_year, data.columns[4]]] = data.at["YoY", data.columns[4]] # Coversion Value
        kpis_results[data.at[current_year, "CPA"]] = data.at["YoY", "CPA"] # CPA
        kpis_results[data.at[current_year, "ROAS"]] = data.at["YoY", "ROAS"]# ROAS
    return kpis_results

def costs(data):
    current_year = date.today().year
    current_month = date.today().month
    yday = (date.today().day) - 1
    total_days = calendar.monthrange(current_year, current_month)[1]
    spend = data.at[current_year, data.columns[2]]
    run_rate = (spend / yday) * total_days

    spend = locale.currency(spend, grouping=True)
    run_rate = locale.currency(run_rate, grouping=True)

    costs_results = [spend, run_rate]
    return costs_results

def metrics(data):
    metrics_results = {}
    current_year = date.today().year
    for columns in data:
        metrics_results[data.at[current_year, columns]] = data.at["YoY", columns]
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
