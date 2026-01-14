import gspread
import smtplib
import json
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from jinja2 import Environment, FileSystemLoader
import numpy as np
import locale
from pandas.tseries.offsets import MonthEnd
from get_plans import build_plan_json_from_sheet
from get_funnel_data import get_funnel_data
from get_context_data import get_context_data
from generate_commentary import generate_commentary

pd.options.mode.chained_assignment = None  # default='warn'
np.seterr(divide='ignore', invalid='ignore')

with open("secrets.json","r") as f:
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


# Initial Config -- Declare Global Variables and initialise datasets
sh = sa.open('Weekly Reports')
cfg = sh.worksheet("Config")
ws_config = pd.DataFrame(cfg.get_all_records()).iloc[:, 1:]
locale.setlocale(locale.LC_ALL, 'en_GB.UTF-8')
# Time Variables
now = pd.Timestamp.now()
yday = (now - pd.DateOffset(days=2)).normalize()
if now.day <= 5:
    now = now - MonthEnd(1)
    yday = now
first_of_current_month = now.replace(day=1).normalize()
end_of_current_month = now + pd.offsets.MonthEnd(0)


def main():
    # Initialise the client list
    clients = init_clients()
    for client in clients:
        #if client['report_due_date'] != datetime.today().strftime("%A"):
            #continue
        print(client['name'])
        if client["plan"] != "":
            plans = build_plan_json_from_sheet(client)  # now returns {"full": ..., "report_window": ...}
            client["plan_json_full"] = plans["full"]
            client["plan_json"] = plans["report_window"] 
        client = get_funnel_data(client)
        client['site_context'] = get_context_data(client)
        client['commentary'] = generate_commentary(client)
        email_template = create_email_template(client)
        send_email(client, email_template)
    return 0

# Initialise the client 
def init_clients():
    clients = []
    for column in ws_config:
        clients_tmp = {}
        clients_tmp['name'] = column
        clients_tmp['account_type'] = ws_config.at[0, column]
        clients_tmp['dashboard'] = ws_config.at[1, column]
        clients_tmp['plan'] = ws_config.at[4, column]
        clients_tmp['budget'] = ws_config.at[2, column]
        clients_tmp['dimension'] = ws_config.at[3, column]
        clients_tmp['start_date'] = first_of_current_month
        clients_tmp['end_date'] = end_of_current_month
        clients_tmp['report_due_date'] = ws_config.at[5, column]
        clients_tmp['client_context'] = ws_config.at[6, column]
        clients_tmp['start_date_string'] = clients_tmp['start_date'].normalize().strftime("%d/%m/%Y") 
        if yday < clients_tmp['end_date']:
            clients_tmp['end_date_string'] = yday.normalize().strftime("%d/%m/%Y")
        else:
            clients_tmp['end_date_string'] = clients_tmp['end_date'].normalize().strftime("%d/%m/%Y")
        clients.append(clients_tmp)
    return clients    

# Input data into email template
def create_email_template(client):
    env = Environment(loader=FileSystemLoader('templates'))
    template = env.get_template('email_template_2025.html')
    html_email = template.render(client=client)
    return(html_email)


# Send email
def send_email(client, html):
    # Define email & password for sender email

    wr_email = secrets["email"]
    wr_password = secrets["password"]

    # Create a message object
    msg = MIMEMultipart()
    msg['From'] = wr_email
    msg['Subject'] = f"{client['name']} Weekly Report"

    # Add the HTML body to the message
    body = MIMEText(html, 'html')
    msg.attach(body)

    # Send the message
    with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(wr_email, wr_password)
        smtp.sendmail(wr_email, secrets["send_email_test"], msg.as_string()) 
main()