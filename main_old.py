import gspread
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from jinja2 import Environment, FileSystemLoader
import datetime
import re

# API call
sa = gspread.service_account() #(filename=serene-lotus-379510-b3f9b3b23758)
# Sheet
sh = sa.open('Weekly Reports')
# Config sheet
config = sh.worksheet("Config")
act = sh.worksheet("Actions & Insights")

def main():
    # Define a list with all the clients
    clients = {}
    clients_list = config.row_values(1)
    del clients_list[0]
    clients_type = config.row_values(2)
    del clients_type[0]
    for key, value in zip(clients_list, clients_type):
            clients[key] = value
    # Loop through the list and execute all the functions
    for client, client_type in clients.items():
        # b1 = wwd(client)
        # b2 = insights(client)
        b3 = kpis(client, client_type)
        b4 = spend(client)
        # b5 = actions(client)

        # Write the email
        env = Environment(loader=FileSystemLoader('templates'))
        template = env.get_template('email_template.html')
        # Get the date
        yday = (datetime.date.today() - datetime.timedelta(days=1))
        month_start = yday - datetime.timedelta(yday.day - 1)
        html = template.render(client=client, yday=yday, month_start=month_start, b3=b3, b4=b4)
        email(html, client)
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

def kpis(client, client_type):
    results = {}
    cell = config.find(client)
    for i in range(3,9,1):
        key = config.cell(i, 1).value
        # If key contains CPA
        if re.search("Conversions / Conversion Value", key):
            if client_type == "Lead Gen":
                key = re.sub(r"Conversions / Conversion Value", "Conversions", key)
            else:
                key = re.sub(r"Conversions / Conversion Value", "Conversion Value", key)
        if re.search("CPA / ROAS", key):
            if client_type == "Lead Gen":
                key = re.sub(r"CPA / ROAS", "CPA", key)
            else:
                key = re.sub(r"CPA / ROAS", "ROAS", key)
        # if re.search(r"Conversions / Conversion Value"):
            # Check the client[key]
                # If ecomm - ROAS
                # If lead gen - CPA
        value = config.cell(i, cell.col).value
        results[key] = value
    return results

def spend(client):
    results = {}
    cell = config.find(client)
    for i in range(9, 13, 1):
        key = config.cell(i, 1).value
        value = config.cell(i, cell.col).value
        results[key] = value
    return results

# def actions(client):
#     results = []
#     cell = act.find(client)
#     for i in range(15, 30, 1):
#         if act.cell(i, cell.col).value is None:
#             break
#         else:
#             results.append(act.cell(i, cell.col).value)
#     return results

def email(html, client):
    # Define email & password for sender email
    wr_email = os.environ.get('WEEKLY_REPORTS_EMAIL')
    wr_password = os.environ.get('WEEKLY_REPORTS_PASSWORD')

    # Create a message object
    msg = MIMEMultipart()
    msg['From'] = wr_email
    msg['To'] = 'liam.driver@door4.com'
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