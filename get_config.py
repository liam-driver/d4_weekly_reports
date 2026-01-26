import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from main import log_error



def init_clients():
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
    clients = []
    for column in ws_config:
        clients_tmp = {}
        # Mandatory
        clients_tmp['name'] = column
        if clients_tmp['name'] == '' or clients_tmp['name'] is None:
            log_error("Client Initialisation Skipped: missing 'Name' in config sheet")
            continue
        clients_tmp['account_type'] = ws_config.at[0, column]
        if clients_tmp['account_type'] != 'Lead Gen' and clients_tmp['account_type'] != 'Ecommerce':
            log_error("Client Initialisation Skipped: missing 'Account Type' in config sheet")
            continue
        clients_tmp['plan'] = ws_config.at[4, column]
        if clients_tmp['plan'] == '' or clients_tmp['plan'] is None:
            log_error("Client Initialisation Skipped: missing 'Plan' in config sheet")
            continue
        clients_tmp['report_due_date'] = ws_config.at[5, column]
        if clients_tmp['report_due_date'] == '' or clients_tmp['report_due_date'] is None:
            log_error("Client Initialisation Skipped: missing 'Day of Week' in config sheet")
            continue
        clients_tmp['data_config'] = ws_config.at[7, column]
        if clients_tmp['data_config'] == 'FALSE':
            log_error("Client Initialisation Skipped: missing 'data configuration' in config sheet")
            continue

        # Optional
        clients_tmp['dashboard'] = ws_config.at[1, column]
        clients_tmp['budget'] = ws_config.at[2, column]
        clients_tmp['dimension'] = ws_config.at[3, column]
        clients_tmp['client_context'] = ws_config.at[6, column]
        
        clients.append(clients_tmp)
    with open("config.json", "w", encoding="utf-8") as f:
        json.dump(clients, f, ensure_ascii=False, indent=2)
    return 0

init_clients()