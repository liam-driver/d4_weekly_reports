
import gspread
import pandas as pd

# API call
sa = gspread.service_account() #(filename=serene-lotus-379510-b3f9b3b23758)
# Sheet
sh = sa.open('Weekly Reports')
# Config sheet
cfg = sh.worksheet("Config")
ws_config = pd.DataFrame(cfg.get_all_records())

def main():
    clients = config()
    email = emails()
    print(clients, email)
def config():
    clients = {}
    for column in ws_config:
        clients[column] = ws_config.at[0, column]
    return clients

def emails():
    email = []
    for column in ws_config:
        email.append(ws_config.at[1, column])
    return email
main()