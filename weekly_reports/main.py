import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
from datetime import datetime
from core.error_logger import log_error
from core.config_dates import config_dates
from core.get_funnel_data import get_funnel_data
from core.get_run_rate import get_run_rate
from core.generate_commentary import generate_weekly_commentary
from send_email import send_email


def main():
    # Initialise the client list
    with open("storage/config.json", "r") as config_json:
        clients = json.load(config_json)
    for client in clients:
        if client['report_due_date'] != datetime.today().strftime("%A"):
            continue
        print(client['name'])
        client = config_dates(client)
        try:
            if client["plan"] != "":
                with open("storage/plans.json", "r") as plans_json:
                    plans = json.load(plans_json)
                client["plan_json"] = plans[client["name"]]
        except:
            log_error(f"{client['name']} Report Skipped: misconfigured 90 Day Plan")
            continue
        try:
            if client['account_type'] == 'Lead Gen':
                client['paid_data'] = get_funnel_data(client, 'paid_lead_gen')
                client['llm_data'] = get_funnel_data(client, 'llm_lead_gen')
                client['timeseries_data'] = get_funnel_data(client, 'time_series_lead_gen')
                client['overall_data'] = get_funnel_data(client, 'overall_lead_gen')
            if client['account_type'] == 'Ecommerce':
                client['paid_data'] = get_funnel_data(client, 'paid_ecommerce')
                client['llm_data'] = get_funnel_data(client, 'llm_ecommerce')
                client['timeseries_data'] = get_funnel_data(client, 'time_series_ecommerce')
                client['overall_data'] = get_funnel_data(client, 'overall_ecommerce')
            client['run_rate'] = get_run_rate(client)
        except:
            log_error(f"{client['name']} Report Skipped: misconfigured Paid Data") 
            continue

        try:
            client['commentary'] = generate_weekly_commentary(client)
        except:
            log_error(f"{client['name']} Report Skipped: misconfigured Commentary")
            continue
        try:
            send_email(client)
        except:
            log_error(f"{client['name']} Report Skipped: Error Sending Email")
            continue
    return 0
  
main()