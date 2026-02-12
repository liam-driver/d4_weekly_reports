import json
from datetime import datetime
from error_logger import log_error
from config_dates import config_dates
from get_funnel_data import get_funnel_data, get_llm_data
from get_run_rate import get_run_rate
from generate_commentary import generate_commentary
from send_email import send_email


def main():
    # Initialise the client list
    with open("storage/config.json", "r") as config_json:
        clients = json.load(config_json)
    for client in clients:
        # if client['report_due_date'] != datetime.today().strftime("%A"):
        #     continue
        # if client['name'] != 'Paintnuts':
        #     continue
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
        if client['account_type'] == 'Lead Gen':
            client['paid_data'] = get_funnel_data(client, client['dimension'], 'paid_lead_gen')
            client['llm_data'] = get_llm_data(client, 'Ad Platform', 'llm_lead_gen')
            client['overall_data'] = get_funnel_data(client, 'Channel', 'overall_lead_gen')
        if client['account_type'] == 'Ecommerce':
            client['paid_data'] = get_funnel_data(client, client['dimension'], 'paid_ecommerce')
            client['llm_data'] = get_llm_data(client, 'Ad Platform', 'llm_ecommerce')
            client['overall_data'] = get_funnel_data(client, 'Channel', 'overall_ecommerce')
        client['run_rate'] = get_run_rate(client)
        # try:
        #     if client['account_type'] == 'Lead Gen':
        #         client['paid_data'] = get_funnel_data(client, client['dimension'], 'paid_lead_gen')
        #         client['llm_data'] = get_llm_data(client, 'Ad Platform', 'llm_lead_gen')
        #         client['overall_data'] = get_funnel_data(client, 'Channel', 'overall_lead_gen')
        #     if client['account_type'] == 'Ecommerce':
        #         client['paid_data'] = get_funnel_data(client, client['dimension'], 'paid_ecommerce')
        #         client['llm_data'] = get_llm_data(client, 'Ad Platform', 'llm_ecommerce')
        #         client['overall_data'] = get_funnel_data(client, 'Channel', 'overall_ecommerce')
        #     client['run_rate'] = get_run_rate(client)
        # except:
        #     log_error(f"{client['name']} Report Skipped: misconfigured Paid Data") 
            # continue
        # Get Email Data
        # client['commentary'] = generate_commentary(client)
        try:
            client['commentary'] = generate_commentary(client)
        except:
            log_error(f"{client['name']} Report Skipped: misconfigured Commentary")
            continue
        send_email(client)
        # try:
        #     send_email(client)
        # except:
        #     log_error(f"{client['name']} Report Skipped: Error Sending Email")
        #     continue
    return 0
  
main()