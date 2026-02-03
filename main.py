import json
from error_logger import log_error
from config_dates import config_dates
from get_funnel_data import get_funnel_data
from get_context_data import get_context_data
from generate_commentary import generate_commentary
from send_email import send_email


def main():
    # Initialise the client list
    with open("storage/config.json", "r") as config_json:
        clients = json.load(config_json)
    for client in clients:
        # if client['report_due_date'] != datetime.today().strftime("%A"):
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
        client = get_funnel_data(client)

        # try:
        #     client = get_funnel_data(client)
        # except:
        #     log_error(f"{client['name']} Report Skipped: misconfigured Funnel Data Export")
        #     continue
        try:
            client['site_context'] = get_context_data(client)
        except:
            log_error(f"{client['name']} Report Skipped: misconfigured Site Context from GA$")
            continue
        try:
            client['commentary'] = generate_commentary(client)
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