import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import argparse
import pandas as pd
from core.error_logger import log_error
from core.config_dates import config_dates
from core.get_funnel_data import get_funnel_data
from core.get_run_rate import get_run_rate


class TimestampEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        return super().default(obj)


def fetch_client_data(client_name):
    with open("storage/config.json", "r") as f:
        clients = json.load(f)

    client = next((c for c in clients if c['name'] == client_name), None)
    if client is None:
        raise ValueError(f"Client '{client_name}' not found in config")

    client = config_dates(client)

    try:
        if client["plan"] != "":
            with open("storage/plans.json", "r") as f:
                plans = json.load(f)
            client["plan_json"] = plans[client["name"]]
    except Exception as e:
        log_error(f"{client['name']} fetch_data: misconfigured 90 Day Plan: {e}")
        raise

    try:
        if client['account_type'] == 'Lead Gen':
            client['paid_data'] = get_funnel_data(client, 'paid_lead_gen')
            client['llm_data'] = get_funnel_data(client, 'llm_lead_gen')
            client['timeseries_data'] = get_funnel_data(client, 'time_series_lead_gen')
            client['overall_data'] = get_funnel_data(client, 'overall_lead_gen')
        elif client['account_type'] == 'Ecommerce':
            client['paid_data'] = get_funnel_data(client, 'paid_ecommerce')
            client['llm_data'] = get_funnel_data(client, 'llm_ecommerce')
            client['timeseries_data'] = get_funnel_data(client, 'time_series_ecommerce')
            client['overall_data'] = get_funnel_data(client, 'overall_ecommerce')
        client['run_rate'] = get_run_rate(client)
    except Exception as e:
        log_error(f"{client['name']} fetch_data: misconfigured Paid Data: {e}")
        raise

    output_path = f"storage/{client_name}_data.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(client, f, ensure_ascii=False, indent=2, cls=TimestampEncoder)

    print(f"Data written to {output_path}")
    return client


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--client", required=True, help="Client name as it appears in config.json")
    args = parser.parse_args()
    fetch_client_data(args.client)
