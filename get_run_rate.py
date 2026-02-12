import pandas as pd
import locale

def get_run_rate(client):
    now = pd.Timestamp.now()
    yday = (now - pd.DateOffset(days=2)).normalize()
    spend = float(client['paid_data']['Total']['Cost']['curr'].replace('£', '').replace(',', '').strip())
    day_diff_yday = ((yday - client['start_date']).days)
    day_diff_month = ((client['end_date'] - client['start_date']).days) + 1
    run_rate = (spend / day_diff_yday) * day_diff_month
    run_rate = locale.currency(run_rate, grouping=True)
    return(run_rate)