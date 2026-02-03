import pandas as pd
from pandas.tseries.offsets import MonthEnd


# Config Client object dates
def config_dates(client):
    now = pd.Timestamp.now()
    yday = (now - pd.DateOffset(days=2)).normalize()
    if now.day <= 5:
        now = now - MonthEnd(1)
        yday = now
    first_of_current_month = now.replace(day=1).normalize()
    end_of_current_month = now + pd.offsets.MonthEnd(0)

    client['start_date'] = first_of_current_month
    client['start_date_string'] = client['start_date'].normalize().strftime("%d/%m/%Y") 
    client['end_date'] = end_of_current_month
    if yday < client['end_date']:
        client['end_date_string'] = yday.normalize().strftime("%d/%m/%Y")
    else:
        client['end_date_string'] = client['end_date'].normalize().strftime("%d/%m/%Y")
    return(client)
