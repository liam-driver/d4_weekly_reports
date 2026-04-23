import pandas as pd
import numpy as np

def safe_div(num, den, multiplier=1.0, default=0.0):
    # pandas Series / DataFrame path
    if isinstance(num, (pd.Series, pd.DataFrame)) or isinstance(den, (pd.Series, pd.DataFrame)):
        den = den.replace(0, np.nan)
        result = (num / den) * multiplier
        return result.fillna(default)
    # scalar path
    if den == 0 or pd.isna(den) or pd.isna(num):
        return default
    return (num / den) * multiplier