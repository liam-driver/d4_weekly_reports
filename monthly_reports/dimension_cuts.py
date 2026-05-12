import os
import json
import pandas as pd
from core.get_funnel_data import initialise_df, apply_filters, pivot_df, df_to_json
from core.safe_div import safe_div

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Additive raw column names → canonical metric name.
# Each entry is a list of candidate column names tried in order.
_ADDITIVE_METRIC_CANDIDATES = [
    ('Cost',               ['Cost (GBP)', 'Cost']),
    ('Transaction Revenue', ['Transaction Revenue (GBP)', 'Transaction Revenue']),
    ('Conversions',        ['Conversions']),
    ('Impressions',        ['Impressions']),
    ('Clicks',             ['Clicks']),
    ('Transactions',       ['Transactions']),
]


def _load_dimension_config():
    path = os.path.join(PROJECT_ROOT, 'storage', 'dimension_config.json')
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _find_column(columns_set, candidates):
    """Return the first candidate that exists in the column set, or None."""
    for name in candidates:
        if name in columns_set:
            return name
    return None


def get_available_dimensions(client):
    """
    Initialises the client DataFrame and returns the subset of dimension_config.json
    entries whose column_name is present in the sheet headers.

    Returns a list of dicts: [{column_name, label, requires_channel_filter}, ...]
    """
    df = initialise_df(client)
    columns = set(df.columns.tolist())
    config = _load_dimension_config()
    return [entry for entry in config if entry['column_name'] in columns]


def get_dimension_cut(client, dimension_column, channel_filter=None):
    """
    Fetches MoM comparison data sliced by dimension_column using string-based
    column matching (not index-based).

    client must have start_date, end_date, compare_start_date, compare_end_date
    set to MoM window (Timestamp objects).

    channel_filter: dict {"type": "include"|"exclude", "channels": [...]} or None.
    Returns: dict matching paid_data_mom JSON shape, keyed by dimension value.
    """
    from weekly_reports.generate_df import get_total_row

    df = initialise_df(client)

    if dimension_column not in df.columns:
        raise ValueError(
            f"Column '{dimension_column}' not found in sheet. "
            f"Available columns: {df.columns.tolist()}"
        )

    account_type = client.get('account_type', 'Ecommerce')
    table_type = 'paid_lead_gen' if account_type == 'Lead Gen' else 'paid_ecommerce'

    breakdown_dimension = [dimension_column, 'Period']
    date_range = {
        'start_date':         client['start_date'],
        'end_date':           client['end_date'],
        'compare_start_date': client['compare_start_date'],
        'compare_end_date':   client['compare_end_date'],
    }

    df = apply_filters(df, client, breakdown_dimension, date_range)

    # Apply optional Ad Channel include/exclude filter
    if channel_filter:
        channels = channel_filter.get('channels', [])
        if channels:
            if channel_filter.get('type') == 'exclude':
                df = df[~df['Ad Channel'].isin(channels)]
            else:
                df = df[df['Ad Channel'].isin(channels)]

    columns_set = set(df.columns.tolist())

    # Discover which additive metrics exist in the sheet
    selected = {}  # canonical_name -> actual column name
    for canonical, candidates in _ADDITIVE_METRIC_CANDIDATES:
        col = _find_column(columns_set, candidates)
        if col is not None:
            selected[canonical] = col

    if not selected:
        raise ValueError(
            f"No recognised metric columns found for dimension cut on '{dimension_column}'."
        )

    # Build a clean working DataFrame with only the columns we need
    work_cols = [breakdown_dimension[1], breakdown_dimension[0]] + list(selected.values())
    df_work = df[work_cols].copy()
    df_work[list(selected.values())] = df_work[list(selected.values())].apply(
        pd.to_numeric, errors='coerce'
    )
    df_work = df_work.groupby(
        [breakdown_dimension[1], breakdown_dimension[0]], as_index=False
    ).sum()

    # Rename raw column names to canonical names
    rename_map = {v: k for k, v in selected.items()}
    df_work = df_work.rename(columns=rename_map)

    # Add total row per period (matches pattern in generate_df.py)
    curr_df = get_total_row(df_work[df_work['Period'].eq('Current')].copy(), 'Current')
    prev_df = get_total_row(df_work[df_work['Period'].eq('Previous')].copy(), 'Previous')
    df_work = pd.concat([curr_df, prev_df], ignore_index=True)

    # Compute derived metrics from summed additive columns
    if 'Cost' in df_work.columns and 'Transaction Revenue' in df_work.columns:
        df_work['ROAS'] = safe_div(
            df_work['Transaction Revenue'], df_work['Cost'], multiplier=100
        )
    if 'Cost' in df_work.columns and 'Conversions' in df_work.columns:
        df_work['CPA'] = safe_div(
            df_work['Cost'], df_work['Conversions'], multiplier=1
        )
    if 'Impressions' in df_work.columns and 'Clicks' in df_work.columns:
        df_work['CTR'] = safe_div(
            df_work['Clicks'], df_work['Impressions'], multiplier=100
        )
    if 'Clicks' in df_work.columns and 'Cost' in df_work.columns:
        df_work['CPC'] = safe_div(
            df_work['Cost'], df_work['Clicks'], multiplier=1
        )
    conv_col = (
        'Transactions' if 'Transactions' in df_work.columns
        else ('Conversions' if 'Conversions' in df_work.columns else None)
    )
    if 'Clicks' in df_work.columns and conv_col:
        df_work['Conversion Rate'] = safe_div(
            df_work[conv_col], df_work['Clicks'], multiplier=100
        )
    if 'Transactions' in df_work.columns and 'Transaction Revenue' in df_work.columns:
        df_work['AOV'] = safe_div(
            df_work['Transaction Revenue'], df_work['Transactions'], multiplier=1
        )

    metrics = [col for col in df_work.columns if col not in breakdown_dimension]
    df_pivot = pivot_df(df_work, breakdown_dimension, metrics, table_type)
    return df_to_json(df_pivot, breakdown_dimension, metrics, table_type)


def fetch_and_append_dimension_cut(client_name, dimension, channel_filter=None):
    """
    Loads the cached monthly JSON, fetches MoM dimension cut data, generates
    insight commentary, appends the result to dimension_cuts, and persists.

    dimension: column_name string (e.g. 'Campaign').
    channel_filter: dict or None.
    Returns: the new dimension cut entry dict.
    """
    from core.generate_commentary import generate_dimension_cut_commentary

    data_path = os.path.join(PROJECT_ROOT, 'storage', f'{client_name}_monthly_data.json')
    with open(data_path, 'r', encoding='utf-8') as f:
        client = json.load(f)

    # Restore Timestamp objects that JSON serialisation converted to ISO strings
    for key in ('start_date', 'end_date', 'compare_start_mom', 'compare_end_mom'):
        if key in client and isinstance(client[key], str):
            client[key] = pd.Timestamp(client[key])

    # Dimension cuts always use MoM comparison dates
    client['compare_start_date'] = client['compare_start_mom']
    client['compare_end_date']   = client['compare_end_mom']

    data_mom = get_dimension_cut(client, dimension, channel_filter)

    # Find the human-readable label from dimension_config
    config = _load_dimension_config()
    label = next(
        (e['label'] for e in config if e['column_name'] == dimension),
        dimension
    )

    commentary = generate_dimension_cut_commentary(
        data_mom=data_mom,
        label=label,
        channel_filter=channel_filter,
        client=client,
    )

    cut_entry = {
        'dimension':      dimension,
        'label':          label,
        'channel_filter': channel_filter,
        'data_mom':       data_mom,
        'commentary':     commentary,
    }

    if 'dimension_cuts' not in client or not isinstance(client['dimension_cuts'], list):
        client['dimension_cuts'] = []
    client['dimension_cuts'].append(cut_entry)

    # Persist: convert Timestamps back to ISO strings for JSON
    from monthly_reports.main import TimestampEncoder
    with open(data_path, 'w', encoding='utf-8') as f:
        json.dump(client, f, ensure_ascii=False, indent=2, cls=TimestampEncoder)

    return cut_entry
