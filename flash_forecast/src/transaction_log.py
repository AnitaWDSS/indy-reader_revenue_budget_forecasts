"""
Transaction Log Data
--------------------------------
Parameters:
"""

# Need to install google-cloud-bigquery, db-dtypes if not already installed
from google.cloud import bigquery

# Run the following in terminal to authenticate account: gcloud auth application-default login

client = bigquery.Client(project="indy-eng")

"""# Initial Import"""

# Uploading transactions_enhanced dataset

query = """
SELECT
    # piano_uid,  transactions_log_enhanced equvalent
    uid,
    tax_type,
    status,
    term_name,
    term_id,
    subscription_id,
    local_price,
    currency AS currency_raw,
    CASE WHEN (currency = '$' OR price LIKE '%USD%' OR price LIKE '%$%') THEN 'USD' ELSE currency END AS currency,
    price,
    expires,
    price,
    expires,
    tax,
    tax_base,
    tax_rate,
    tax_country,
    # term_cadence, transactions_log_enhanced equvalent
    cadence,
    region,
    term_type,
    tenure,
    term_price,
    trial_price,
    trial_cadence,
    # CASE WHEN subscription_trial_end_date >= date THEN 'trialist' ELSE 'tenured' END AS tenure,
    # Only to be included when we can transitions to transaction_log_enhanced
    tax_base,
    YearMonth,
    # year_month, transactions_log_enhanced equvalent
    date
FROM `indy-eng.users_v2.transactions_enhanced`
"""

query_job = client.query(query)
transactions_df = query_job.to_dataframe()

# Check max_date passed (Day before current date)
#  Check total number of currencies in raw data (should only be GBP,USD,EUR)
