"""# Initial Import

Below three key changes are made to the raw transaction data


1.   New `flash_tenure` dimension is created to consider users on their trial end date days as tenured (for amortisation purposes)
2.   To optimise script, only transactions for the last 12 months is taken (also for amortisation purposes)
3. Only transactions categorised as "completed" are considered
"""

from google.cloud import bigquery
# Uploading transactions_enhanced dataset

# Note - is_trialist definition considers a user as trialist on the day of their conversion to tenure
# CASE
# WHEN DATE(subscription_trial_end_date) <CURRENT_DATE()
# THEN 'tenured'
# WHEN subscription_trial_end_date IS NULL
# THEN 'tenured'
# ELSE 'trialist'
# END AS is_trialist

query = """
SELECT
    piano_uid,
    customer_type,
    package_type,
    is_trialist,
    CASE WHEN DATE(subscription_trial_end_date) <= date OR subscription_trial_end_date IS NULL OR trial_cadence = 'No trial'
    THEN 'tenured' ELSE 'trialist' END AS flash_tenure, # Need this exclusively for amortisation - slightly differs vs is_trialist as here, at trial_end_date one is considered tenured
    start_date,
    tax_type,
    transaction_status,
    term_name,
    term_id,
    subscription_id,
    local_price,
    currency,
    price,
    expires,
    tax,
    tax_base,
    tax_rate,
    tax_country,
    term_cadence,
    region,
    term_price,
    trial_price,
    trial_cadence,
    DATE_DIFF(CURRENT_DATE(), DATE(start_date), MONTH) AS month_index,
    year_month,
    date
FROM `indy-eng.reader_revenue_dataform.transactions_log_enhanced`
WHERE DATE_TRUNC(date, MONTH) >= DATE_TRUNC(date, MONTH) - INTERVAL 12 MONTH # Only include last 12 months as money for this month dependent only on the last year of transactions (as annual subs are amortised over 12 months)
AND transaction_status = 'completed' # Removes any pending, cancelled or refunded transanctions (note, refunds already have their own separate negative "completed" transaction)
"""

client = bigquery.Client(project="indy-eng")
query_job = client.query(query)
transactions_df = query_job.to_dataframe()

# Check max_date passed (Day before current date)
#  Check total number of currencies in raw data (should only be GBP,USD,EUR)
