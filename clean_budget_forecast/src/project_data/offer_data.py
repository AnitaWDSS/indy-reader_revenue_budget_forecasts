"""# Offers
---------------------------------
Parameters:

"""

from .subscriptions_data import subs_details_rundate
from .user_base_data import client

query = f"""
WITH
clean_transactions_df AS (
  SELECT DISTINCT
    DATE(Date) AS transaction_date,
    DATE(TIMESTAMP_TRUNC(Date, MONTH)) AS yearmonth,
    piano_uid,
    tax_type,
    transaction_status,
    term_name,
    term_id,
    subscription_id,
    local_price,
    term_price_value,
    trial_price_value,
    expires,
    tax,
    tax_base,
    tax_rate,
    tax_country,
    term_cadence,
    region,
    package_type,
    customer_type,
    term_price,
    trial_duration,
  FROM
    `indy-eng.reader_revenue_dataform.transactions_log_enhanced`
  WHERE
    customer_type NOT LIKE "Test"
    AND DATE <= "{subs_details_rundate}"
  ),
-- Step 3: Net out refunds from payments to obtain what each uid payed per month
monthly_amount_paid AS (
  SELECT
    transaction_date,
    DATE_TRUNC(transaction_date, MONTH) AS transaction_month,
    piano_uid,
    term_id,
    subscription_id,
    trial_price_value,
    term_price_value,
    term_cadence,
    region,
    package_type,
    customer_type,
    ROUND(SUM(local_price),2) AS monthly_amount_paid
  FROM
    clean_transactions_df
  GROUP BY
    ALL
  ORDER BY
    monthly_amount_paid DESC, piano_uid
  ),
-- Step 4: Join transaction data with subscription_details
subs_details_needed AS (
  SELECT
    piano_uid,
    term_id,
    term_name,
    subscription_id,
    start_date,
    piano_status,
    user_access_expiration_date,
    subscription_trial_end_date
  FROM `indy-eng.reader_revenue_dataform.subscription_details_snapshots_enhanced`

  WHERE
    NOT REGEXP_CONTAINS(LOWER(term_name), r'(app | google | apple)') # As there is no transaction data for app subscriptions, I've removed it from subs
    AND run_date = "{subs_details_rundate}"
),
subs_and_trans AS (
  SELECT
    piano_uid,
    term_id,
    subscription_id,
    start_date,
    subscription_trial_end_date,
    user_access_expiration_date,
    transaction_month,
    term_cadence,
    region,
    package_type,
    customer_type,
    transaction_date,
    trial_price_value,
    term_price_value,
    CASE
      WHEN transaction_date < DATE(subscription_trial_end_date) THEN 'trialist'
      WHEN (transaction_date >= DATE(subscription_trial_end_date) OR subscription_trial_end_date IS NULL) THEN 'non_trialist'
      ELSE 'No tenure'
    END AS tenure,
    monthly_amount_paid
  FROM monthly_amount_paid AS mp
  LEFT JOIN subs_details_needed
  USING(
    piano_uid,
    term_id,
    subscription_id
    )
  ORDER BY
    piano_uid,
    term_id,
    subscription_id,
    transaction_month
)

  SELECT
    COUNT(subscription_id) AS number_of_offers,
    region,
    term_cadence,
    package_type,
    customer_type,
    transaction_month,
    monthly_amount_paid
  FROM
    subs_and_trans
  WHERE
    monthly_amount_paid in (49.00, 20.00, 25.00, 34.99, 40.99, 74.99, 86.99)
    AND transaction_month >= '2024-01-01'
    AND customer_type LIKE "Subscription"
    -- AND term_type='Current Annual'
  GROUP BY
    ALL
  ORDER BY
    number_of_offers DESC
"""

query_job = client.query(query)
offers_df = query_job.to_dataframe()

offers_df
