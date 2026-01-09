"""# Refunds"""

from .subscriptions_data import subs_details_rundate
from .user_base_data import start_date, client

# Adding refunds
query = f"""
  WITH clean_transactions_df AS (
  SELECT
  -- Note: DISTINCT included as transactions log has duplicate identical payments not found in Piano
    DISTINCT
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
    trial_cadence,
  FROM
    `indy-eng.reader_revenue_dataform.transactions_log_enhanced`

  WHERE
    -- need to remove tests as these do not represent real subscribers
    LOWER(customer_type) NOT LIKE "test"
    AND DATE <= "{subs_details_rundate}"),

  refunds_this_year AS (
    SELECT DISTINCT
        subscription_id,
        yearmonth,
        term_cadence,
        region,
        package_type,
        customer_type,
        local_price
    FROM clean_transactions_df
    WHERE tax_type='refund'
    AND yearmonth>= "{start_date}"
    AND yearmonth<'2025-10-01'
  )

  SELECT
  ROUND(SUM(local_price),2) AS refund_amount,
  region,
  yearmonth
  FROM refunds_this_year
  GROUP BY ALL
  ORDER BY ROUND(SUM(local_price),2)

"""

query_job = client.query(query)
refunds_df = query_job.to_dataframe()
