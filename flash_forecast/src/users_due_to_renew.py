from google.cloud import bigquery

#  Pulling up users due to renew this month
query = """
SELECT
  piano_uid,
  piano_status,
  term_id,
  subscription_id,
  DATE(start_date) AS start_date,
  subscription_grace_period_start_date,
  subscription_trial_end_date,
  next_billing_date,
  month_index,
  geo,
  term_name,
  trial_cadence,
  trial_duration,
  term_cadence,
  term_price,
  term_price_value,
  trial_price,
  trial_price_value,
  is_trialist,
  customer_type,
  package_type
FROM
  `indy-eng.reader_revenue_dataform.subscription_details_enhanced`
WHERE
  (DATE_TRUNC(next_billing_date, MONTH)=DATE_TRUNC(CURRENT_DATE(), MONTH)
  # Needed the below as transactions log oly updated once a day in the morning
  OR last_billing_date = CURRENT_DATE())
  AND package_type != 'App Subscription'
"""
client = bigquery.Client(project="indy-eng")
query_job = client.query(query)
renewal_due_df = query_job.to_dataframe()
