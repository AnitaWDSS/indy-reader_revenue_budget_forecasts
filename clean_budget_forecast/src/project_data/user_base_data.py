"""User Bases
--------------------------------
Parameters:
- latest_date: The latest date for which we want to include cohorts
- forecast_to_date: The date up to which we want to forecast active users

Dependencies:
- subscriptions_data.py: For splits and subs_details_rundate

Future Improvements:
- Automate the calculation of payment_failure_loss_rate based on recent data
"""

# Is this the right to way to import from another file in the same directory?
from .subscriptions_data import splits, subs_details_rundate
from google.cloud import bigquery
from datetime import date

base_splits = splits + [
    "trial_price_value",
    "trial_price",
    "term_price",
    "term_price_value",
    "payment_currency",
    "region",
    "signup_cohort",
    "trial_duration",
]
base_splits_str = ", ".join(base_splits)

start_date = "2015-01-01"
# Change the below to adjust the parameters of the cohorts included
latest_date = "2025-09-01"
forecast_to_date = "2027-12-01"

# Based on 6-month average (March-July 2025)
payment_failure_loss_rate = 0.8

# Accessing BQ
client = bigquery.Client(project="indy-eng")

query = f"""
WITH
# Creating calendar dates to fill in missing months + add future months
  calendar AS (
    SELECT calendar_month
    FROM UNNEST(GENERATE_DATE_ARRAY('{start_date}', '{forecast_to_date}', INTERVAL 1 MONTH)) AS calendar_month
  ),

  subs_details AS (
  SELECT
# Base splits are all the different cohorts in our user base: This includes the retention curves splits and the sign up cohorts
    {base_splits_str},

    calendar_month,
#  This pulls expiration date back 28 days for (potential) payment failures as these individuals will not make any payments on their last 28 days (this makes forecasting revenue more accurate)
    DATE(
      DATE_TRUNC(
        CASE
          WHEN (LOWER(piano_status) LIKE 'payment failure') OR (LOWER(piano_status) LIKE 'active' AND subscription_grace_period_start_date IS NOT NULL AND RAND() < {payment_failure_loss_rate})
            THEN DATE_SUB(user_access_expiration_date, INTERVAL 28 DAY)
          ELSE
            user_access_expiration_date
        END
      , MONTH))
    AS expire_month,

    DATE(DATE_TRUNC(subscription_trial_end_date, MONTH)) AS trial_end_month,

  FROM
    `indy-eng.reader_revenue_dataform.subscription_details_snapshots_enhanced`
  JOIN
    calendar ON calendar.calendar_month >= DATE(DATE_TRUNC(start_date, MONTH))
# Membership is removed here as it is included in the final forecast doc
  WHERE
     customer_type NOT LIKE "Test"
  AND LOWER(package_type) NOT LIKE "%app%"
  AND customer_type NOT LIKE "Membership"
  AND start_date IS NOT NULL
  AND is_blacklisted IS FALSE
  AND run_date = "{subs_details_rundate}"
  )

SELECT

  {base_splits_str},

  calendar_month,
  DATE_DIFF(calendar_month, signup_cohort, month) AS month_index,

  SUM(
    CASE
        WHEN
            (calendar_month BETWEEN signup_cohort AND (expire_month - 1))
            # OR (expire_month IS NULL)
        THEN 1
    ELSE 0 END) AS active_users,

  MAX(COUNT(*)) OVER(PARTITION BY {base_splits_str}) as total_cohort_users

FROM
  subs_details
WHERE
  signup_cohort <= '{latest_date}'

GROUP BY
    ALL
ORDER BY
    signup_cohort, calendar_month
"""

query_job = client.query(query)
cohort_df = query_job.to_dataframe()

# Filling in any empty entries w/ null values
cohort_df["trial_duration_months"].fillna(0, inplace=True)
cohort_df["trial_price_value"].fillna(0, inplace=True)

# Check for NA values
mask = cohort_df["calendar_month"] >= date(2025, 11, 1)
cohort_df.loc[mask, "active_users"] = 0

cohort_df.isna().sum()

# Checking historical base
mask = cohort_df["calendar_month"] > date(2025, 7, 1)

cohort_df[mask].groupby(["calendar_month"])["active_users"].sum().reset_index()
print(cohort_df[mask].groupby(["calendar_month"])["active_users"].sum().reset_index())
