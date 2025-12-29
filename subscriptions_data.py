# Need to install google-cloud-bigquery, db-dtypes if not already installed
from google.cloud import bigquery


# Run the following in terminal to authenticate account: gcloud auth application-default login

client = bigquery.Client(project="indy-eng")

# Dimensions used to create categorisation of our new retention curves
splits = [
    "package_type",
    "trial_duration_months",
    "term_cadence",
]

splits_str = ", ".join(splits)

subs_details_rundate = "2025-10-17"

query = f"""

WITH fixed_expiry_date_df AS (
    SELECT * EXCEPT(user_access_expiration_date),
    CASE WHEN LOWER(piano_status) LIKE 'payment failure'
    # Move the expiry date up by 28 days for payment failures as these individuals will not have payed for their last 28 days of the subscription
    OR (LOWER(piano_status) LIKE 'active' AND subscription_grace_period_start_date IS NOT NULL AND RAND() < 0.8)
    THEN DATE_SUB(user_access_expiration_date, INTERVAL 28 DAY)
    ELSE user_access_expiration_date END AS fixed_expiry_date
    FROM
    `indy-eng.reader_revenue_dataform.subscription_details_snapshots_enhanced`
    WHERE
        run_date = "{subs_details_rundate}"
)

SELECT

    {splits_str},

    CAST(
        CASE
            WHEN CAST(fixed_expiry_date AS DATE) > "{subs_details_rundate}"
            #NOTE SOME USERS DO NOT SEEM TO HAVE AN EXPIRY DATE - DYNAMIC ISSUE, IN ANY CASE STILL NEED TO BE ADDED
            OR fixed_expiry_date IS NULL
            THEN DATE_DIFF("{subs_details_rundate}", CAST(start_date AS DATE), MONTH)
            ELSE
                DATE_DIFF(CAST(fixed_expiry_date AS DATE), CAST(start_date AS DATE), MONTH)
        END AS FLOAT64
    ) AS duration,

    CASE
        WHEN CAST(fixed_expiry_date AS DATE) < "{subs_details_rundate}" THEN 1
        ELSE 0
    END AS churned

FROM
    fixed_expiry_date_df

WHERE
  customer_type NOT LIKE "Test"
  AND LOWER(package_type) NOT LIKE "%app%"
  AND customer_type NOT LIKE "Membership"
  AND start_date IS NOT NULL
"""

query_job = client.query(query)
retention_curves_df = query_job.to_dataframe()
retention_curves_df["trial_duration_months"].fillna(0, inplace=True)

