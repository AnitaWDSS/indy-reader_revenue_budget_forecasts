from main import final_df, churn_rate_df
from datetime import date

mask = final_df["calendar_month"] > date(2025, 9, 1)

final_df[mask].groupby(["calendar_month"])[
    ["acquisition", "churn", "trialist_conversions", "converted"]
].sum().reset_index()

final_df[
    (final_df["package_type"] == "DIGITAL Subscriber")
    & (final_df["trial_duration_months"] == 6)
    & (final_df["trial_price_value"] == 1)
    & (final_df["term_cadence_months"] == 12)
    & (final_df["geo"] == "EUR")
    & (final_df["signup_cohort"] == date(2025, 6, 1))
]

# Testing churn rate
churn_rate_df[
    (churn_rate_df["package_type"] == "DIGITAL Subscriber")
    & (churn_rate_df["trial_duration_months"] == 3)
    & (churn_rate_df["trial_price_value"] == 1)
    & (churn_rate_df["term_cadence_months"] == 12)
    & (churn_rate_df["geo"] == "UK")
    & (churn_rate_df["calendar_month"] == date(2025, 10, 1))
]
