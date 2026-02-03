import sys
from pathlib import Path

# Add parent of parent of parent to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from main import cohort_joined_converted_df
from datetime import date

"""## Revenue Testing"""

dimensions = [
    "package_type",
    "trial_duration_months",
    # 'term_cadence',
    "trial_price_value",
    # 'trial_price',
    # 'term_price',
    "term_price_value",
    "payment_currency",
    "geo",
    "signup_cohort",
    # 'trial_duration',
    "calendar_month",
    "month_index",
    "total_cohort_users",
    "retention_curve_rate",
    "piecewise_retention_rate",
    "is_trialist",
    "GBP_Conversion",
    "paying_amount",
    "term_cadence_months",
    "current_tenure_cadence",
    "amortised_paying_amount",
]

metrics = [
    "active_users",
    "previous_active_users",
    "predicted_active_users",
    "predicted_amortised_revenue",
    "converted_predicted_amortised_revenue",
]

final_df = cohort_joined_converted_df.groupby(dimensions)[metrics].sum().reset_index()

final_df["trial_price_value"] = final_df["trial_price_value"].astype("float")

cohort_joined_converted_df[
    (cohort_joined_converted_df["package_type"] == "DIGITAL Subscriber")
    & (cohort_joined_converted_df["trial_duration_months"] == 6)
    & (cohort_joined_converted_df["trial_price_value"] == "1")
    & (cohort_joined_converted_df["term_cadence_months"] == 12)
    & (cohort_joined_converted_df["geo"] == "EUR")
    & (cohort_joined_converted_df["signup_cohort"] == date(2025, 6, 1))
]

final_df[
    (final_df["package_type"] == "DIGITAL Subscriber")
    & (final_df["trial_duration_months"] == 6)
    & (final_df["trial_price_value"] == 1.00)
    & (final_df["term_cadence_months"] == 12)
    & (final_df["geo"] == "UK")
    & (final_df["signup_cohort"] == date(2025, 9, 1))
]

mask = cohort_joined_converted_df["calendar_month"] == date(2025, 9, 1)

cohort_joined_converted_df[mask].groupby(["package_type"])[
    ["converted_predicted_amortised_revenue", "predicted_active_users"]
].sum().reset_index()

mask = cohort_joined_converted_df["calendar_month"] > date(2025, 8, 1)
mask_2027 = cohort_joined_converted_df["calendar_month"] >= date(2027, 1, 1)

cohort_joined_converted_df[mask].groupby(["calendar_month"])[
    ["converted_predicted_amortised_revenue", "predicted_active_users"]
].sum().reset_index()

cohort_joined_converted_df[mask_2027][
    ["converted_predicted_amortised_revenue", "predicted_active_users"]
].sum().reset_index()

print(
    cohort_joined_converted_df[mask_2027][
        ["converted_predicted_amortised_revenue", "predicted_active_users"]
    ]
    .sum()
    .reset_index()
)
