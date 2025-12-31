"""
Parameters:
1. start_month: The starting month for the acquisition forecast
Future Improvements:
1. Integrate validation checks to ensure data integrity during merges.
"""
# Import necessary libraries

import pandas as pd
import numpy as np
from src.models.retention_curves import apply_km
from src.project_data.subscriptions_data import retention_curves_df, splits
from src.project_data.user_base_data import cohort_df
from datetime import date
from src.models.new_cohorts_generator import generate_new_cohorts
from src.project_data.acquisition_data import combined
from src.models.fill_in_acq_terms import extend_aquisition_data

# Produce Kaplan-Meier retention curves
retention_curves = retention_curves_df.groupby(splits, group_keys=False).apply(apply_km)

""" Calculating acquisition forecast for special terms (donations, students, COP) """

start_month = date(2025, 10, 1)

acq_forecast_cohort_df = cohort_df[cohort_df["calendar_month"] < start_month].copy()


#  Adding unique keys for retention curve splits and cohort splits
retention_curve_splits = [split for split in splits if split != "trial_duration_months"]
added_splits = ["region", "term_price", "trial_price", "trial_duration"]


acq_forecast_cohort_df["retention_curve_splits"] = acq_forecast_cohort_df[
    retention_curve_splits
].apply(lambda row: ",".join(row.values.astype(str)), axis=1)
acq_forecast_cohort_df["cohort_splits"] = acq_forecast_cohort_df[
    retention_curve_splits + added_splits
].apply(lambda row: ",".join(row.values.astype(str)), axis=1)

future_cohorts = generate_new_cohorts(acq_forecast_cohort_df.copy()).sort_values(
    ["cohort_splits", "signup_cohort", "calendar_month"]
)

future_cohorts[
    [
        "package_type",
        "term_cadence",
        "region",
        "term_price",
        "trial_price",
        "trial_duration",
    ]
] = future_cohorts["cohort_splits"].str.split(",", expand=True)
future_cohorts["retention_curve_splits"] = future_cohorts[retention_curve_splits].apply(
    lambda row: ",".join(row.values.astype(str)), axis=1
)

future_cohorts["signup_cohort"] = future_cohorts["signup_cohort"].astype(str) + "-01"

future_cohorts["calendar_month"] = future_cohorts["calendar_month"].astype(str) + "-01"


#  Set all future cohorts to be non-trialists except current monthly and current annual
future_cohorts["trial_status"] = "non-trialist"

active_subs_terms = (
    (future_cohorts["package_type"] == "DIGITAL Subscriber")
    & (future_cohorts["term_cadence"].isin(["month", "year"]))
    & (
        future_cohorts["term_price"].isin(
            ["GBP11.00", "EUR11.00", "GBP99.00", "EUR99.00"]
        )
    )
    & (future_cohorts["trial_price"].isin(["GBP1.00", "EUR1.00"]))
    & (future_cohorts["trial_duration"] == "6 month")
)

future_cohorts.loc[active_subs_terms, "trial_status"] = "trialist"

""" Final acquisition forecast dataframe """

final_forecasted_subs = combined[
    [
        "date",
        "region",
        "package_type",
        "term_cadence",
        "trial_duration",
        "trial_price",
        "term_price",
        "month_index",
        "forecasted_subs",
    ]
]
final_forecasted_subs["cohort_splits"] = final_forecasted_subs[
    retention_curve_splits + added_splits
].apply(lambda row: ",".join(row.values.astype(str)), axis=1)

# Ensure key columns are same type
future_cohorts["cohort_splits"] = future_cohorts["cohort_splits"].astype(str)
final_forecasted_subs["cohort_splits"] = final_forecasted_subs["cohort_splits"].astype(
    str
)

final_forecasted_subs["date"] = final_forecasted_subs["date"].astype(str)

merged = future_cohorts.merge(
    final_forecasted_subs[["date", "cohort_splits", "forecasted_subs"]],
    left_on=["calendar_month", "cohort_splits"],
    right_on=["date", "cohort_splits"],
    how="left",
)

## NEED TO IMPROVE VALIDATION CHECKS HERE FOR MERGE BETWEEN ALL ACQ. DATAFRAMES
merged.loc[merged["forecasted_subs"].notna(), "active_users"] = merged[
    "forecasted_subs"
]
merged.loc[merged["forecasted_subs"].notna(), "total_cohort_users"] = merged[
    "forecasted_subs"
]

acquisition_df = merged.copy()

acquisition_df.drop(
    [
        "retention_curve_splits",
        "cohort_splits",
        "date",
        "forecasted_subs",
        "trial_status",
    ],
    axis=1,
    inplace=True,
)

final_future_cohorts = acquisition_df[
    pd.to_datetime(acquisition_df["calendar_month"]).dt.date >= date(2025, 10, 1)
]

# Extending final_future_cohorts to reach 2027-12, for past terms add 0 active users to merge with user base data

extend_aquisition_splits = [
    "signup_cohort",
    "package_type",
    "term_cadence",
    "region",
    "term_price",
    "trial_price",
    "trial_duration",
    "total_cohort_users",
]

extended_aquisition_data_extended = (
    final_future_cohorts.groupby(extend_aquisition_splits)
    .apply(extend_aquisition_data, include_groups=False)
    .reset_index()
)

extended_aquisition_data_extended["trial_price_value"] = (
    extended_aquisition_data_extended["trial_price"]
    .str.extract(r"(\d+\.?\d*)")[0]
    .astype(float)
    .fillna(0)
)
extended_aquisition_data_extended["term_price_value"] = (
    extended_aquisition_data_extended["term_price"]
    .str.extract(r"(\d+\.?\d*)")[0]
    .astype(float)
    .fillna(0)
)

extended_aquisition_data_extended.groupby("signup_cohort")["active_users"].sum()
