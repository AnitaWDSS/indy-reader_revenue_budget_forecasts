"""
Parameters:
1. start_month: The starting month for the acquisition forecast
"""  # Import necessary libraries

import pandas as pd
import numpy as np
from src.models.retention_curves import apply_km
from src.project_data.subscriptions_data import retention_curves_df, splits
from src.project_data.user_base_data import cohort_df
from datetime import date
from src.models.new_cohorts_generator import generate_new_cohorts

# Produce Kaplan-Meier retention curves
retention_curves = retention_curves_df.groupby(splits, group_keys=False).apply(apply_km)

# Calculating recurring donation acquisition, students and COP (Based on 6-month average
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
