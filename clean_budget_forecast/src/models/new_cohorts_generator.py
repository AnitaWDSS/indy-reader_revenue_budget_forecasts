# Introducing new cohorts that will be acquired in 2026
from src.project_data.subscriptions_data import splits
import pandas as pd
import numpy as np

added_splits = ["geo", "term_price", "trial_price", "trial_duration"]


def generate_new_cohorts(cohorts_df, forecast_months=27):
    cohorts_df = cohorts_df.copy()
    cohorts_df["calendar_month"] = pd.PeriodIndex(
        cohorts_df["calendar_month"], freq="M"
    )

    acq_weights = np.array([0.083, 0.083, 0.084, 0.25, 0.25, 0.25])
    acq_weights = acq_weights / acq_weights.sum()

    future_cohorts = []

    for month_ahead in range(1, forecast_months + 1):
        last_calendar = cohorts_df["calendar_month"].max()  # global last month

        new_month = last_calendar + 1  # next calendar month

        # Process each cohort_split separately
        for cohort_splits, group in cohorts_df[cohorts_df["month_index"] == 0].groupby(
            "cohort_splits"
        ):
            full_range = pd.period_range(
                start=group["calendar_month"].min(), end=last_calendar, freq="M"
            )

            group = (
                group.set_index("calendar_month")
                .reindex(full_range)
                .rename_axis("calendar_month")
                .reset_index()
            )
            group["total_cohort_users"] = group["total_cohort_users"].fillna(0)
            group["active_users"] = group["active_users"].fillna(0)
            for col in splits + added_splits:
                if col in group.columns:
                    group[col] = group[col].ffill().bfill()

            recent = group.sort_values("calendar_month").tail(6)
            # As some terms have been removed in Aug 2025, I have shortened length of months until we consider a term expired to 2
            is_term_active = group.sort_values("calendar_month").tail(2)
            if len(is_term_active) == 0:
                continue

            w = acq_weights[-len(recent) :]
            avg_new_users = round(
                np.average(recent["total_cohort_users"], weights=w), 0
            )

            new_cohort = {
                "signup_cohort": new_month,
                "calendar_month": new_month,
                "month_index": 0,
                "total_cohort_users": avg_new_users,
                "active_users": avg_new_users,
                "cohort_splits": cohort_splits,
            }

            future_cohorts.append(new_cohort)

            # Append immediately so next iteration uses this new cohort
            cohorts_df = pd.concat(
                [cohorts_df, pd.DataFrame([new_cohort])], ignore_index=True
            )

    return pd.DataFrame(future_cohorts)
