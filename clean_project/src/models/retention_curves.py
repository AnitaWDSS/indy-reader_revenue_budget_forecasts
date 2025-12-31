"""
Retention Curve Fitting and Extension Module
--------------------------------------

"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from lifelines import KaplanMeierFitter
from src.project_data.subscriptions_data import splits


## ANA COMMENT: Is this used anywhere else? If not, we can remove it.
# Fitting the retention curves, to be cleaned w/ Alex's version of the split
def naive_forecast(survival_df, months_to_extend, plot=False):
    """
    Extends a survival dataframe using a naïve forecast where future retention rates
    remain constant at the last observed value.

    Parameters
    ----------
    survival_df : pd.DataFrame
        DataFrame with columns ['month_index', 'retention_curve_rate'].
    months_to_extend : int
        Number of months to extend the forecast.
    plot : bool, optional
        Whether to plot the historical and forecasted retention curves.

    Returns
    -------
    pd.DataFrame
        Combined DataFrame with both historical and forecasted retention rates.
    """

    last_month = survival_df["month_index"].max()
    last_value = survival_df["retention_curve_rate"].iloc[-1]

    future_months = pd.DataFrame(
        {
            "month_index": np.arange(last_month + 1, last_month + 1 + months_to_extend),
            "retention_curve_rate": last_value,
        }
    )

    forecasted_df = pd.concat([survival_df, future_months], ignore_index=True)

    if plot:
        plt.figure(figsize=(8, 4))
        plt.plot(
            survival_df["month_index"],
            survival_df["retention_curve_rate"],
            label="Historical",
            marker="o",
        )
        plt.plot(
            future_months["month_index"],
            future_months["retention_curve_rate"],
            label="Forecast",
            linestyle="--",
            color="orange",
            marker="o",
        )
        plt.title("Naïve Retention Forecast")
        plt.xlabel("Month Index")
        plt.ylabel("Retention Curve Rate")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()

    return forecasted_df


def extend_retention_curve(
    survival_df: pd.DataFrame,
    trial_months: int,
    term_cadence: str,
    months_to_extend: int = 27,
) -> pd.DataFrame:
    """
    Extend the Kaplan–Meier survival curve by repeating the last observed value
    for a specified number of months.

    Parameters
    ----------
    survival_df : pd.DataFrame
        DataFrame with columns ["month_index", "percentage_of_users"].
    months_to_extend : int, default=12
        Number of additional months to extend the curve.

    Returns
    -------
    pd.DataFrame
        Extended survival DataFrame including the additional months.
    """

    last_month = survival_df["month_index"].max()
    eligible_forecast_months = last_month - trial_months

    tenured_months_df = survival_df[survival_df["month_index"] > trial_months]

    required_periods_to_forecast = 4

    forecast_df = naive_forecast(survival_df, months_to_extend)

    return forecast_df


def apply_km(group: pd.DataFrame) -> pd.DataFrame:
    """
    Fit a Kaplan–Meier survival curve for a given group and extend it

    Parameters
    ----------
    group : pd.DataFrame
        Input data containing 'duration' and 'churned' columns, plus cohort splits.

    Returns
    -------
    pd.DataFrame
        Extended survival DataFrame with cohort identifiers.
    """

    trial_months = group["trial_duration_months"].iloc[0]
    term_cadence = group["term_cadence"].iloc[0]

    kmf = KaplanMeierFitter()

    durations = group["duration"]
    events = group["churned"].astype(int)  # 1 = churned, 0 = censored

    timeline = np.arange(0, group["duration"].max() + 1)

    kmf.fit(durations, event_observed=events, timeline=timeline)

    survival_df = kmf.survival_function_.reset_index()
    survival_df.columns = ["month_index", "retention_curve_rate"]

    # Extend retention curve
    extended_survival_df = extend_retention_curve(
        survival_df,
        trial_months=trial_months,
        term_cadence=term_cadence,
        months_to_extend=24,
    )

    # Calculating Piecewise retention Curve
    extended_survival_df["piecewise_retention_rate"] = extended_survival_df[
        "retention_curve_rate"
    ] / extended_survival_df["retention_curve_rate"].shift(1)

    for split in splits:
        extended_survival_df[split] = group.iloc[0][split]

    return extended_survival_df


# retention_curves = retention_curves_df.groupby(splits, group_keys=False).apply(apply_km)

# checking_map = {
#     "package_type": "DIGITAL Subscriber",
#     "trial_duration_months": 6,
#     "term_cadence": "year",
# }
# retention_curves[
#     (retention_curves["package_type"] == checking_map["package_type"])
#     & (
#         retention_curves["trial_duration_months"]
#         == checking_map["trial_duration_months"]
#     )
#     & (retention_curves["term_cadence"] == checking_map["term_cadence"])
# ]
