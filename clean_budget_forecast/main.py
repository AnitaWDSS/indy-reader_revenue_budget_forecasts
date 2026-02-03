"""
Parameters:
1. start_month: The starting month for the acquisition forecast
Future Improvements:
1. Integrate validation checks to ensure data integrity during merges.
"""
# Import necessary libraries

import pandas as pd
import numpy as np
from clean_budget_forecast.src.models.retention_curves import apply_km
from clean_budget_forecast.src.project_data.subscriptions_data import (
    retention_curves_df,
    splits,
)
from clean_budget_forecast.src.project_data.user_base_data import cohort_df, base_splits
from datetime import date
from clean_budget_forecast.src.models.new_cohorts_generator import generate_new_cohorts
from clean_budget_forecast.src.project_data.acquisition_data import combined
from clean_budget_forecast.src.models.fill_in_acq_terms import extend_aquisition_data
from clean_budget_forecast.src.models.apply_retention_curves import recursive_forecast
from clean_budget_forecast.src.models.x_month_average import last_x_average
from clean_budget_forecast.src.project_data.offer_data import offers_df
from clean_budget_forecast.src.models.offers_user_base import calculate_user_base
from clean_budget_forecast.src.project_data.currency_conv_data import (
    currency_conversion_extended_df,
)
from clean_budget_forecast.src.project_data.refunds_data import refunds_df
from clean_budget_forecast.src.models.refund_forecast import generate_refund_forecast

# Produce Kaplan-Meier retention curves
retention_curves = retention_curves_df.groupby(splits, group_keys=False).apply(apply_km)

""" Calculating acquisition forecast for special terms (donations, students, COP) """

start_month = date(2025, 10, 1)

acq_forecast_cohort_df = cohort_df[cohort_df["calendar_month"] < start_month].copy()


#  Adding unique keys for retention curve splits and cohort splits
retention_curve_splits = [split for split in splits if split != "trial_duration_months"]
added_splits = ["geo", "term_price", "trial_price", "trial_duration"]


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
        "geo",
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
        "geo",
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
    "geo",
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

"""# Merging User Base & Aquisition"""

for col in ["active_users", "total_cohort_users"]:
    cohort_df[col] = pd.to_numeric(cohort_df[col], errors="coerce").astype("Int64")
    extended_aquisition_data_extended[col] = (
        pd.to_numeric(extended_aquisition_data_extended[col], errors="coerce")
        .round()
        .astype("Int64")
    )

for col in ["calendar_month", "signup_cohort"]:
    cohort_df[col] = pd.to_datetime(cohort_df[col], errors="coerce").dt.date
    extended_aquisition_data_extended[col] = pd.to_datetime(
        extended_aquisition_data_extended[col], errors="coerce"
    ).dt.date

# This map is needed to translate trial_duration into trial_duration in months
cadence_map = {
    "day": 1,
    "Staging test": 0,
    "week": 1,
    "2 week": 1,
    "month": 1,
    "quarter": 3,
    "6 month": 6,
    "year": 12,
    "3 year": 36,
    "No trial": 0,
}

extended_aquisition_data_extended["trial_duration_months"] = (
    extended_aquisition_data_extended["trial_duration"].map(cadence_map)
)

base_acq_df = pd.concat(
    [cohort_df, extended_aquisition_data_extended], ignore_index=True
)

base_acq_df = base_acq_df.drop("level_8", axis="columns")

currency_code_map = {"UK": "GBP", "EUR": "EUR", "US": "USD"}

base_acq_df["payment_currency"] = base_acq_df["geo"].map(currency_code_map)

"""# Applying Retention Curves"""

base_acq_retcurves_df = base_acq_df.merge(
    retention_curves, how="left", on=splits + ["month_index"]
)

base_acq_retcurves_df.piecewise_retention_rate = (
    base_acq_retcurves_df.piecewise_retention_rate.fillna(1)
)

base_acq_retcurves_df = base_acq_retcurves_df.sort_values(base_splits + ["month_index"])

base_acq_retcurves_df["previous_active_users"] = base_acq_retcurves_df.groupby(
    base_splits, dropna=False
)["active_users"].shift(1)


base_acq_retcurves_forecast_df = base_acq_retcurves_df.groupby(
    base_splits, group_keys=False, dropna=False
).apply(recursive_forecast)

base_acq_retcurves_forecast_df["is_trialist"] = np.where(
    (
        (base_acq_retcurves_forecast_df["trial_duration_months"] == 0)
        | (
            base_acq_retcurves_forecast_df["trial_duration_months"]
            <= (base_acq_retcurves_forecast_df["month_index"])
        )
    ),
    False,
    True,
)

offer_splits = [
    "geo",
    "term_cadence",
    "package_type",
    "customer_type",
    "monthly_amount_paid",
]

offers_forecast_df = offers_df.groupby(offer_splits, group_keys=True).apply(
    last_x_average, include_groups=False
)

offers_forecast_df.groupby("transaction_month")["number_of_offers"].sum()

"""## Ammortising Offers"""

offers_forecast_df = offers_forecast_df.reset_index()
offers_forecast_df.term_cadence.unique()

# Note: Offers can only be yearly so I'm removing any payments that do not have that cadence

offers_forecast_df = offers_forecast_df[offers_forecast_df["term_cadence"] == "year"]

offers_forecast_df.head(1)

offers_forecast_df["relevant_cadence_months"] = 12

# Ensure transactions are only amortised throughout the subscription cadence if they're payments - refunds do not get amortised
offers_forecast_df["amortised_summed_local_price"] = (
    offers_forecast_df["monthly_amount_paid"]
    / offers_forecast_df["relevant_cadence_months"]
)


offers_forecast_df["transaction_month"] = pd.to_datetime(
    offers_forecast_df["transaction_month"]
)

grouping_columns = [
    "term_cadence",
    "geo",
    "package_type",
    "customer_type",
    "monthly_amount_paid",
    "relevant_cadence_months",
]

amortised_offers_df = (
    offers_forecast_df.groupby(grouping_columns, group_keys=False)
    .apply(calculate_user_base)
    .reset_index(drop=True)
)

amortised_offers_df["amortised_revenue"] = (
    amortised_offers_df["user_base"]
    * amortised_offers_df["amortised_summed_local_price"]
)

"""##Converting Offers Currencies"""

currency_code_map = {"UK": "GBP", "EUR": "EUR", "US": "USD"}
amortised_offers_df["Currency_Code"] = amortised_offers_df["geo"].map(currency_code_map)

# Convert 'transaction_month' and 'Date' columns to datetime objects before merging
amortised_offers_df["transaction_month"] = pd.to_datetime(
    amortised_offers_df["transaction_month"]
)
currency_conversion_extended_df["Date"] = pd.to_datetime(
    currency_conversion_extended_df["Date"]
)


GBP_amortised_offers_df = amortised_offers_df.merge(
    currency_conversion_extended_df,
    how="left",
    left_on=["transaction_month", "Currency_Code"],
    right_on=["Date", "Currency_Code"],
).drop(columns=["Date", "Currency_Code"])
GBP_amortised_offers_df["GBP_amortised_revenue"] = (
    GBP_amortised_offers_df["amortised_revenue"]
    * GBP_amortised_offers_df["GBP_Conversion"]
)

GBP_amortised_offers_df.head(1)

"""## Final Dataset"""

GBP_amortised_offers_df["transaction_month"] = pd.to_datetime(
    GBP_amortised_offers_df["transaction_month"]
)
GBP_amortised_offers_df = GBP_amortised_offers_df[
    GBP_amortised_offers_df.transaction_month.dt.date >= date(2025, 1, 1)
]

"""
Refunds
--------------------------------

"""

forecasted_refunds = generate_refund_forecast(refunds_df)

forecasted_refunds["yearmonth"] = forecasted_refunds["yearmonth"].astype(str) + "-01"
forecasted_refunds["yearmonth"] = pd.to_datetime(
    forecasted_refunds["yearmonth"]
).dt.date

forecasted_refunds_full = pd.concat([refunds_df, forecasted_refunds], ignore_index=True)

forecasted_refunds_full.head(1)

"""## Converting Refunds Currencies"""

forecasted_refunds_full.head(1)

forecasted_refunds_full["Currency_Code"] = forecasted_refunds_full["geo"].map(
    currency_code_map
)

# Convert 'transaction_month' and 'Date' columns to datetime objects before merging
forecasted_refunds_full["yearmonth"] = pd.to_datetime(
    forecasted_refunds_full["yearmonth"]
)
currency_conversion_extended_df["Date"] = pd.to_datetime(
    currency_conversion_extended_df["Date"]
)


GBP_forecasted_refunds_full = forecasted_refunds_full.merge(
    currency_conversion_extended_df,
    how="left",
    left_on=["yearmonth", "Currency_Code"],
    right_on=["Date", "Currency_Code"],
).drop(columns=["Date", "Currency_Code"])
GBP_forecasted_refunds_full["GBP_amortised_revenue"] = (
    GBP_forecasted_refunds_full["refund_amount"]
    * GBP_forecasted_refunds_full["GBP_Conversion"]
)
"""# Revenue

## Cohorts Merge
"""

currency_conversion_extended_df["Date"] = pd.to_datetime(
    currency_conversion_extended_df["Date"]
).dt.date
base_acq_retcurves_forecast_df["calendar_month"] = pd.to_datetime(
    base_acq_retcurves_forecast_df["calendar_month"]
).dt.date

cohort_joined_converted_df = base_acq_retcurves_forecast_df.merge(
    currency_conversion_extended_df,
    how="left",
    left_on=["calendar_month", "payment_currency"],
    right_on=["Date", "Currency_Code"],
).drop(columns=["Date", "Currency_Code"])
cohort_joined_converted_df["GBP_Conversion"] = cohort_joined_converted_df[
    "GBP_Conversion"
].fillna(1)

"""## Revenue Calculation"""

cohort_joined_converted_df["paying_amount"] = np.where(
    cohort_joined_converted_df["is_trialist"],
    cohort_joined_converted_df["trial_price_value"],
    cohort_joined_converted_df["term_price_value"],
)

cohort_joined_converted_df["term_cadence_months"] = cohort_joined_converted_df[
    "term_cadence"
].map(cadence_map)

cohort_joined_converted_df["current_tenure_cadence"] = np.where(
    cohort_joined_converted_df["is_trialist"],
    cohort_joined_converted_df["trial_duration_months"],
    cohort_joined_converted_df["term_cadence_months"],
)

cohort_joined_converted_df["amortised_paying_amount"] = cohort_joined_converted_df[
    "paying_amount"
].astype(float) / cohort_joined_converted_df["current_tenure_cadence"].astype(float)
cohort_joined_converted_df["predicted_amortised_revenue"] = (
    cohort_joined_converted_df["amortised_paying_amount"]
    * cohort_joined_converted_df["predicted_active_users"]
)

cohort_joined_converted_df["converted_predicted_amortised_revenue"] = (
    cohort_joined_converted_df["predicted_amortised_revenue"]
    * cohort_joined_converted_df["GBP_Conversion"]
)

final_revenue_df = cohort_joined_converted_df

final_cohort_splits = [
    "geo",
    "package_type",
    "term_cadence_months",
    "term_price_value",
    "trial_duration_months",
    "trial_price_value",
    "calendar_month",
]
"""
# Additional KPIs
________________________________
This includes: 
1. Acquisition numbers
2. Churn numbers
3. Trialist Conversion
4. Churn rate
"""

# Creating final_df to hold only necessary KPIs
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

final_df = (
    cohort_joined_converted_df.groupby(dimensions, dropna=False)[metrics]
    .sum()
    .reset_index()
)

kpi_dimensions = [
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
    # 'calendar_month',
    # 'month_index',
    # 'total_cohort_users',
    # 'retention_curve_rate',
    # 'piecewise_retention_rate',
    # 'is_trialist',
    # 'GBP_Conversion',
    # 'paying_amount',
    "term_cadence_months",
    # 'current_tenure_cadence',
    # 'amortised_paying_amount',
]

final_cohort_splits = [
    "geo",
    "package_type",
    "term_cadence_months",
    "term_price_value",
    "trial_duration_months",
    "trial_price_value",
    "calendar_month",
]

### Acquisition Numbers
final_df["acquisition"] = 0

final_df.loc[
    (final_df["month_index"] == 0)
    & (final_df["calendar_month"] >= date(2025, 10, 1))
    & (final_df["predicted_active_users"] > 0),
    "acquisition",
] = final_df["predicted_active_users"]

### Churn Numbers

final_df["previous_predicted_active_users"] = (
    final_df.groupby(kpi_dimensions, dropna=False)["predicted_active_users"]
    .shift(1)
    .fillna(0)
)
final_df["churn"] = (
    final_df["previous_predicted_active_users"] - final_df["predicted_active_users"]
)

final_df.loc[final_df["month_index"] == 0, "churn"] = (
    0  # Set churn to 0 for month_index = 0 (since there is no previous month)
)

final_df["churn_is_trialist"] = np.where(
    (
        (final_df["trial_duration_months"] == 0)
        | (final_df["trial_duration_months"] < (final_df["month_index"]))
    ),
    False,
    True,
)

### Trialist Conversion
final_df["trialist_conversions"] = np.where(
    final_df["month_index"] == final_df["trial_duration_months"],
    final_df["predicted_active_users"],
    0,
)
### Churn Rate
churn_rate_df = (
    final_df.groupby(final_cohort_splits)[
        ["churn", "predicted_active_users", "previous_predicted_active_users"]
    ]
    .sum()
    .reset_index()
)
churn_rate_df["churn_rate"] = (
    churn_rate_df["churn"] / churn_rate_df["previous_predicted_active_users"]
).fillna(0)
