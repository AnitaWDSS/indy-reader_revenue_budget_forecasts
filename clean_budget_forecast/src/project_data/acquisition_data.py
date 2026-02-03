"""Acquisition
--------------------------------
NEED:
Access to GSheets API (more widely need to figure out google auth)
Sections:
    1. Calculating Subscription Acquisitions based on PAVs
    2. Calculating Subscription Acquisitions based on 6-month average (Naive)
    3. Introducing InDigital Uplift
    4. Joining Acquisition Sources Data

Parameters:

Future Improvements:
- Automate the calculation of CVR based on recent data (Using GA4 data directly)

"""

from google.cloud import bigquery
import pandas as pd
from datetime import date
from .user_base_data import cohort_df


client = bigquery.Client(
    project="indy-eng",
    client_options={
        "scopes": [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform",
        ]
    },
)

# Introduce % of conversions and CVR per subscription source
conversion_query = """
SELECT
    *
FROM `indy-eng.reader_revenue_external.conversions_per_subscription_experience`
"""

query_job = client.query(conversion_query)
conv_by_source_df = query_job.to_dataframe()

cvr_query = """
SELECT
    * 
    FROM `indy-eng.reader_revenue_external.cvr_per_subscription_experience`

"""

query_job = client.query(cvr_query)
cvr_by_source_df = query_job.to_dataframe()

# Introducing FY26.V2 traffic assumptions
traffic_query = """
SELECT
    *
FROM `indy-eng.reader_revenue_external.fy26-v2_traffic_predictions` 
"""
query_job = client.query(traffic_query)
traffic_forecast_df = query_job.to_dataframe()

# Note: Only UK+ROW subscriptions will be tied to traffic, US traffic has been removed from the below
PAV_forecast_df = traffic_forecast_df[["date", "PAVs"]]
PAV_forecast_df = PAV_forecast_df[
    pd.to_datetime(PAV_forecast_df["date"]).dt.date > date(2025, 12, 1)
]

HPPU_forecast_df = traffic_forecast_df[["date", "HPPU_PVs"]]
HPPU_forecast_df = HPPU_forecast_df[
    pd.to_datetime(HPPU_forecast_df["date"]).dt.date > date(2025, 12, 1)
]

# Defining subscription source
PAV_forecast_df["Subscription_Experience"] = "Premium Article Gate"
HPPU_forecast_df["Subscription_Experience"] = "HPPU / Section PU"

# NOTE: This adds all traffic UK+ROW into UK geo - change if possible
PAV_forecast_df["geo"] = "UK"
HPPU_forecast_df["geo"] = "UK"

# Merging CVR into traffic Forecasts
PAV_forecast_df = pd.merge(
    PAV_forecast_df,
    cvr_by_source_df[["Subscription_Experience", "CVR"]],
    on="Subscription_Experience",
    how="left",
)
HPPU_forecast_df = pd.merge(
    HPPU_forecast_df,
    cvr_by_source_df[["Subscription_Experience", "CVR"]],
    on="Subscription_Experience",
    how="left",
)

# Calculate new subscribers from PAV forecasts
PAV_forecast_df["New Subscribers"] = (
    PAV_forecast_df["PAVs"] * PAV_forecast_df["CVR"] / 100.0
)

# Calculate new subscribers from HPPU forecasts
HPPU_forecast_df["New Subscribers"] = (
    HPPU_forecast_df["HPPU_PVs"] * HPPU_forecast_df["CVR"] / 100.0
)

# Grouping HPPU and PAG acquisitions together
forecasted_cohorts = pd.concat(
    [
        PAV_forecast_df[["date", "New Subscribers", "Subscription_Experience"]],
        HPPU_forecast_df[["date", "New Subscribers", "Subscription_Experience"]],
    ],
    ignore_index=True,
)

# Introducing remaining subscription cohorts
sources = ["Navigation", "Direct"]
months_2026 = pd.date_range(start="2026-01-01", end="2026-12-01", freq="MS")

new_data = []
for source in sources:
    for month in months_2026:
        new_data.append(
            {"date": month, "New Subscribers": 0, "Subscription_Experience": source}
        )

new_sources_df = pd.DataFrame(new_data)

forecasted_cohorts = pd.concat([forecasted_cohorts, new_sources_df], ignore_index=True)
forecasted_cohorts.date = pd.to_datetime(forecasted_cohorts.date).dt.date

#  Adding % of conversions from each sub journey

forecasted_cohorts = forecasted_cohorts.merge(
    conv_by_source_df[["Subscription_Experience", "Percentage"]],
    on="Subscription_Experience",
    how="left",
)

# Calculate number of acquisitions for Navigation and Direct based on remaining acquisition %
forecasted_navigation = forecasted_cohorts.loc[
    forecasted_cohorts["Subscription_Experience"] == "Navigation", "New Subscribers"
] = (
    forecasted_cohorts[
        forecasted_cohorts["Subscription_Experience"] == "Premium Article Gate"
    ]["New Subscribers"].values[0]
    * forecasted_cohorts[forecasted_cohorts["Subscription_Experience"] == "Navigation"][
        "Percentage"
    ]
    / 100.0
    / forecasted_cohorts[
        forecasted_cohorts["Subscription_Experience"] == "Premium Article Gate"
    ]["Percentage"]
    * 100.0
)

PAG_metrics = (
    forecasted_cohorts[
        forecasted_cohorts["Subscription_Experience"] == "Premium Article Gate"
    ][["date", "New Subscribers", "Percentage"]]
    .copy()
    .rename(columns={"Percentage": "PAG_%", "New Subscribers": "PAG_New_Subscribers"})
)
forecasted_cohorts = forecasted_cohorts.merge(PAG_metrics, on="date", how="left")

#  Calculating new acquisitions from Navigation
forecasted_cohorts.loc[
    forecasted_cohorts["Subscription_Experience"] == "Navigation", "New Subscribers"
] = (
    forecasted_cohorts["PAG_New_Subscribers"]
    * (forecasted_cohorts["Percentage"] / 100.0)
    / (forecasted_cohorts["PAG_%"] / 100.0)
)
#  Calculating new acquisitions from Direct
forecasted_cohorts.loc[
    forecasted_cohorts["Subscription_Experience"] == "Direct", "New Subscribers"
] = (
    forecasted_cohorts["PAG_New_Subscribers"]
    * (forecasted_cohorts["Percentage"] / 100.0)
    / (forecasted_cohorts["PAG_%"] / 100.0)
)

forecasted_subs = forecasted_cohorts.groupby("date")["New Subscribers"].sum()

forecasted_subs = pd.DataFrame(forecasted_subs).reset_index()


# Add COP and Student Acquisitions based on 6-month average
six_months_ago = (pd.Timestamp.now() - pd.DateOffset(months=6)).replace(day=1).date()


current_users_breakdown = cohort_df[
    (
        (
            (cohort_df["package_type"].isin(["DIGITAL Subscriber"]))
            & (cohort_df["term_cadence"].isin(["month", "year"]))
            & (
                cohort_df["term_price"].isin(
                    [
                        "GBP11.00",
                        "EUR11.00",
                        "GBP99.00",
                        "EUR99.00",
                        "GBP4.00",
                        "EUR4.00",
                    ]
                )
            )
            & (cohort_df["trial_price"].isin(["GBP1.00", "EUR1.00"]))
            & (cohort_df["trial_duration"].isin(["6 month"]))
            | (
                (
                    cohort_df["package_type"].isin(
                        ["Consent Or Pay Only Subscriber", "Student Subscription"]
                    )
                )
                & (cohort_df["term_cadence"].isin(["month"]))
                & (
                    cohort_df["term_price"].isin(
                        ["GBP4.00", "EUR4.00", "GBP1.00", "EUR1.00"]
                    )
                )
                & (cohort_df["trial_price"].isin(["No trial"]))
            )
        )
        & (cohort_df["signup_cohort"] >= six_months_ago)
        & (cohort_df["month_index"] == 0)
    )
]

current_users_breakdown = (
    current_users_breakdown.groupby(
        [
            "geo",
            "package_type",
            "trial_duration",
            "term_cadence",
            "trial_price",
            "term_price",
            "month_index",
        ]
    )["total_cohort_users"]
    .sum()
    .reset_index()
)
current_users_breakdown["all_in_cohort"] = current_users_breakdown[
    "total_cohort_users"
].sum()

current_users_breakdown["perc_subs"] = (
    current_users_breakdown["total_cohort_users"]
    / current_users_breakdown["all_in_cohort"]
)

combined = forecasted_subs.merge(
    current_users_breakdown[
        [
            "geo",
            "package_type",
            "term_cadence",
            "trial_duration",
            "trial_price",
            "term_price",
            "month_index",
            "perc_subs",
        ]
    ],
    how="cross",
)

combined["forecasted_subs"] = combined["New Subscribers"] * combined["perc_subs"]

# Manually fixing erroneous term

broken_term = (
    (combined["geo"] == "EUR")
    & (combined["term_cadence"] == "month")
    & (combined["term_price"] == "GBP99.00")
)
combined.loc[broken_term, "forecasted_subs"] = 0

print(combined)
