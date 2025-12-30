""" Acquisition
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
import gspread
from google.oauth2.service_account import Credentials
client = bigquery.Client(project="indy-eng")

# # Define the scope
# scopes = ['https://www.googleapis.com/auth/spreadsheets']

# # Authenticate using the JSON key file
# creds = Credentials.from_service_account_file('path/to/your/credentials.json', scopes=scopes)
# client = gspread.authorize(creds)

# # Open the spreadsheet (by name or URL)
# sheet = client.open('Your Spreadsheet Name').sheet1
# # Or by URL: sheet = client.open_by_url('your-sheet-url').sheet1

# # Read data
# data = sheet.get_all_records()  # Returns list of dictionaries
# print(data)
# # Authorize gspread
# gc = gspread.authorize(creds)
# #  Introducing CVR of different acquisition sources

# # Uploading GSheet with CVR (Ideally would pull this directly from Adobe/ GA4 but nature of conversion data complicates this)

# # Opening GSheet
# sheet_url = "https://docs.google.com/spreadsheets/d/14bVpG_hj_YrBmHdCTTfckKHAkBOAlVSMeKUgEyzZqSk/edit?usp=sharing"

# acq_spreadsheet = gc.open_by_url(sheet_url)

conversions_by_source = acq_spreadsheet.sheet1


# Use get_worksheet on the spreadsheet object to get the second worksheet (index 1)
cvr_by_source = acq_spreadsheet.get_worksheet(1)

#  Turning into dataframes
conv_by_source_df = pd.DataFrame(conversions_by_source.get_all_records())
cvr_by_source_df = pd.DataFrame(cvr_by_source.get_all_records())

# Introducing Emily's traffic assumptions

sheet_url = "https://docs.google.com/spreadsheets/d/1u1luHB9rRBj9ih0x9e2kBF2CA9Dj8RjRaSkvf1jZc9Q/edit?usp=sharing"

traffic_forecast = gc.open_by_url(sheet_url)

# Note: Only UK+ROW subscriptions will be tied to traffic, US traffic has been removed from the below
PAV_forecast_df = pd.DataFrame(traffic_forecast.get_worksheet(2).get_all_records())
PAV_forecast_df = PAV_forecast_df[
    pd.to_datetime(PAV_forecast_df["date"]).dt.date > date(2025, 12, 1)
]

HPPU_forecast_df = pd.DataFrame(traffic_forecast.get_worksheet(4).get_all_records())
HPPU_forecast_df = HPPU_forecast_df[
    pd.to_datetime(HPPU_forecast_df["date"]).dt.date > date(2025, 12, 1)
]

# Defining subscription source
PAV_forecast_df["Subscription Experience"] = "Premium Article Gate"
HPPU_forecast_df["Subscription Experience"] = "HPPU / Section PU"

# NOTE: This adds all traffic UK+ROW into UK region - change if possible
PAV_forecast_df["region"] = "UK"
HPPU_forecast_df["region"] = "UK"

# Merging CVR into traffic Forecasts
PAV_forecast_df = pd.merge(
    PAV_forecast_df,
    cvr_by_source_df[["Subscription Experience", "CVR"]],
    on="Subscription Experience",
    how="left",
)
HPPU_forecast_df = pd.merge(
    HPPU_forecast_df,
    cvr_by_source_df[["Subscription Experience", "CVR"]],
    on="Subscription Experience",
    how="left",
)

# Calculate new subscribers from PAV forecasts
PAV_forecast_df["New Subscribers"] = (
    PAV_forecast_df["PAVs"]
    * PAV_forecast_df["CVR"].str.rstrip("%").astype("float")
    / 100.0
)

# Calculate new subscribers from HPPU forecasts
HPPU_forecast_df["New Subscribers"] = (
    HPPU_forecast_df["UK_PVs"]
    * HPPU_forecast_df["CVR"].str.rstrip("%").astype("float")
    / 100.0
)

print("PAV Forecast:")
display(PAV_forecast_df.head())


print("\nHPPU Forecast:")
display(HPPU_forecast_df.head())

# Grouping HPPU and PAG acquisitions together
forecasted_cohorts = pd.concat(
    [
        PAV_forecast_df[["date", "New Subscribers", "Subscription Experience"]],
        HPPU_forecast_df[["date", "New Subscribers", "Subscription Experience"]],
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
            {"date": month, "New Subscribers": 0, "Subscription Experience": source}
        )

new_sources_df = pd.DataFrame(new_data)

forecasted_cohorts = pd.concat([forecasted_cohorts, new_sources_df], ignore_index=True)
forecasted_cohorts.date = pd.to_datetime(forecasted_cohorts.date).dt.date

#  Adding % of conversions from each sub journey

forecasted_cohorts = forecasted_cohorts.merge(
    conv_by_source_df[["Subscription Experience", "%"]],
    on="Subscription Experience",
    how="left",
)

# Calculate number of acquisitions for Navigation and Direct based on remaining acquisition %
forecasted_navigation = forecasted_cohorts.loc[
    forecasted_cohorts["Subscription Experience"] == "Navigation", "New Subscribers"
] = (
    forecasted_cohorts[
        forecasted_cohorts["Subscription Experience"] == "Premium Article Gate"
    ]["New Subscribers"].values[0]
    * forecasted_cohorts[forecasted_cohorts["Subscription Experience"] == "Navigation"][
        "%"
    ]
    .str.rstrip("%")
    .astype("float")
    / 100.0
    / forecasted_cohorts[
        forecasted_cohorts["Subscription Experience"] == "Premium Article Gate"
    ]["%"]
    .str.rstrip("%")
    .astype("float")
    * 100.0
)

PAG_metrics = (
    forecasted_cohorts[
        forecasted_cohorts["Subscription Experience"] == "Premium Article Gate"
    ][["date", "New Subscribers", "%"]]
    .copy()
    .rename(columns={"%": "PAG_%", "New Subscribers": "PAG_New_Subscribers"})
)
forecasted_cohorts = forecasted_cohorts.merge(PAG_metrics, on="date", how="left")

#  Calculating new acquisitions from Navigation
forecasted_cohorts.loc[
    forecasted_cohorts["Subscription Experience"] == "Navigation", "New Subscribers"
] = (
    forecasted_cohorts["PAG_New_Subscribers"]
    * (forecasted_cohorts["%"].str.rstrip("%").astype("float") / 100.0)
    / (forecasted_cohorts["PAG_%"].str.rstrip("%").astype("float") / 100.0)
)
#  Calculating new acquisitions from Direct
forecasted_cohorts.loc[
    forecasted_cohorts["Subscription Experience"] == "Direct", "New Subscribers"
] = (
    forecasted_cohorts["PAG_New_Subscribers"]
    * (forecasted_cohorts["%"].str.rstrip("%").astype("float") / 100.0)
    / (forecasted_cohorts["PAG_%"].str.rstrip("%").astype("float") / 100.0)
)

forecasted_subs = forecasted_cohorts.groupby("date")["New Subscribers"].sum()

forecasted_subs = pd.DataFrame(forecasted_subs).reset_index()

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
            "region",
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
            "region",
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
    (combined["region"] == "EUR")
    & (combined["term_cadence"] == "month")
    & (combined["term_price"] == "GBP99.00")
)
combined.loc[broken_term, "forecasted_subs"] = 0
