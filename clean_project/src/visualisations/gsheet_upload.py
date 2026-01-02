from datetime import date
from main import GBP_amortised_offers_df, cohort_joined_converted_df
import numpy as np
import pandas as pd

"""# Ghsheet Upload"""

from google.auth import default
import gspread

creds, _ = default()
gc = gspread.authorize(creds)

forecast_worksheet = gc.open_by_url(
    "https://docs.google.com/spreadsheets/d/1kSjsdMVuyk7weo_ZcaLYn1DbLtrSSOELBlABF24WyWQ/edit?usp=sharing"
)

"""## Offers Upload"""

GBP_amortised_offers_df.isnull().sum()

# # Write offers forecasts onto Gsheet
# GBP_amortised_offers_df['transaction_month'] = GBP_amortised_offers_df['transaction_month'].astype(str)

# # Replace NaN values with None to make the DataFrame JSON compliant
# GBP_amortised_offers_df = GBP_amortised_offers_df.replace({np.nan: None})

# forecast_worksheet.worksheet('offers_forecast_raw').clear()
# forecast_worksheet.worksheet('offers_forecast_raw').update([GBP_amortised_offers_df.columns.values.tolist()] + GBP_amortised_offers_df.values.tolist())

"""## Refunds Upload"""

# # Write refunds forecasts onto Gsheet
# GBP_forecasted_refunds_full['yearmonth'] = GBP_forecasted_refunds_full['yearmonth'].astype(str)

# forecast_worksheet.worksheet('refunds_forecast_raw').clear()
# forecast_worksheet.worksheet('refunds_forecast_raw').update([GBP_forecasted_refunds_full.columns.values.tolist()] + GBP_forecasted_refunds_full.values.tolist())

"""## Standard Upload"""

import_df = cohort_joined_converted_df[
    [
        "package_type",
        "term_cadence",
        "trial_price",
        "term_price",
        "region",
        "signup_cohort",
        "trial_duration",
        "calendar_month",
        "month_index",
        "predicted_active_users",
        "predicted_amortised_revenue",
        "converted_predicted_amortised_revenue",
    ]
]

import_df = import_df[import_df.calendar_month >= date(2025, 10, 1)]

cols_to_convert = import_df.select_dtypes(
    include=["object", "category", "datetime64[ns]", "timedelta64[ns]", "bool"]
).columns

# Convert only those to str
import_df[cols_to_convert] = import_df[cols_to_convert].astype(str)

import_df = import_df.replace({pd.NA: None, np.nan: None})

forecast_worksheet.worksheet("standard_forecast_fixed").clear()
forecast_worksheet.worksheet("standard_forecast_fixed").update(
    [import_df.columns.values.tolist()] + import_df.values.tolist()
)

"""# Base Upload

"""

base_df = cohort_joined_converted_df[
    [
        "package_type",
        "term_cadence",
        "trial_price",
        "term_price",
        "region",
        "signup_cohort",
        "trial_duration",
        "calendar_month",
        "month_index",
        "predicted_active_users",
        "predicted_amortised_revenue",
        "converted_predicted_amortised_revenue",
        "is_trialist",
    ]
]
base_df = base_df[base_df["calendar_month"] >= date(2025, 10, 1)]
base_df["acquisition"] = base_df.loc[
    base_df["month_index"] == 0, "predicted_active_users"
]

base_df["calendar_month"] = base_df["calendar_month"].astype(str)

cols_to_convert = base_df.select_dtypes(
    include=["object", "category", "datetime64[ns]", "timedelta64[ns]", "bool"]
).columns

# Convert only those to str
base_df[cols_to_convert] = base_df[cols_to_convert].astype(str)

# Replace NaN, inf, and -inf values with None to make the DataFrame JSON compliant
base_df = base_df.replace({pd.NA: None, np.nan: None, np.inf: None, -np.inf: None})

forecast_worksheet.worksheet("base_fixed").clear()
forecast_worksheet.worksheet("base_fixed").update(
    [base_df.columns.values.tolist()] + base_df.values.tolist()
)

base_df.groupby(["calendar_month"])[
    ["predicted_active_users", "acquisition", "converted_predicted_amortised_revenue"]
].sum()
