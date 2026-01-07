"""
FLASH FORECAST (USING RETENTION CURVE LOGIC)
--------------------------------
This script generates a flash forecast for subscription revenue using same retention curves as used in budget forecast.
It is constitued by the following main sections:
1. Calculating paying user base and amortised revenue acquired up to flash date
2. Importing acquisition forecasts and applying retention curves to generate forecasted paying user base and revenue for the month
"""

import pandas as pd
import random
import numpy as np
from datetime import date
from src.transaction_log import transactions_df
from src.calculate_user_base import calculate_user_base
from clean_budget_forecast.src.project_data.currency_conv_data import (
    currency_conversion_extended_df,
)
from clean_budget_forecast.src.models.apply_retention_curves
from clean_budget_forecast.main
"""## Summed Local Price

Creates the summed local prices by:
* Subsetting the initial SQL query
* Calculates the summed local price summing local prices over all splits
* Cohorts divided by the year-month a subscription was taken out
"""

grouping_columns = [
    "piano_uid",
    "term_name",
    "term_id",
    "currency",
    "term_cadence",
    "region",
    "customer_type",
    "package_type",
    "is_trialist",
    "flash_tenure",
    "trial_price",
    "term_price",
    "tax_type",
    "month_index",
    "trial_cadence",
    "year_month",
]

subset_columns = grouping_columns + ["local_price"]

grouped_transactions_df_subset = transactions_df[subset_columns]
grouped_transactions_df = (
    grouped_transactions_df_subset.groupby(grouping_columns)["local_price"]
    .sum()
    .reset_index(name="summed_local_price")
)
grouped_transactions_df

# Identify refunds (summed_local_price <= 0)
refund_amount = (
    pd.DataFrame(
        grouped_transactions_df[grouped_transactions_df.summed_local_price <= 0][
            "summed_local_price"
        ].unique(),
        columns=["summed_local_price"],
    )
    .sort_values("summed_local_price", ascending=False)
    .head()
)

"""## User count

Calculates the number of users billed within each split
"""

# removes piano_uid from summed_local_price grouping
users_grouping_columns = [
    "term_name",
    "term_id",
    "currency",
    "term_cadence",
    "region",
    "customer_type",
    "package_type",
    "is_trialist",
    "flash_tenure",
    "trial_price",
    "term_price",
    "tax_type",
    "month_index",
    "trial_cadence",
    "year_month",
    "summed_local_price",
]
users_count_df = (
    grouped_transactions_df.groupby(users_grouping_columns)["piano_uid"]
    .count()
    .reset_index(name="user_count")
)

"""## User Base & Amortises Revenue

Once we have both the number users and the amounts billed per split, we calculate:
1. The user base still active per split. Unless on a monthly term, this includes all the people that have had a billing in a period equal to their term_cadence. E.g.: For annual subs, whether they have had a billing in the last year.
2. The amortised revenue in a month: finance splits a user's billed amount by the period of time they are active for
"""

# Define relevant cadence needed to amortise transaction
users_count_df["relevant_cadence"] = np.where(
    users_count_df["flash_tenure"] == "trialist",
    users_count_df["trial_cadence"],
    users_count_df["term_cadence"],
)

# Turn their cadence into number of months they are active for (for anything smaller than a month, we limit it to the moth in which a payment was made)
cadence_map = {
    "day": 1,
    "week": 1,
    "2 week": 1,
    "month": 1,
    "quarter": 3,
    "6 month": 6,
    "7 month": 7,
    "year": 12,
    "3 year": 36,
}

users_count_df["relevant_cadence_months"] = users_count_df["relevant_cadence"].map(
    cadence_map
)

# Renaming for clarity
clean_transactions_df = users_count_df

# Ensure transactions are only amortised throughout the subscription cadence if they're payments - refunds do not get amortised
clean_transactions_df["amortised_summed_local_price"] = np.where(
    clean_transactions_df["summed_local_price"] >= 0,
    clean_transactions_df["summed_local_price"]
    / clean_transactions_df["relevant_cadence_months"],
    clean_transactions_df["summed_local_price"],
)
# Ensure year_month is of type date_time for calculations
clean_transactions_df["year_month"] = pd.to_datetime(
    clean_transactions_df["year_month"]
)

grouping_columns = [
    "term_name",
    "term_id",
    "term_cadence",
    "region",
    "customer_type",
    "package_type",
    "is_trialist",
    "flash_tenure",
    "term_price",
    "currency",
    "tax_type",
    "month_index",
    "trial_cadence",
    "trial_price",
    "summed_local_price",
    "relevant_cadence",
    "amortised_summed_local_price",
    "relevant_cadence_months",  # Needs to be last column
]

# Executes calculation of base function
amortised_transactions_df = (
    clean_transactions_df.groupby(grouping_columns).apply(
        calculate_user_base, include_groups=False
    )
).reset_index()

# Calculated amortised revenue
amortised_transactions_df["amortised_revenue"] = np.where(
    amortised_transactions_df["amortised_summed_local_price"] >= 0,
    amortised_transactions_df["user_base"]
    * amortised_transactions_df["amortised_summed_local_price"],
    amortised_transactions_df["user_count"]
    * amortised_transactions_df["amortised_summed_local_price"],
)

"""## Currency Conversion"""
GBP_amortised_transactions_df = amortised_transactions_df.merge(
    currency_conversion_extended_df,
    how="left",
    left_on=["year_month", "currency"],
    right_on=["Date", "Currency_Code"],
).drop(columns=["Date", "Currency_Code"])
GBP_amortised_transactions_df["gbp_amortised_revenue"] = (
    GBP_amortised_transactions_df["amortised_revenue"]
    * GBP_amortised_transactions_df["GBP_Conversion"]
)

"""# Forecasting EOM Performance (With Retention Curves)

1. Identify number of users due to renew in the current month
2. Upload rentention curves
3. Calculate MoM retention likelihood difference for all month_indexes found among users due to renew
4. Use said difference to calculate expected EOM number of billed users
5. Calculate expected amount of amortised revenue
"""
