"""# Currency Conversion
--------------------------------
Parameters:
- subs_details_rundate: The run date of the subscription details snapshot to be used
- start_date: The starting date for the forecast
- forecast_to_date: The end date for the forecast

"""

from .subscriptions_data import subs_details_rundate
from .user_base_data import start_date, forecast_to_date, client
import pandas as pd

query = f"""
  SELECT
    Date,
    Currency_Code,
    MAX(GBP_Conversion) AS GBP_Conversion
  FROM `indy-eng.partners.currency_matchtable`
  WHERE
    date <= "{subs_details_rundate}"
  GROUP BY ALL
"""

query_job = client.query(query)
currency_conversion_df = query_job.to_dataframe()
currency_conversion_df["Date"] = pd.to_datetime(currency_conversion_df["Date"]).dt.date

# Creating full date range from start_date to forecast_to_date for currency conversion

min_date = start_date
max_date = forecast_to_date

date_range = pd.date_range(min_date, max_date, name="Date")


def ffill_currency_conversion(group):
    group = group.sort_values("Date", ascending=True)
    group = group.set_index("Date")
    group = group.reindex(date_range)
    group = group["GBP_Conversion"].ffill()
    group = group.reset_index()

    return group


currency_conversion_extended_df = (
    currency_conversion_df.groupby(["Currency_Code"], group_keys=True)
    .apply(ffill_currency_conversion, include_groups=True)
    .reset_index()
    .drop(["level_1"], axis="columns")
)
currency_conversion_extended_df["Date"] = pd.to_datetime(
    currency_conversion_extended_df["Date"]
).dt.date
