"""
Parameters:
1. min_date: The minimum date for historical data
2. max_date: The maximum date for historical data (before forecasting starts)
3. num_months: Number of past months to average for forecasting
4. forecast_months: Number of months to forecast into the future
"""

import pandas as pd
import numpy as np


# A change was made in April to higher offer prices and this brought number of offer acceptances down, have limited num_months to reflect this
def last_x_average(group, num_months=5, forecast_months=27):
    # Define date range
    min_date = pd.Timestamp("2024-01-01")
    max_date = pd.Timestamp("2025-09-01")

    # Ensure datetime type
    group["transaction_month"] = pd.to_datetime(group["transaction_month"])

    # Reindex to cover full historical range
    date_range = pd.date_range(min_date, max_date, freq="MS")

    group = (
        group.sort_values("transaction_month")
        .set_index("transaction_month")
        .reindex(date_range)
        .reset_index()
        .rename(columns={"index": "transaction_month"})
    )

    group["key"] = "actuals"
    group["number_of_offers"] = group["number_of_offers"].fillna(0)

    # Iterative forecasting
    forecast_values = []
    last_values = list(group["number_of_offers"].tail(num_months))

    for i in range(forecast_months):
        next_month = max_date + pd.DateOffset(months=i + 1)
        avg_value = np.round(np.mean(last_values[-num_months:]))
        forecast_values.append((next_month, avg_value))
        last_values.append(avg_value)

    # Build forecast DataFrame
    forecast_df = pd.DataFrame(
        forecast_values, columns=["transaction_month", "number_of_offers"]
    )
    forecast_df["key"] = "forecast"

    # Combine actuals and forecasts
    combined = pd.concat([group, forecast_df], ignore_index=True)
    combined["transaction_month"] = pd.to_datetime(combined["transaction_month"])

    return combined
