# Defining function to calculate user_base
from dateutil.relativedelta import relativedelta
import pandas as pd


def calculate_user_base(group):
    cadence_month = group.name[-1]

    min_date = group["year_month"].min()
    max_date = group["year_month"].max() + (
        relativedelta(months=max(cadence_month, 1) - 1)
    )

    # Generate complete date range

    all_dates = pd.date_range(min_date, max_date, freq="MS")

    # Reindex to fill missing months
    group = group.set_index("year_month").reindex(all_dates, fill_value=0)

    # Calculates rolling sum of user_count across the cadence_months to obtain the active user base
    group["user_base"] = group["user_count"].rolling(cadence_month, min_periods=1).sum()

    group = group.reset_index(names="year_month")
    return group
