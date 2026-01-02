import pandas as pd


def calculate_user_base(group):
    cadence_interval = 12

    # sort by date
    group = group.sort_values("transaction_month").copy()

    min_date = group["transaction_month"].min()
    max_date = group["transaction_month"].max()

    # Ensure full range of months
    all_dates = pd.date_range(min_date, max_date, freq="MS")

    # Reindex by date to ensure all months exist
    group = (
        group.set_index("transaction_month")
        .reindex(all_dates)
        .rename_axis("transaction_month")
    )

    # Fill missing offers with 0
    group["number_of_offers"] = group["number_of_offers"].fillna(0)

    # Make sure the index is sorted and monotonic
    group = group.sort_index()
    group["user_base"] = (
        group["number_of_offers"].rolling(window=cadence_interval, min_periods=1).sum()
    )

    # Reset back to flat structure
    group = group.reset_index()

    return group
