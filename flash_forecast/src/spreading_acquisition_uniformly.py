import pandas as pd


def spread_acquisition_daily(df):
    """
    Spreads the acquisition of users uniformly across the month. This is done by:
    * Calculating the number of days in the month for each row
    * Dividing the user count by the number of days in the month to get a daily acquisition count
    * Expanding the dataframe to have one row per day of acquisition, with the daily acquisition count
    """
    # Calculate the number of days in the month for each row
    df["days_in_month"] = df["calendar_month"].dt.days_in_month

    # Calculate daily acquisition count
    df["daily_acquisition"] = df["active_users"] / df["days_in_month"]

    # Create a new dataframe to hold the expanded rows
    expanded_rows = []

    for _, row in df.iterrows():
        date_range = pd.date_range(
            start=row["calendar_month"],
            periods=row["days_in_month"],
            freq="D",
        )
        # Create df with one row per day
        daily_df = pd.DataFrame(
            {
                "acquisition_date": date_range,
                "daily_acquisition": row["daily_acquisition"],
            }
        )
        # Add al other columns from the original row
        for col in df.columns:
            if col not in ["calendar_month", "days_in_month", "active_users"]:
                daily_df[col] = row[col]
        expanded_rows.append(daily_df)
    # Concatenate all daily dfs into a single df
    expanded_df = pd.concat(expanded_rows, ignore_index=True)
    return expanded_df
