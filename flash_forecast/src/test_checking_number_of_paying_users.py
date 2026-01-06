from flash_forecast.main import users_count_df

checking_subs = users_count_df.loc[
    users_count_df.customer_type == "Subscription",
    [
        "package_type",
        "term_name",
        "is_trialist",
        "flash_tenure",
        "summed_local_price",
        "term_cadence",
        "trial_cadence",
        "term_price",
        "trial_price",
        "month_index",
        "year_month",
    ],
].sort_values(["month_index"])
checking_subs
