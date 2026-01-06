import pandas as pd
from src.project_data.user_base_data import forecast_to_date


def extend_aquisition_data(group):
    if len(group) > 1:
        raise Exception()

    start_month = group["calendar_month"].iloc[0]

    date_range = pd.date_range(start_month, forecast_to_date, freq="MS")

    group = group.set_index("calendar_month")
    group.index = pd.to_datetime(group.index)
    group = group.reindex(date_range)

    group["month_index"] = range(0, len(group))
    group["active_users"] = group["active_users"].fillna(0)

    group = group.reset_index().rename(columns={"index": "calendar_month"})

    return group
