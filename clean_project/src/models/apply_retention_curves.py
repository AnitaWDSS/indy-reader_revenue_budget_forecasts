import pandas as pd
import numpy as np
from datetime import date


def recursive_forecast(group):
    preds = []
    for i, row in group.iterrows():
        # If row is an actual value, keep prediction to be active users
        if row["calendar_month"] <= date(2025, 9, 1):
            preds.append(row["active_users"])

        elif not preds and row["active_users"] != 0:
            preds.append(row["active_users"])

        elif not preds and row["active_users"] == 0:
            preds.append(0)

        # If a forecast value, multiply the previous predicted value by the piecewise retention rate
        else:
            prev_value = preds[-1] if preds else np.nan
            preds.append(
                prev_value * row["piecewise_retention_rate"]
                if pd.notna(prev_value)
                else np.nan
            )

    group["predicted_active_users"] = preds

    return group
