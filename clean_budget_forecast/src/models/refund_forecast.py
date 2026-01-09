import pandas as pd


def generate_refund_forecast(refunds_df, forecast_months=27):
    refunds_df = refunds_df.copy()
    refunds_df["yearmonth"] = pd.PeriodIndex(refunds_df["yearmonth"], freq="M")

    future_refunds = []

    for month_ahead in range(1, forecast_months + 1):
        last_month = refunds_df["yearmonth"].max()
        new_month = last_month + 1  # forecast next month

        # Forecast separately for each region
        for region, group in refunds_df.groupby("region"):
            group = group.sort_values("yearmonth")

            # Take last 6 actual/forecasted months
            recent = group.tail(6)

            if recent.empty:
                continue

            # Simple mean (no weights)
            avg_refund = recent["refund_amount"].mean()

            # Create new forecast row
            new_row = {
                "region": region,
                "yearmonth": new_month,
                "refund_amount": avg_refund,
            }

            # Store the forecast record
            future_refunds.append(new_row)

            # Append it so that the next forecast includes it
            refunds_df = pd.concat(
                [refunds_df, pd.DataFrame([new_row])], ignore_index=True
            )

    return pd.DataFrame(future_refunds)
