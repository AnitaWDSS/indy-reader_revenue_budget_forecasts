from clean_budget_forecast.main import final_forecasted_subs
from flash_forecast.main import subs_to_renew_df
import pandas as pd

# Get column names as sets
subs_to_renew_cols = set(subs_to_renew_df.columns)
acquisition_cols = set(final_forecasted_subs.columns)

# Columns in df1 but NOT in df2
only_in_renew = subs_to_renew_cols - acquisition_cols

# Columns in df2 but NOT in df1
only_in_acq = acquisition_cols - subs_to_renew_cols

# All columns that are NOT in both (symmetric difference)
not_in_common = subs_to_renew_cols.symmetric_difference(acquisition_cols)
in_common = subs_to_renew_cols.intersection(acquisition_cols)

print("Only in renew:", only_in_renew)
print("Only in acq:", only_in_acq)
print("Not in common:", not_in_common)
print("In common:", in_common)
