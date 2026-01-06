from flash_forecast.main import clean_transactions_df

# Checking that cadence map addresses all terms
clean_transactions_df.relevant_cadence_months.unique()
clean_transactions_df[clean_transactions_df.relevant_cadence_months.isna()]
