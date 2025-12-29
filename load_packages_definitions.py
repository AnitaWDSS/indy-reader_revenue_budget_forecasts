## NEED TO INSTALL THE FOLLOWING PACKAGES IF NOT ALREADY INSTALLED
# lifelines
# gspread
# google-cloud-bigquery
# Loading necessary packages

# NEED TO FIX UPLOAD ERRORs
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta, date
import pandas as pd
from google.cloud import bigquery
import random
import matplotlib.pyplot as plt
from lifelines import KaplanMeierFitter
import numpy as np
from scipy.optimize import curve_fit
from google.colab import auth

# Authenticate to access Google Cloud services
auth.authenticate_user()
client = bigquery.Client(project="indy-eng")

subs_details_rundate = "2025-10-17"
