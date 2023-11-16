import pandas as pd
import redis
from datetime import datetime, timedelta

# Connect to redis
r = redis.Redis(host='redis', port=6379, db=0)

# Read tables from redis
supplier = pd.json_normalize(r.get('supplier'))
lineitem = pd.json_normalize(r.get('lineitem'))

# Date filter for lineitem
start_date = datetime.strptime('1996-01-01', '%Y-%m-%d')
end_date = start_date + timedelta(months=3)
lineitem_filtered = lineitem[(lineitem['L_SHIPDATE']>= start_date) & (lineitem['L_SHIPDATE']< end_date)]

# Create revenue0 dataframe
revenue0 = lineitem_filtered.groupby('L_SUPPKEY').agg({'TOTAL_REVENUE': lambda x: sum(x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']))}).reset_index()

# Merge revenue0 and supplier dataframes
results = pd.merge(supplier, revenue0, how='inner', left_on='S_SUPPKEY', right_on='SUPPLIER_NO')

# Filter rows where TOTAL_REVENUE equals to max TOTAL_REVENUE
results = results[results['TOTAL_REVENUE'] == results['TOTAL_REVENUE'].max()]

# Write output to csv
results.to_csv('query_output.csv', index=False)
