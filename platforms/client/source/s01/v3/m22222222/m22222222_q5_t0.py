import redis
import pandas as pd
from datetime import datetime

# Connecting to Redis Database
db = redis.Redis(host='redis', port=6379, db=0)

# Loading data tables from Redis
nation = pd.DataFrame(db.get('nation'))
region = pd.DataFrame(db.get('region'))
supplier = pd.DataFrame(db.get('supplier'))
customer = pd.DataFrame(db.get('customer'))
orders = pd.DataFrame(db.get('orders'))
lineitem = pd.DataFrame(db.get('lineitem'))

# Converting C_NATIONKEY and N_NATIONKEY to compatible datatypes
customer['C_NATIONKEY'] = customer['C_NATIONKEY'].astype(int)
nation['N_NATIONKEY'] = nation['N_NATIONKEY'].astype(int)

# Joining customer and nation dataframes
merged_df = pd.merge(customer, nation, how='inner', left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Further extending joined dataframe with supplier dataframe
merged_df = pd.merge(merged_df, supplier, how='inner', left_on='C_NATIONKEY', right_on='S_NATIONKEY')
merged_df = pd.merge(merged_df, region, how='inner', left_on='O_REGIONKEY', right_on='R_REGIONKEY')
merged_df = pd.merge(merged_df, orders, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, lineitem, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Setting the correct data type for date and filtering the orders dataframe based on that
merged_df['O_ORDERDATE'] = pd.to_datetime(merged_df['O_ORDERDATE'])
mask = (merged_df['O_ORDERDATE'] >= datetime.strptime('1990-01-01', '%Y-%m-%d')) & (merged_df['O_ORDERDATE'] < datetime.strptime('1995-01-01', '%Y-%m-%d'))
filtered_df = merged_df.loc[mask]

# Calculating REVENUE
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Evaluating the query by grouping on N_NAME and calculating sum of REVENUE
output_df = filtered_df.groupby('N_NAME').agg({'REVENUE': 'sum'}).sort_values(by='REVENUE', ascending=False)

# Writing the output to .csv file
output_df.to_csv('query_output.csv')
