# query.py
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to Redis
redis_host = "redis"
redis_port = 6379
redis_db = 0
dredis = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Load data from Redis into pandas DataFrames
df_nation = pd.read_json(dredis.get('nation'))
df_customer = pd.read_json(dredis.get('customer'))
df_orders = pd.read_json(dredis.get('orders'))
df_lineitem = pd.read_json(dredis.get('lineitem'))

# Filtering the data
start_date = datetime(1993, 10, 1)
end_date = datetime(1994, 1, 1)
df_orders_filtered = df_orders[(df_orders['O_ORDERDATE'] >= start_date) & (df_orders['O_ORDERDATE'] <= end_date)]
df_lineitem_filtered = df_lineitem[df_lineitem['L_RETURNFLAG'] == 'R']
df_lineitem_filtered = df_lineitem_filtered.merge(df_orders_filtered, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Calculating lost revenue
df_lineitem_filtered['LOST_REVENUE'] = df_lineitem_filtered['L_EXTENDEDPRICE'] * (1 - df_lineitem_filtered['L_DISCOUNT'])

# Merging tables
df_result = (df_lineitem_filtered
             .groupby(['L_ORDERKEY', 'O_CUSTKEY'])
             .agg({'LOST_REVENUE': 'sum'})
             .reset_index()
             .merge(df_customer, how='left', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
             .merge(df_nation, how='left', left_on='C_NATIONKEY', right_on='N_NATIONKEY'))

# Selecting columns and sorting
df_result = (df_result[['C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT', 'LOST_REVENUE']]
             .rename(columns={'N_NAME': 'NATION', 'C_NAME': 'CUSTOMER_NAME', 'C_ADDRESS': 'CUSTOMER_ADDRESS',
                              'C_PHONE': 'PHONE_NUMBER', 'C_ACCTBAL': 'ACCOUNT_BALANCE', 'C_COMMENT': 'COMMENT'})
             .sort_values(by=['LOST_REVENUE', 'O_CUSTKEY', 'CUSTOMER_NAME', 'ACCOUNT_BALANCE'], ascending=[False, True, True, True]))

# Write results to CSV
df_result.to_csv('query_output.csv', index=False)
