import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve DataFrame from Redis
def get_df_from_redis(table_name):
    return pd.read_json(redis_client.get(table_name))

# Load tables
nation_df = get_df_from_redis('nation')
region_df = get_df_from_redis('region')
supplier_df = get_df_from_redis('supplier')
customer_df = get_df_from_redis('customer')
orders_df = get_df_from_redis('orders')
lineitem_df = get_df_from_redis('lineitem')

# Filter for dates and region 'ASIA'
filtered_orders_df = orders_df[orders_df['O_ORDERDATE'].between('1990-01-01', '1994-12-31')]
asia_region = region_df[region_df['R_NAME'] == 'ASIA']
asia_nations = nation_df[nation_df['N_REGIONKEY'].isin(asia_region['R_REGIONKEY'])]
asia_customers = customer_df[customer_df['C_NATIONKEY'].isin(asia_nations['N_NATIONKEY'])]
asia_orders = filtered_orders_df[filtered_orders_df['O_CUSTKEY'].isin(asia_customers['C_CUSTKEY'])]

# Join tables for revenue computation
joined_df = pd.merge(asia_orders, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
joined_df['REVENUE'] = joined_df['L_EXTENDEDPRICE'] * (1 - joined_df['L_DISCOUNT'])

# Group by nation and calculate revenue
revenue_df = pd.merge(joined_df, asia_nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
result_df = revenue_df.groupby('N_NAME', as_index=False)['REVENUE'].sum().sort_values(by='REVENUE', ascending=False)

# Write the results to a CSV file
result_df.to_csv('query_output.csv', index=False)
