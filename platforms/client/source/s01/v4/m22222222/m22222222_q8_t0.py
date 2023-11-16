# query.py
import pandas as pd
from direct_redis import DirectRedis
import datetime

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Get the data from Redis
part_df = pd.read_json(redis.get('part'))
supplier_df = pd.read_json(redis.get('supplier'))
lineitem_df = pd.read_json(redis.get('lineitem'))
orders_df = pd.read_json(redis.get('orders'))
customer_df = pd.read_json(redis.get('customer'))
nation_df = pd.read_json(redis.get('nation'))
region_df = pd.read_json(redis.get('region'))

# Merge dataframes to simulate the SQL joins
merged_df = part_df.merge(supplier_df, left_on='P_PARTKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')
merged_df = merged_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.rename(columns={"N_NAME": "NATION"})
merged_df = merged_df.merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Filter the data according to the WHERE clause
filtered_df = merged_df[
    (merged_df['R_NAME'] == 'ASIA') &
    (merged_df['P_TYPE'] == 'SMALL PLATED COPPER') &
    (merged_df['O_ORDERDATE'] >= datetime.date(1995, 1, 1)) &
    (merged_df['O_ORDERDATE'] <= datetime.date(1996, 12, 31))
]

# Select and transform the necessary columns
filtered_df['O_YEAR'] = filtered_df['O_ORDERDATE'].dt.strftime('%Y')
filtered_df['VOLUME'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Compute the market share
result = filtered_df.groupby('O_YEAR').apply(
    lambda df: pd.Series({
        'MKT_SHARE': df[df['NATION'] == 'INDIA']['VOLUME'].sum() / df['VOLUME'].sum()
    })
).reset_index()

# Save the result to CSV
result.to_csv('query_output.csv', index=False)
