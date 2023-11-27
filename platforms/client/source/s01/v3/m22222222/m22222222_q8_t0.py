import redis
import pandas as pd
import numpy as np

#Initialize connection to Redis
r = redis.Redis(host='redis', port=6379, db=0)

# Fetch data from the Redis database
tables = ["nation", "region", "part", "supplier", "customer", "orders", "lineitem"]
df = {table: pd.read_msgpack(r.get(table)) for table in tables if r.exists(table)}

# merging dataframes
m1 = df['orders'].merge(df['lineitem'], how='inner', left_on = 'O_ORDERKEY', right_on = 'L_ORDERKEY')
m2 = m1.merge(df['customer'], how='inner', left_on = 'O_CUSTKEY', right_on = 'C_CUSTKEY')
m3 = m2.merge(df['nation'], how='inner', left_on = 'C_NATIONKEY', right_on = 'N_NATIONKEY')
m4 = m3.merge(df['region'], how='inner', left_on = 'N_REGIONKEY', right_on = 'R_REGIONKEY')
final_df = m4.merge(df['part'], how='inner', left_on = 'L_PARTKEY', right_on = 'P_PARTKEY')

# additional filtering
final_df = final_df[final_df['R_NAME'] == 'ASIA']
final_df = final_df[final_df['O_ORDERDATE'].between('1995-01-01', '1996-12-31')]
final_df = final_df[final_df['P_TYPE'] == 'SMALL PLATED COPPER']

# create volume and year fields
final_df['VOLUME'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])
final_df['O_YEAR'] = final_df['O_ORDERDATE'].dt.strftime('%Y')

# grouping and aggregating data
grouped = final_df.groupby(['O_YEAR', 'N_NAME'])['VOLUME'].sum().reset_index()

# calculate MKT_SHARE
total_volume = grouped['VOLUME'].sum()
grouped['MKT_SHARE'] = grouped.apply(lambda row: row['VOLUME'] if row['N_NAME']=='INDIA' else 0, axis=1) / total_volume

result_df = grouped[['O_YEAR', 'MKT_SHARE']]

# Save to csv
result_df.to_csv('query_output.csv', index=False)
