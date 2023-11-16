import pandas as pd
from direct_redis import DirectRedis
import csv

# Initialize connection to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Function to get a DataFrame from Redis table
def get_table_from_redis(table_name):
    table_json = redis_conn.get(table_name) # Get table data as json
    if table_json is not None:
        return pd.read_json(table_json)
    else:
        return None

# Load the data
customer_df = get_table_from_redis('customer')
orders_df = get_table_from_redis('orders')
lineitem_df = get_table_from_redis('lineitem')

# Merge dataframes
merged_df = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']\
    .merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')\
    .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter data according to conditions
filtered_df = merged_df[
    (merged_df['O_ORDERDATE'] < '1995-03-15') &
    (merged_df['L_SHIPDATE'] > '1995-03-15')
]

# Perform calculations and group by
result_df = filtered_df.assign(REVENUE=filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT']))\
    .groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])\
    .agg({'REVENUE': 'sum'})\
    .reset_index()

# Sort the result
result_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Write output to CSV file
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
