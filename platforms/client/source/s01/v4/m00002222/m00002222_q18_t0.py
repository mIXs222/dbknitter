import pandas as pd
from direct_redis import DirectRedis

# Connect to the Redis database
redis_host = "redis"
redis_port = 6379
redis_db = 0
r = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Get the dataframes from the redis
df_customer = pd.read_json(r.get('customer'))
df_orders = pd.read_json(r.get('orders'))
df_lineitem = pd.read_json(r.get('lineitem'))

# Perform the SQL operation using Pandas
lineitem_grouped = df_lineitem.groupby('L_ORDERKEY').filter(lambda x: x['L_QUANTITY'].sum() > 300).groupby('L_ORDERKEY')

# Merge the tables on the relevant keys
df_merge = df_customer.merge(df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df_merge = df_merge.merge(df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter based on the condition in the SQL WHERE clause
df_filtered = df_merge[df_merge['L_ORDERKEY'].isin(lineitem_grouped.groups.keys())]

# Group by the necessary fields
df_result = df_filtered.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).agg({
    'L_QUANTITY': 'sum'
}).reset_index()

# Order by the given fields
df_result = df_result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write to CSV
df_result.to_csv('query_output.csv', index=False)
