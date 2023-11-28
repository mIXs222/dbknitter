# python code
import pandas as pd
import direct_redis
from datetime import datetime

# Connect to Redis
hostname = "redis"
port = 6379
database_name = 0
redis_client = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Load data from Redis
df_lineitem = pd.read_json(redis_client.get('lineitem'))

# Filter the data
filtered_df = df_lineitem[df_lineitem['L_SHIPDATE'] <= datetime(1998, 9, 2)]

# Perform aggregation
result = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
    SUM_QTY=('L_QUANTITY', 'sum'),
    SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
    SUM_DISC_PRICE=('L_EXTENDEDPRICE', lambda x: (x * (1 - filtered_df.loc[x.index, 'L_DISCOUNT'])).sum()),
    SUM_CHARGE=('L_EXTENDEDPRICE', lambda x: (x * (1 - filtered_df.loc[x.index, 'L_DISCOUNT'] + filtered_df.loc[x.index, 'L_TAX'])).sum()),
    AVG_QTY=('L_QUANTITY', 'mean'),
    AVG_PRICE=('L_EXTENDEDPRICE', 'mean'),
    AVG_DISC=('L_DISCOUNT', 'mean'),
    COUNT_ORDER=('L_ORDERKEY', 'count')
).reset_index()

# Sort the results
final_result = result.sort_values(['L_RETURNFLAG', 'L_LINESTATUS'])

# Write to CSV
final_result.to_csv('query_output.csv', index=False)
