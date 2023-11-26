# Filename: query_redis.py

import pandas as pd
import direct_redis
from datetime import datetime

# Connection information for Redis
hostname = 'redis'
port = 6379
db_name = '0'  # Redis does not have named databases, but uses integers 0-15

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host=hostname, port=port, db=db_name)

# Load lineitem table from Redis
lineitem_data_str = redis_conn.get('lineitem')
lineitem_df = pd.read_json(lineitem_data_str)

# Filter the data based on L_SHIPDATE and perform aggregations
filtered_df = lineitem_df[
    lineitem_df['L_SHIPDATE'] <= datetime.strptime('1998-09-02', '%Y-%m-%d').date()
]

# Perform aggregations
result_df = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
    SUM_QTY=('L_QUANTITY', 'sum'),
    SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
    SUM_DISC_PRICE=('L_EXTENDEDPRICE', lambda x: (x * (1 - filtered_df['L_DISCOUNT'])).sum()),
    SUM_CHARGE=('L_EXTENDEDPRICE', lambda x: (x * (1 - filtered_df['L_DISCOUNT']) * (1 + filtered_df['L_TAX'])).sum()),
    AVG_QTY=('L_QUANTITY', 'mean'),
    AVG_PRICE=('L_EXTENDEDPRICE', 'mean'),
    AVG_DISC=('L_DISCOUNT', 'mean'),
    COUNT_ORDER=('L_ORDERKEY', 'count')
).reset_index()

# Sorting the results
sorted_result_df = result_df.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Write the query output to CSV
sorted_result_df.to_csv('query_output.csv', index=False)
