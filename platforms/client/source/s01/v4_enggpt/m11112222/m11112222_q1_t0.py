import pandas as pd
import direct_redis

# Connection information for Redis
redis_host = 'redis'
redis_port = 6379
redis_db = 0

# Create a connection to Redis Database
dredis = direct_redis.DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Getting the data from Redis
lineitem_df = pd.read_json(dredis.get('lineitem'))

# Convert dates to pandas datetime
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter data based on shipping date criteria
filtered_df = lineitem_df[
    lineitem_df['L_SHIPDATE'] <= pd.Timestamp('1998-09-02')
]

# Calculating aggregates
result = (filtered_df
        .groupby(['L_RETURNFLAG', 'L_LINESTATUS'])
        .agg(SUM_QTY=('L_QUANTITY', 'sum'),
             SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
             SUM_DISC_PRICE=('L_DISCOUNT', lambda x: (filtered_df['L_EXTENDEDPRICE'] * (1 - x)).sum()),
             SUM_CHARGE=lambda x: ((filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT']) * (1 + filtered_df['L_TAX'])).sum()),
             AVG_QTY=('L_QUANTITY', 'mean'),
             AVG_PRICE=('L_EXTENDEDPRICE', 'mean'),
             AVG_DISC=('L_DISCOUNT', 'mean'),
             COUNT_ORDER=('L_ORDERKEY', 'count'))
        .reset_index())

# Sorting results
sorted_result = result.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Exporting results to CSV
sorted_result.to_csv('query_output.csv', index=False)
