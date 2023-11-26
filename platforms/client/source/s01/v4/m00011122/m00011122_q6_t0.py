import pandas as pd
from direct_redis import DirectRedis

# Establishing connection to Redis
redis_hostname = 'redis'
redis_port = 6379
redis_db = 0
r = DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)

# Getting data from Redis
lineitem_df_redis = pd.read_json(r.get('lineitem'), orient='records')

# Performing necessary filtering and computations as per the SQL query
filtered_df_redis = lineitem_df_redis[
    (lineitem_df_redis['L_SHIPDATE'] >= '1994-01-01') &
    (lineitem_df_redis['L_SHIPDATE'] < '1995-01-01') &
    (lineitem_df_redis['L_DISCOUNT'] >= 0.05) &
    (lineitem_df_redis['L_DISCOUNT'] <= 0.07) &
    (lineitem_df_redis['L_QUANTITY'] < 24)
]

# Calculating revenue
revenue_redis = filtered_df_redis.eval('L_EXTENDEDPRICE * L_DISCOUNT').sum()

# Writing results to file
result_df = pd.DataFrame({'REVENUE': [revenue_redis]})
result_df.to_csv('query_output.csv', index=False)
