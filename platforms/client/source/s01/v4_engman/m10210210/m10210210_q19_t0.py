# revenue_query.py
import pymysql
import pandas as pd
import direct_redis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
mysql_query = """
SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM lineitem
WHERE (
            (L_SHIPMODE = 'AIR' OR L_SHIPMODE = 'AIR REG') AND
            L_SHIPINSTRUCT = 'DELIVER IN PERSON'
      )
"""

# Execute query and fetch results
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    result = cursor.fetchone()

# Convert MySQL result to dataframe
df_mysql = pd.DataFrame([result], columns=['REVENUE'])

# Redis connection
redis_conn = direct_redis.DirectRedis(port=6379, host='redis')
part_keys = ['part:12:container', 'part:23:container', 'part:34:container']

# Fetch Redis data frames
redis_data_frames = []
for key in part_keys:
    # Fetching dataframe from Redis
    ser = redis_conn.get(key)  # Assuming the key structure should be determined
    if ser:
        df_redis = pd.read_msgpack(ser)
        redis_data_frames.append(df_redis)
        
# Combining Redis data frames
df_redis_combined = pd.concat(redis_data_frames, ignore_index=True) if redis_data_frames else pd.DataFrame()

# Merging MySQL and Redis data frames based on part keys
df_merged = pd.merge(df_mysql, df_redis_combined, how='inner', left_on=['L_PARTKEY'], right_on=['P_PARTKEY'])

# Perform calculation on merged dataframe
df_merged['REVENUE'] = df_merged.apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']), axis=1)

# Sum and output
total_revenue = df_merged['REVENUE'].sum()
final_output = pd.DataFrame({'REVENUE': [total_revenue]})
final_output.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
