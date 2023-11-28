import direct_redis
import pandas as pd
from datetime import datetime

# Establish connection to Redis
hostname = 'redis'
port = 6379
database_name = '0'
r = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Retrieve lineitem DataFrame from Redis
lineitem = r.get('lineitem')
lineitem = pd.read_json(lineitem)

# Filtering the data
filtered_lineitem = lineitem[
    (lineitem['L_SHIPDATE'] >= '1994-01-01') &
    (lineitem['L_SHIPDATE'] <= '1994-12-31') &
    (lineitem['L_DISCOUNT'] >= 0.05) & 
    (lineitem['L_DISCOUNT'] <= 0.07) & 
    (lineitem['L_QUANTITY'] < 24)
]

# Calculate the total revenue
filtered_lineitem['REVENUE'] = filtered_lineitem['L_EXTENDEDPRICE'] * (1 - filtered_lineitem['L_DISCOUNT'])
total_revenue = filtered_lineitem['REVENUE'].sum()

# Output the result to a CSV file
result_df = pd.DataFrame([{'Total_Revenue': total_revenue}])
result_df.to_csv('query_output.csv', index=False)
