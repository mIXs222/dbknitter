import pandas as pd
from direct_redis import DirectRedis
import datetime
import csv

# Establish a connection to Redis
hostname = 'redis'
port = 6379
redis_client = DirectRedis(host=hostname, port=port)

# Retrieve `lineitem` table stored in Redis
lineitem_data = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Define the date range and discount range
date_start = datetime.date(1994, 1, 2)
date_end = datetime.date(1995, 1, 1)
discount_min = 0.06 - 0.01
discount_max = 0.06 + 0.01

# Performing the query
filtered_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] > str(date_start)) &
    (lineitem_df['L_SHIPDATE'] < str(date_end)) &
    (lineitem_df['L_DISCOUNT'] >= discount_min) &
    (lineitem_df['L_DISCOUNT'] <= discount_max) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Calculate the revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']

# Sum up the revenue
total_revenue = filtered_df['REVENUE'].sum()

# Output to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['REVENUE'])
    writer.writerow([total_revenue])
