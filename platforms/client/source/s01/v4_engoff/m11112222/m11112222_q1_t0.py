import pandas as pd
import csv
from direct_redis import DirectRedis

# Connect to the Redis database
redis_hostname = 'redis'
redis_port = 6379
redis_db = 0
redis_client = DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)

# Access the lineitem table from Redis and convert to Pandas DataFrame
lineitem_data = redis_client.get('lineitem')
df_lineitem = pd.read_json(lineitem_data)

# Filter data for lineitems shipped before 1998-09-02
df_filtered = df_lineitem[df_lineitem['L_SHIPDATE'] < '1998-09-02']

# Create the aggregates
summary = df_filtered.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
    Total_Quantity=('L_QUANTITY', 'sum'),
    Total_ExtendedPrice=('L_EXTENDEDPRICE', 'sum'),
    Total_DiscountedPrice=('L_EXTENDEDPRICE', lambda x: (x * (1 - df_filtered['L_DISCOUNT'])).sum()),
    Total_DiscountedPrice_WithTax=('L_EXTENDEDPRICE', lambda x: ((x * (1 - df_filtered['L_DISCOUNT'])) * (1 + df_filtered['L_TAX'])).sum()),
    Average_Quantity=('L_QUANTITY', 'mean'),
    Average_ExtendedPrice=('L_EXTENDEDPRICE', 'mean'),
    Average_Discount=('L_DISCOUNT', 'mean'),
    Lineitem_Count=('L_ORDERKEY', 'count')
).reset_index()

# Sort the results by RETURNFLAG and LINESTATUS
summary_sorted = summary.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Write to CSV
summary_sorted.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
