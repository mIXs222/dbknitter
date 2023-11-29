import pandas as pd
from direct_redis import DirectRedis

# Connection to Redis server
redis_hostname = 'redis'
redis_port = 6379
redis_db = 0

# Create a connection
redis_client = DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)

# Retrieve the lineitem table as a DataFrame
lineitem_table = redis_client.get("lineitem")
lineitem_df = pd.read_json(lineitem_table)
lineitem_df.columns = ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE',
                       'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE',
                       'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT']

# Filtering the DataFrame
filtered_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] > '1994-01-01') & 
    (lineitem_df['L_SHIPDATE'] < '1995-01-01') &
    (lineitem_df['L_DISCOUNT'] > 0.05) & 
    (lineitem_df['L_DISCOUNT'] < 0.07) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Calculate revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']

# Output the result to the file
output_df = filtered_df[['REVENUE']].sum().reset_index()
output_df.columns = ['_', 'REVENUE']
output_df.to_csv('query_output.csv', index=False)
