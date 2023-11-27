import pandas as pd
import pymysql
from datetime import datetime, timedelta
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    passwd='my-secret-pw',
    db='tpch'
)

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load lineitem from Redis
lineitem_df = pd.DataFrame(redis_conn.get('lineitem'))
# Convert data types for date comparison and arithmetic operations
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df['L_EXTENDEDPRICE'] = pd.to_numeric(lineitem_df['L_EXTENDEDPRICE'])
lineitem_df['L_DISCOUNT'] = pd.to_numeric(lineitem_df['L_DISCOUNT'])

# Date range for lineitem
start_date = datetime(1996, 1, 1)
end_date = start_date + timedelta(days=90)

# Create revenue0 DataFrame
revenue0_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] < end_date)
].groupby('L_SUPPKEY').agg(
    TOTAL_REVENUE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc=lambda x: (x * (1 - lineitem_df.loc[x.index, 'L_DISCOUNT'])).sum())
).reset_index()
revenue0_df.rename(columns={'L_SUPPKEY': 'SUPPLIER_NO'}, inplace=True)

# Get Supplier DataFrame from MySQL
supplier_sql = "SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE FROM supplier"
supplier_df = pd.read_sql(supplier_sql, mysql_conn)

# Merge supplier and revenue0 on SUPPLIER_NO
result_df = supplier_df.merge(revenue0_df, left_on='S_SUPPKEY', right_on='SUPPLIER_NO', how='inner')

# Get the supplier with the max TOTAL_REVENUE
max_total_revenue = revenue0_df['TOTAL_REVENUE'].max()
result_df = result_df[result_df['TOTAL_REVENUE'] == max_total_revenue]

# Sort the result by S_SUPPKEY
result_df.sort_values(by='S_SUPPKEY', inplace=True)

# Output to CSV
result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
redis_conn.close()
