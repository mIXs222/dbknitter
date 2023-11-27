import pandas as pd
import pymysql
from datetime import datetime
import direct_redis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from Redis
try:
    lineitem_df = pd.read_json(redis_connection.get('lineitem'))
    lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
    filtered_lineitem = lineitem_df[(lineitem_df['L_SHIPDATE'] >= '1996-01-01') & (lineitem_df['L_SHIPDATE'] <= '1996-04-01')]
except Exception as e:
    print(f"An error occurred while fetching data from Redis: {e}")
    filtered_lineitem = pd.DataFrame()

# Compute the revenue contribution for each supplier from lineitem
revenue_by_suppkey = filtered_lineitem.groupby('L_SUPPKEY')['L_EXTENDEDPRICE'].sum().reset_index()
revenue_by_suppkey.rename(columns={'L_SUPPKEY': 'S_SUPPKEY', 'L_EXTENDEDPRICE': 'TOTAL_REVENUE'}, inplace=True)

# Get supplier data from MySQL
try:
    with mysql_connection.cursor() as cursor:
        cursor.execute("SELECT S_SUPPKEY, S_NAME FROM supplier")
        supplier_df = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME'])
except pymysql.MySQLError as e:
    print(f"Error connecting to MySQL Platform: {e}")
    supplier_df = pd.DataFrame()

# Merge data from MySQL and Redis
merged_df = pd.merge(supplier_df, revenue_by_suppkey, on='S_SUPPKEY', how='inner')

# Find the top supplier(s) based on revenue
max_revenue = merged_df['TOTAL_REVENUE'].max()
top_suppliers = merged_df[merged_df['TOTAL_REVENUE'] == max_revenue]

# Sort by supplier number and write to CSV
top_suppliers.sort_values('S_SUPPKEY').to_csv('query_output.csv', index=False)
