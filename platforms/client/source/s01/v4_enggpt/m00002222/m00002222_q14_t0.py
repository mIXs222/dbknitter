import pymysql
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Parameter constants
MYSQL_DETAILS = {'host': 'mysql', 'user': 'root', 'password': 'my-secret-pw', 'db': 'tpch'}
REDIS_HOST = 'redis'
REDIS_PORT = 6379

# Connect to MySQL
mysql_conn = pymysql.connect(host=MYSQL_DETAILS['host'],
                             user=MYSQL_DETAILS['user'],
                             password=MYSQL_DETAILS['password'],
                             db=MYSQL_DETAILS['db'])

# Define the date range for filtering
start_date = datetime.strptime('1995-09-01', '%Y-%m-%d')
end_date = datetime.strptime('1995-09-30', '%Y-%m-%d')

# Query to select parts of interest from MySQL
part_query = """
SELECT P_PARTKEY, P_TYPE 
FROM part 
WHERE P_TYPE LIKE 'PROMO%%';
"""
parts_df = pd.read_sql(part_query, mysql_conn)

# Connect to Redis
redis_conn = DirectRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# Retrieve the lineitem DataFrame
lineitem_df = pd.read_msgpack(redis_conn.get('lineitem'))

# Close MySQL connection
mysql_conn.close()

# Convert dates in 'lineitem_df' to datetime objects and filter by date range
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] <= end_date)]

# Merge the dataframes on part keys
merged_df = pd.merge(lineitem_df, parts_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate promotional and total revenue
merged_df['TOTAL_REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
promotional_df = merged_df[merged_df['P_TYPE'].str.startswith('PROMO')]
promotional_revenue = promotional_df['TOTAL_REVENUE'].sum()
total_revenue = merged_df['TOTAL_REVENUE'].sum()

# Calculate the promotional revenue as a percentage of the total revenue
promo_revenue_percentage = (promotional_revenue / total_revenue) * 100 if total_revenue else 0

# Write the result to a CSV file
output_df = pd.DataFrame({'Promotional_Revenue_Percentage': [promo_revenue_percentage]})
output_df.to_csv('query_output.csv', index=False)
