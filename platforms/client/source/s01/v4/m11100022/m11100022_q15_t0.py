# script.py
import pymysql
import pandas as pd
from datetime import datetime, timedelta
import direct_redis

# Configuration for MySQL connection
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Configuration for Redis connection
redis_conn_info = {
    'host': 'redis',
    'port': 6379,
    'db': 0,
}

# Connect to MySQL
mysql_connection = pymysql.connect(**mysql_conn_info)
mysql_cursor = mysql_connection.cursor()

# Retrieve suppliers from MySQL
supplier_query = """SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE FROM supplier;"""
mysql_cursor.execute(supplier_query)
suppliers = pd.DataFrame(mysql_cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE'])

# Close MySQL connection
mysql_cursor.close()
mysql_connection.close()

# Connect to Redis and retrieve lineitem data
redis_connection = direct_redis.DirectRedis(**redis_conn_info)
lineitem_data = pd.read_json(redis_connection.get('lineitem'))

# Filter lineitem table based on shipdate
start_date = datetime(1996, 1, 1)
end_date = start_date + timedelta(days=90)
mask = (lineitem_data['L_SHIPDATE'] >= start_date.strftime('%Y-%m-%d')) & (lineitem_data['L_SHIPDATE'] < end_date.strftime('%Y-%m-%d'))
lineitem_filtered = lineitem_data.loc[mask]

# Calculate revenue
revenue0 = lineitem_filtered.groupby('L_SUPPKEY').apply(lambda df: (df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])).sum()).reset_index(name='TOTAL_REVENUE')
revenue0.columns = ['SUPPLIER_NO', 'TOTAL_REVENUE']

# Combine suppliers and revenue data
combined_data = pd.merge(suppliers, revenue0, left_on='S_SUPPKEY', right_on='SUPPLIER_NO')

# Find the suppliers with the maximum revenue
max_total_revenue = combined_data['TOTAL_REVENUE'].max()
max_revenue_suppliers = combined_data[combined_data['TOTAL_REVENUE'] == max_total_revenue]

# Order by S_SUPPKEY
result = max_revenue_suppliers.sort_values('S_SUPPKEY')

# Write to output file
result.to_csv('query_output.csv', index=False)
