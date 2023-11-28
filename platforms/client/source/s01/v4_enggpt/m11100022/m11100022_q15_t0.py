import pandas as pd
import pymysql
import direct_redis
from datetime import datetime

# Connecting to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Create a pandas dataframe to query supplier data from MySQL
supplier_query = "SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE FROM supplier;"
df_supplier = pd.read_sql(supplier_query, mysql_connection)
mysql_connection.close()

# Connecting to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379)

# Load lineitem data from Redis as Pandas DataFrame
df_lineitem = pd.read_json(redis_connection.get('lineitem'), orient='records')

# Filter lineitem data by the specified date range
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 3, 31)
df_lineitem['L_SHIPDATE'] = pd.to_datetime(df_lineitem['L_SHIPDATE'])
df_lineitem = df_lineitem[(df_lineitem['L_SHIPDATE'] >= start_date) & (df_lineitem['L_SHIPDATE'] <= end_date)]

# Calculate the revenue
df_lineitem['TOTAL_REVENUE'] = df_lineitem['L_EXTENDEDPRICE'] * (1 - df_lineitem['L_DISCOUNT'])

# Calculate total revenue for each supplier (revenue0 CTE substitute)
revenue0 = df_lineitem.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()

# Merge the supplier data with the revenue data
result = df_supplier.merge(revenue0, how='inner', left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Filter to get the supplier with the maximum revenue
max_revenue_supplier = result[result['TOTAL_REVENUE'] == result['TOTAL_REVENUE'].max()]

# Order the result based on S_SUPPKEY
max_revenue_supplier = max_revenue_supplier.sort_values(by='S_SUPPKEY').reset_index(drop=True)

# Write the results to CSV
max_revenue_supplier.to_csv('query_output.csv', index=False)
