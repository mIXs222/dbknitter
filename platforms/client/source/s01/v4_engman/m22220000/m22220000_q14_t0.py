# query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query to select lineitems within the date range
lineitem_query = """
SELECT L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT
FROM lineitem
WHERE L_SHIPDATE >= '1995-09-01' AND L_SHIPDATE < '1995-10-01'
"""
lineitem_df = pd.read_sql(lineitem_query, mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to the Redis database
redis = DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'part' table data from Redis as Pandas DataFrame
part_df = pd.read_json(redis.get('part'), orient='records')

# Data Preprocessing
lineitem_df['REVENUE'] = lineitem_df.L_EXTENDEDPRICE * (1 - lineitem_df.L_DISCOUNT)

# Merge the dataframes on PARTKEY
merged_df = lineitem_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# The user assumes that parts considered "promotional" can be identified by a flag
# or attribute, and stored in SQL DBMS. This needs to be checked if there is
# such a column in the database. Here we assume P_COMMENT includes 'Promotion'
# keyword for promotional parts.

# Filter promotional parts from merged dataframes
promotional_df = merged_df[merged_df.P_COMMENT.str.contains("Promotion", na=False)]

# Calculate the total revenue and total promotional revenue
total_revenue = lineitem_df['REVENUE'].sum()
promotional_revenue = promotional_df['REVENUE'].sum()

# Calculate the percentage
percentage_promotional_revenue = (promotional_revenue / total_revenue) * 100

# Output to CSV
result_df = pd.DataFrame({'Percentage': [percentage_promotional_revenue]})
result_df.to_csv('query_output.csv', index=False)
