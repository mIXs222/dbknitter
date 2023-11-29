import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

try:
    with mysql_connection.cursor() as cursor:
        # Query to select required data from mysql db (lineitem table)
        sql = """
        SELECT L_SUPPKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
        FROM lineitem
        WHERE L_SHIPDATE BETWEEN '1996-01-01' AND '1996-04-01'
        GROUP BY L_SUPPKEY
        """
        cursor.execute(sql)
        mysql_data = cursor.fetchall()

finally:
    mysql_connection.close()

# Convert MySQL data to DataFrame
mysql_df = pd.DataFrame(mysql_data, columns=['SUPPKEY', 'TOTAL_REVENUE'])

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)
supplier_data = redis_connection.get('supplier')
supplier_df = pd.read_json(supplier_data)

# Cast SUPPKEY to int for merging
supplier_df['S_SUPPKEY'] = supplier_df['S_SUPPKEY'].astype(int)

# Merge DataFrames on SUPPKEY
merged_df = pd.merge(
    supplier_df, 
    mysql_df, 
    left_on='S_SUPPKEY', 
    right_on='SUPPKEY'
)

# Find the max TOTAL_REVENUE
max_revenue = merged_df['TOTAL_REVENUE'].max()

# Filter rows that have TOTAL_REVENUE equal to max_revenue and sort by SUPPKEY
top_suppliers_df = merged_df[merged_df['TOTAL_REVENUE'] == max_revenue].sort_values(by='S_SUPPKEY')

# Select the required columns and save to CSV
top_suppliers_df.to_csv(
    'query_output.csv', 
    columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'], 
    index=False, 
    quoting=csv.QUOTE_NONNUMERIC
)
