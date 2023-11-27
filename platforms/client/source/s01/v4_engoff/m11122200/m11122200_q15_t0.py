# top_supplier.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query to retrieve lineitem from MySQL
lineitem_query = """
SELECT
    L_SUPPKEY,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1996-01-01' AND
    L_SHIPDATE < '1996-04-01'
GROUP BY
    L_SUPPKEY
"""

# Execute the query on MySQL
lineitem_df = pd.read_sql(lineitem_query, mysql_conn)

# Get supplier table from Redis as a string and convert it to Pandas DataFrame
supplier_encoded = redis_conn.get('supplier')
supplier_df = pd.read_json(supplier_encoded, orient='records')

# Combine lineitem and supplier DataFrames
combined_df = pd.merge(
    lineitem_df,
    supplier_df,
    left_on='L_SUPPKEY',
    right_on='S_SUPPKEY'
)

# Find the max revenue
max_revenue = combined_df['revenue'].max()

# Select only the top suppliers
top_suppliers = combined_df[combined_df['revenue'] == max_revenue]

# Sort by supplier key
sorted_top_suppliers = top_suppliers.sort_values(by=['S_SUPPKEY'])

# Write to CSV file
sorted_top_suppliers.to_csv('query_output.csv', index=False)

# Close the database connections
mysql_conn.close()
redis_conn.close()
