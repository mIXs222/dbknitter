import pymysql
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Query to select suppliers and join with the revenue from line items
supplier_query = """
SELECT
    s.S_SUPPKEY,
    s.S_NAME,
    s.S_ADDRESS,
    s.S_PHONE,
    COALESCE(revenue0.total_revenue, 0) as total_revenue
FROM
    supplier s
LEFT JOIN (
    SELECT
        L_SUPPKEY as SUPPLIER_NO,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as total_revenue
    FROM
        lineitem
    WHERE
        L_SHIPDATE BETWEEN '1996-01-01' AND '1996-03-31'
    GROUP BY
        L_SUPPKEY
) as revenue0
ON
    s.S_SUPPKEY = revenue0.SUPPLIER_NO
ORDER BY
    total_revenue DESC
LIMIT 1;
"""

# Execute query on MySQL database for supplier data
with mysql_connection.cursor() as cursor:
    cursor.execute(supplier_query)
    supplier_data = cursor.fetchall()

# Schema for supplier data retrieved from MySQL
supplier_col_names = ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE", "total_revenue"]
supplier_df = pd.DataFrame(supplier_data, columns=supplier_col_names)

# Connect to Redis database and get lineitem data
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Get 'lineitem' table from Redis
lineitem_df = redis_connection.get('lineitem')

# Parse shipping dates and filter records within the timeframe
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= datetime(1996, 1, 1)) &
                          (lineitem_df['L_SHIPDATE'] <= datetime(1996, 3, 31))]

# Calculate total revenue per supplier
lineitem_revenue = lineitem_df.groupby('L_SUPPKEY')['L_EXTENDEDPRICE'].sum().reset_index()
lineitem_revenue['total_revenue'] = lineitem_revenue['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Merge with supplier data and output to CSV
output_df = pd.merge(supplier_df, lineitem_revenue, left_on='S_SUPPKEY', right_on='L_SUPPKEY', how='left')
output_df.sort_values('S_SUPPKEY', ascending=True, inplace=True)
output_df.to_csv('query_output.csv', index=False)
