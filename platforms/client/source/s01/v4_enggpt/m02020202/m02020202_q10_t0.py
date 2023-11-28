import pymysql
import pandas as pd
from sqlalchemy import create_engine
from direct_redis import DirectRedis


# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

# Query to retrieve data from 'orders' and 'nation' tables in MySQL
mysql_query = """
SELECT
    o.O_CUSTKEY,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE,
    n.N_NAME
FROM
    orders o
JOIN
    nation n ON o.O_NATIONKEY = n.N_NATIONKEY
WHERE
    o.O_ORDERDATE >= '1993-10-01' AND o.O_ORDERDATE <= '1993-12-31'
GROUP BY
    o.O_CUSTKEY, n.N_NAME;
"""

# Execute MySQL query and fetch the results
with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_result = cursor.fetchall()

# Create DataFrame from MySQL result
mysql_df = pd.DataFrame(mysql_result, columns=['C_CUSTKEY', 'REVENUE', 'N_NAME'])

# Close MySQL connection
mysql_connection.close()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load Redis data into DataFrames
customer_df = pd.DataFrame(redis_client.get('customer'))
lineitem_df = pd.DataFrame(redis_client.get('lineitem'))

# Filter for line items with return flag 'R'
lineitem_df = lineitem_df[lineitem_df['L_RETURNFLAG'] == 'R']

# Merge Redis and MySQL DataFrames
combined_df = customer_df.merge(mysql_df, on='C_CUSTKEY', how='inner')
combined_df = combined_df.merge(lineitem_df[['L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT']], left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')

# Calculate Revenue
combined_df['REVENUE'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])

# Select and order the required columns
final_df = combined_df[['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]
final_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False], inplace=True)

# Write the result to CSV
final_df.to_csv('query_output.csv', index=False)
