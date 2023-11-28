import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Prepare the SQL query for MySQL part and partsupp tables
mysql_query = """
SELECT p.P_BRAND, p.P_TYPE, p.P_SIZE, COUNT(DISTINCT ps.PS_SUPPKEY) AS SUPPLIER_CNT
FROM part p 
JOIN partsupp ps ON p.P_PARTKEY = ps.PS_PARTKEY
WHERE p.P_BRAND != 'Brand#45'
AND p.P_TYPE NOT LIKE 'MEDIUM POLISHED%'
AND p.P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
GROUP BY p.P_BRAND, p.P_TYPE, p.P_SIZE
"""

# Execute the query and read data into a pandas DataFrame
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    parts_and_suppliers = pd.DataFrame(cursor.fetchall(), columns=['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT'])

# Close the connection
mysql_conn.close()

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get the supplier data as a pandas DataFrame
supplier_json = redis_client.get('supplier')
supplier_df = pd.read_json(supplier_json)

# Filter based on the supplier comment condition
supplier_df = supplier_df[~supplier_df['S_COMMENT'].str.contains('Customer Complaints')]

# Combine the results (performing manual semi-join on P_SIZE available in both DataFrames)
final_df = parts_and_suppliers[parts_and_suppliers['P_SIZE'].isin(supplier_df['S_SUPPKEY'])]

# Order the results as specified
final_df = final_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write results to CSV
final_df.to_csv('query_output.csv', index=False)
