import pymysql
import pandas as pd
import direct_redis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Query to select parts from MySQL that match the criteria
mysql_query = """
SELECT 
    p.P_BRAND, p.P_TYPE, p.P_SIZE, COUNT(ps.PS_SUPPKEY) as supplier_count
FROM 
    part p 
JOIN 
    partsupp ps ON p.P_PARTKEY = ps.PS_PARTKEY
WHERE 
    p.P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9) AND
    p.P_TYPE NOT LIKE 'MEDIUM POLISHED%' AND
    p.P_BRAND <> 'Brand#45'
GROUP BY 
    p.P_BRAND, p.P_TYPE, p.P_SIZE
HAVING 
    COUNT(ps.PS_SUPPKEY) > 0
ORDER BY 
    supplier_count DESC, p.P_BRAND, p.P_TYPE, p.P_SIZE;
"""

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    part_suppliers = cursor.fetchall()

# Column names for the result
columns = ['P_BRAND', 'P_TYPE', 'P_SIZE', 'supplier_count']

# Convert MySQL data to DataFrame
df_mysql = pd.DataFrame(part_suppliers, columns=columns)

# Close MySQL connection
mysql_conn.close()

# Now, connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch the 'supplier' table data from Redis
supplier_data = redis_conn.get('supplier')

# If supplier_data is not None, we assume it's a JSON string and load it into a DataFrame
if supplier_data:
    df_supplier = pd.read_json(supplier_data)
    
    # Filter out suppliers with complaints
    df_supplier = df_supplier[df_supplier['S_COMMENT'].str.contains('Customer') == False]
    # Create a set of unique supplier keys without complaints
    suppliers_no_complaints = set(df_supplier['S_SUPPKEY'].unique())
    
    # Filter the MySQL DataFrame to only include parts supplied by suppliers without complaints
    df_mysql['supplier_count'] = df_mysql.apply(lambda row: len([suppkey for suppkey in row['supplier_count']
                                                                if suppkey in suppliers_no_complaints]), axis=1)

# Write the data to a CSV file
df_mysql.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
