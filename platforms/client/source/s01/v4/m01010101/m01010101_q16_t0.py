import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']

# Get the supplier ids to exclude
excluded_suppliers = supplier_collection.find({'S_COMMENT': {'$regex': '.*Customer.*Complaints.*'}}, {'S_SUPPKEY': 1})
excluded_supplier_ids = [supplier['S_SUPPKEY'] for supplier in excluded_suppliers]

# Prepare the SQL query, excluding the supplier ids retrieved from MongoDB
mysql_query = f"""
SELECT
    P_BRAND,
    P_TYPE,
    P_SIZE,
    COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
FROM
    partsupp, part
WHERE
    P_PARTKEY = PS_PARTKEY
    AND P_BRAND <> 'Brand#45'
    AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
    AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND PS_SUPPKEY NOT IN ({','.join(map(str, excluded_supplier_ids))})
GROUP BY
    P_BRAND,
    P_TYPE,
    P_SIZE
ORDER BY
    SUPPLIER_CNT DESC,
    P_BRAND,
    P_TYPE,
    P_SIZE
"""

# Execute the MySQL query
with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_result = cursor.fetchall()

# Store MySQL results in a DataFrame
df = pd.DataFrame(mysql_result, columns=['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT'])

# Close MySQL connection
mysql_connection.close()

# Write results to a CSV file
df.to_csv('query_output.csv', index=False)
