import pymysql
import pymongo
import pandas as pd
from decimal import Decimal

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
nation_collection = mongodb_db['nation']

# Query MySQL
mysql_query = """
SELECT c.C_CUSTKEY, c.C_NAME, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as REVENUE, 
       c.C_ACCTBAL, c.C_ADDRESS, c.C_PHONE, c.C_COMMENT, o.O_CUSTKEY 
FROM customer c 
JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY 
WHERE l.L_RETURNFLAG = 'R' 
  AND o.O_ORDERDATE >= '1993-10-01' 
  AND o.O_ORDERDATE <= '1993-12-31' 
GROUP BY c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL, c.C_ADDRESS, c.C_PHONE, c.C_COMMENT
ORDER BY REVENUE ASC, c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL DESC;
"""
mysql_cursor.execute(mysql_query)

# Fetch MySQL Data and transform it to DataFrame
mysql_data = mysql_cursor.fetchall()
mysql_df = pd.DataFrame(mysql_data, columns=['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT', 'O_CUSTKEY'])

# Fetch Nation Data from MongoDB and convert it to DataFrame
nations_data = list(nation_collection.find({}, {'_id': 0, 'N_NATIONKEY': 1, 'N_NAME': 1}))
nations_df = pd.DataFrame(nations_data)

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Close MongoDB connection
mongodb_client.close()

# Merge MySQL and MongoDB data on Customer's Nation Key
merged_df = mysql_df.merge(nations_df, left_on='C_CUSTKEY', right_on='N_NATIONKEY')

# Select required columns only
final_df = merged_df[['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]

# Convert Decimal type to float to avoid issues while saving to CSV
final_df['REVENUE'] = final_df['REVENUE'].apply(float)
final_df['C_ACCTBAL'] = final_df['C_ACCTBAL'].apply(float)

# Write the results into the file 'query_output.csv'
final_df.to_csv('query_output.csv', index=False)
