# query.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Query to fetch data from MySQL tables
mysql_query = """
SELECT 
    O.O_ORDERKEY, L.L_EXTENDEDPRICE, L.L_DISCOUNT, O.O_CUSTKEY
FROM 
    orders as O
JOIN 
    lineitem as L ON O.O_ORDERKEY = L.L_ORDERKEY
WHERE 
    O.O_ORDERDATE >= '1993-10-01' AND O.O_ORDERDATE < '1994-01-01' AND L.L_RETURNFLAG = 'R'
"""

# Executing MySQL query and fetching the data
mysql_result = pd.read_sql_query(mysql_query, mysql_conn)

# Calculate lost revenue
mysql_result['LOST_REVENUE'] = mysql_result.apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']), axis=1)

# Group by O_CUSTKEY and sum the lost revenues
mysql_revenue = mysql_result.groupby('O_CUSTKEY')['LOST_REVENUE'].sum().reset_index()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client['tpch']
nation_collection = mongodb_db['nation']

# Get all nations from MongoDB
nations_df = pd.DataFrame(list(nation_collection.find()))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
customer_data = redis_client.get('customer')
customers_df = pd.read_json(customer_data)

# Merge data from Redis and MySQL, then with the nations
merged_data = pd.merge(customers_df, mysql_revenue, how='right', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_data = pd.merge(merged_data, nations_df, how='left', left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Filter out only required columns and sort
output_data = merged_data[['C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT', 'LOST_REVENUE']]
output_data.columns = ['CUSTOMER_NAME', 'ADDRESS', 'NATION', 'PHONE_NUMBER', 'ACCOUNT_BALANCE', 'COMMENT', 'LOST_REVENUE']
output_data.sort_values(by=['LOST_REVENUE', 'C_CUSTKEY', 'CUSTOMER_NAME', 'ACCOUNT_BALANCE'], ascending=[False, True, True, True], inplace=True)

# Write the query's output to a CSV file
output_data.to_csv('query_output.csv', index=False)
