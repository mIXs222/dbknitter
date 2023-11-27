import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis database
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query to extract the necessary data from MySQL
mysql_query = """
SELECT 
    o.O_ORDERDATE, 
    l.L_EXTENDEDPRICE, 
    l.L_DISCOUNT, 
    l.L_QUANTITY, 
    s.S_NAME
FROM 
    orders o
JOIN 
    lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
JOIN 
    supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
"""
mysql_cursor.execute(mysql_query)
order_lineitem_supplier = pd.DataFrame(mysql_cursor.fetchall(), columns=['O_ORDERDATE', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_QUANTITY', 'S_NAME'])

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Query to get partsupp from Redis
partsupp_df = pd.read_json(redis_client.get('partsupp'), orient='records')

# Fetch nation data from MongoDB
nation_data = list(mongo_db.nation.find({}, {'_id': 0}))
nation_df = pd.DataFrame(nation_data)

# Combine the data
combined_df = order_lineitem_supplier.merge(partsupp_df, how='inner', left_on='L_QUANTITY', right_on='PS_AVAILQTY')
combined_df = combined_df.merge(nation_df, how='inner', left_on='S_NAME', right_on='N_NAME')

# Calculate profit
combined_df['year'] = pd.to_datetime(combined_df['O_ORDERDATE']).dt.year
combined_df['profit'] = (combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])) - (combined_df['PS_SUPPLYCOST'] * combined_df['L_QUANTITY'])

# Create the final aggregated dataframe
result_df = combined_df.groupby(['N_NAME', 'year']).agg({'profit': 'sum'}).reset_index()

# Sort the results according to the requirements
result_df.sort_values(by=['N_NAME', 'year'], ascending=[True, False], inplace=True)

# Write the result to CSV file
result_df.to_csv('query_output.csv', index=False)
