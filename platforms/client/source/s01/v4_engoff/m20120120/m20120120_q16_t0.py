# import required modules
import pymysql
import pandas as pd
import pymongo
import redis
import csv
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_part = mongo_db['part']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Define the valid sizes for parts
valid_sizes = [49, 14, 23, 45, 19, 3, 36, 9]

# Query MySQL database to find suppliers who can supply parts that match the given criteria
mysql_query = """
SELECT PS_PARTKEY, PS_SUPPKEY
FROM partsupp
WHERE PS_PARTKEY IN (SELECT P_PARTKEY FROM part WHERE P_SIZE IN %s AND P_BRAND <> 'Brand#45' AND P_TYPE NOT LIKE 'MEDIUM POLISHED%%')
"""
mysql_cursor.execute(mysql_query, (valid_sizes,))
partsupp_records = mysql_cursor.fetchall()

# Create a dataframe for partsupp
partsupp_df = pd.DataFrame(partsupp_records, columns=['PS_PARTKEY', 'PS_SUPPKEY'])

# Query MongoDB for parts that match the given criteria
mongo_results = mongo_part.find({
    'P_SIZE': {'$in': valid_sizes},
    'P_BRAND': {'$ne': 'Brand#45'},
    'P_TYPE': {'$not': {'$regex': 'MEDIUM POLISHED'}},
})

# Create a dataframe for parts
part_df = pd.DataFrame(list(mongo_results))

# Get supplier data from Redis
supplier_data = redis_client.get('supplier')
supplier_df = pd.read_csv(supplier_data.decode())

# Filter suppliers with complaints
complaints_df = supplier_df[supplier_df['S_COMMENT'].str.contains('Customer')]
valid_suppliers = supplier_df[~supplier_df['S_SUPPKEY'].isin(complaints_df['S_SUPPKEY'])]

# Merge the partsupp and part dataframes based on part key
merged_df = pd.merge(partsupp_df, part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Further merge with supplier dataframe to exclude suppliers with complaints
final_df = pd.merge(merged_df, valid_suppliers[['S_SUPPKEY']], left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Group by the required fields and get counts
analysis_df = final_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['PS_SUPPKEY'].nunique().reset_index()

# Sort the results according to the specified conditions
sorted_df = analysis_df.sort_values(by=['PS_SUPPKEY', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write the results to query_output.csv
sorted_df.to_csv('query_output.csv', index=False)

# Close all database connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
