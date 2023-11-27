import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query execution
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Fetching forest parts from part table
mysql_cursor.execute("SELECT P_PARTKEY, P_NAME FROM part WHERE P_NAME LIKE 'forest%'")
forest_parts = mysql_cursor.fetchall()
forest_part_keys = [part[0] for part in forest_parts]

# Redis connection and data retrieval
redis_conn = DirectRedis(host='redis', port=6379, db=0)
supplier_info = pd.read_json(redis_conn.get('supplier'))
partsupp_info = pd.read_json(redis_conn.get('partsupp'))

# MongoDB connection and data retrieval
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Filter lineitem table for dates and forest parts
query = {
    'L_SHIPDATE': {'$gte': '1994-01-01', '$lt': '1995-01-01'},
    'L_PARTKEY': {'$in': forest_part_keys},
}
lineitem_info = pd.DataFrame(list(lineitem_collection.find(query)))

# Combine data and calculate excess
combined_data = pd.merge(lineitem_info, partsupp_info, left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
combined_data = pd.merge(combined_data, supplier_info, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Calculate sum of shipped parts and filter suppliers with excess
combined_data['SHIP_QTY'] = combined_data.groupby('S_SUPPKEY')['L_QUANTITY'].transform('sum')
combined_data = combined_data[combined_data['SHIP_QTY'] > (combined_data['PS_AVAILQTY']*0.5)]

# Getting supplier names with excess and write to CSV
supplier_excess = combined_data[['S_NAME']].drop_duplicates()
supplier_excess.to_csv('query_output.csv', index=False)

# Close all connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
redis_conn.close()
