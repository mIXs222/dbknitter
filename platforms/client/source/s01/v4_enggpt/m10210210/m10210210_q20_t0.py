# Python code to execute the complex query across different data platforms
import pandas as pd
import pymysql
from pymongo import MongoClient
import direct_redis

# Connect to MySQL
mysql_con = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cur = mysql_con.cursor()

# Connect to MongoDB
mongo_client = MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
nation_collection = mongo_db['nation']
supplier_collection = mongo_db['supplier']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get data from MySQL
mysql_cur.execute("SELECT PS_PARTKEY, PS_SUPPKEY FROM partsupp WHERE PS_AVAILQTY > (SELECT 0.5 * SUM(L_QUANTITY) FROM lineitem WHERE L_PARTKEY = PS_PARTKEY AND L_SUPPKEY = PS_SUPPKEY AND L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01')")
ps_result = mysql_cur.fetchall()
ps_df = pd.DataFrame(ps_result, columns=['PS_PARTKEY', 'PS_SUPPKEY'])

# Get part names from Redis
part_df = pd.read_json(redis_client.get('part'), orient='records')
forest_parts_df = part_df[part_df['P_NAME'].str.startswith('forest')]

# Combine to get relevant supplier keys
relevant_ps_df = ps_df[ps_df['PS_PARTKEY'].isin(forest_parts_df['P_PARTKEY'])]

# Get supplier information from MongoDB
suppliers_cursor = supplier_collection.find({'S_SUPPKEY': {'$in': relevant_ps_df['PS_SUPPKEY'].tolist()}})
suppliers_df = pd.DataFrame(list(suppliers_cursor))

# Get nation information from MongoDB
canada_nation_cursor = nation_collection.find({'N_NAME': 'CANADA'})
canada_nation_df = pd.DataFrame(list(canada_nation_cursor))

# Combine to get relevant suppliers in Canada
canada_suppliers_df = suppliers_df[suppliers_df['S_NATIONKEY'].isin(canada_nation_df['N_NATIONKEY'])]
result_df = canada_suppliers_df[['S_NAME', 'S_ADDRESS']].sort_values(by='S_NAME').reset_index(drop=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_cur.close()
mysql_con.close()
mongo_client.close()
