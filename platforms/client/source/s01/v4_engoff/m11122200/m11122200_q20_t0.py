import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MySQL
query_lineitem = """
SELECT L_PARTKEY, L_SUPPKEY
FROM lineitem
WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
"""
df_lineitem = pd.read_sql(query_lineitem, mysql_conn)

# Fetch data from MongoDB
nation_docs = mongo_db['nation'].find({'N_NAME': 'CANADA'})
nation_keys = [doc['N_NATIONKEY'] for doc in nation_docs]

supplier_docs = redis_client.get('supplier')
df_supplier = pd.read_msgpack(supplier_docs)
df_supplier = df_supplier[df_supplier['S_NATIONKEY'].isin(nation_keys)]

part_docs = mongo_db['part'].find()
df_part = pd.DataFrame(list(part_docs))
forest_parts = df_part[df_part['P_NAME'].str.contains('forest', case=False)]

# Identify suppliers who have an excess of forest parts
excess_suppliers = df_lineitem[df_lineitem['L_PARTKEY'].isin(forest_parts['P_PARTKEY'])]
supplier_counts = excess_suppliers.groupby('L_SUPPKEY').size()
excess_suppliers = supplier_counts[supplier_counts > supplier_counts.median() * 0.5].index

# Retrieve supplier info
final_suppliers = df_supplier[df_supplier['S_SUPPKEY'].isin(excess_suppliers)]

# Output to CSV
final_suppliers.to_csv('query_output.csv', index=False)

# Close all connections
mysql_conn.close()
mongo_client.close()
