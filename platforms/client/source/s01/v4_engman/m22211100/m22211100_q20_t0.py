# requirements.txt generation
# pymysql~=1.0.2
# pymongo~=3.12.0
# pandas~=1.4.2
# direct_redis~=0.2.0

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    database='tpch'
)
lineitem_df = pd.read_sql('''
    SELECT L_PARTKEY, L_SUPPKEY
    FROM lineitem
    WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01';
''', con=mysql_conn)
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_df = pd.DataFrame(list(mongo_db.supplier.find({}, {'_id': 0})))
# Join lineitem with supplier to get only those suppliers that are from CANADA
supplier_df = supplier_df[supplier_df['S_NATIONKEY'] == 3] # Assuming CANADA's N_NATIONKEY is 3
partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find({}, {'_id': 0})))
mongo_client.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_json(redis_conn.get('nation'))
part_df = pd.read_json(redis_conn.get('part'))

# Filter parts that share a certain naming convention (e.g., name starts with 'forest')
part_df = part_df[part_df['P_NAME'].str.startswith('forest')]

# Merging information from different sources
# Assuming supplier and partsupp should be joined on S_SUPPKEY = PS_SUPPKEY
merged_part_supplier = pd.merge(part_df, partsupp_df, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')
merged_part_supplier_lineitem = pd.merge(merged_part_supplier, lineitem_df, how='inner', left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])
# Assuming we need to have an excess of more than 50% for the supplier
suppliers_with_excess = merged_part_supplier_lineitem.groupby('PS_SUPPKEY').filter(lambda x: len(x) > len(merged_part_supplier_lineitem) * 0.5)

# The final result expected from the query
result = suppliers_with_excess[['PS_SUPPKEY']].drop_duplicates()

# Write results to CSV
result.to_csv('query_output.csv', index=False)
