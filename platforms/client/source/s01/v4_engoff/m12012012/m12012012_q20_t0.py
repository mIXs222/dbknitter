import pymysql
import pymongo
import pandas as pd
import json
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

mysql_query = """
SELECT P_PARTKEY, P_NAME
FROM part
WHERE P_NAME LIKE '%%forest%%';
"""

mysql_cursor.execute(mysql_query)
part_data = mysql_cursor.fetchall()
df_part = pd.DataFrame(part_data, columns=['P_PARTKEY', 'P_NAME'])

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# MongoDB connection and query
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

nation_collection = mongodb_db['nation']
supplier_collection = mongodb_db['supplier']

nations = list(nation_collection.find({'N_NAME': 'CANADA'}))
nation_keys = [nation['N_NATIONKEY'] for nation in nations]

suppliers = list(supplier_collection.find({'S_NATIONKEY': {'$in': nation_keys}}, {'S_SUPPKEY': 1, '_id': 0}))
supplier_keys = [supplier['S_SUPPKEY'] for supplier in suppliers]

mongodb_client.close()

# Redis connection and query
redis_conn = DirectRedis(host='redis', port=6379, db=0)

partsupp_data = json.loads(redis_conn.get('partsupp'))
df_partsupp = pd.DataFrame(partsupp_data)

lineitem_data = json.loads(redis_conn.get('lineitem'))
df_lineitem = pd.DataFrame(lineitem_data)

# Close Redis connection
redis_conn.close()

# Filtering Redis data
df_lineitem['L_SHIPDATE'] = pd.to_datetime(df_lineitem['L_SHIPDATE'])
df_filtered_lineitem = df_lineitem[
    (df_lineitem['L_SHIPDATE'] >= pd.Timestamp('1994-01-01')) &
    (df_lineitem['L_SHIPDATE'] < pd.Timestamp('1995-01-01')) &
    (df_lineitem['L_SUPPKEY'].isin(supplier_keys))
]

partsupp_suppliers = df_partsupp[df_partsupp['PS_SUPPKEY'].isin(df_filtered_lineitem['L_SUPPKEY'])]
partsupp_groups = partsupp_suppliers.groupby('PS_PARTKEY').agg({'PS_AVAILQTY': 'sum'}).reset_index()

# Merge data
df_merged = pd.merge(df_part, partsupp_groups, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')
df_final = df_merged[df_merged['PS_AVAILQTY'] > df_merged['PS_AVAILQTY'].mean() * 1.5]

# Save output to csv
df_final.to_csv('query_output.csv', index=False)
