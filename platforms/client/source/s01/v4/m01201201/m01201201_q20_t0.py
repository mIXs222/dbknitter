import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query execution
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY FROM supplier WHERE S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'CANADA')")
mysql_suppliers = mysql_cursor.fetchall()
mysql_supp_df = pd.DataFrame(mysql_suppliers, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY'])
mysql_conn.close()

# MongoDB connection and query execution
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
partsupp_collection = mongo_db['partsupp']
lineitem_collection = mongo_db['lineitem']
partsupp_df = pd.DataFrame(list(partsupp_collection.find()))
lineitem_aggregate = list(lineitem_collection.aggregate([
    {"$match": {"L_SHIPDATE": {"$gte": "1994-01-01", "$lt": "1995-01-01"}}},
    {"$group": {"_id": {"L_PARTKEY": "$L_PARTKEY", "L_SUPPKEY": "$L_SUPPKEY"}, "SUM_QUANTITY": {"$sum": "$L_QUANTITY"}}}
]))
lineitem_df = pd.DataFrame(lineitem_aggregate)
lineitem_df['PS_PARTKEY'] = lineitem_df['_id'].apply(lambda x: x['L_PARTKEY'])
lineitem_df['PS_SUPPKEY'] = lineitem_df['_id'].apply(lambda x: x['L_SUPPKEY'])
lineitem_df.drop(columns=['_id'], inplace=True)

# Redis connection and query execution
redis_conn = DirectRedis(host='redis', db=0, port=6379)
part_keys_str = redis_conn.get('part')
part_keys_df = pd.read_json(part_keys_str)
filtered_parts_df = part_keys_df[part_keys_df['P_NAME'].str.startswith('forest')]

# Join and filter data
filtered_partsupp_df = partsupp_df[partsupp_df['PS_PARTKEY'].isin(filtered_parts_df['P_PARTKEY'])]
cond_df = pd.merge(filtered_partsupp_df, lineitem_df, how='left', on=['PS_PARTKEY', 'PS_SUPPKEY'])
final_partsupp_df = cond_df[cond_df['PS_AVAILQTY'] > (0.5 * cond_df['SUM_QUANTITY'])]
final_supplier_df = mysql_supp_df[mysql_supp_df['S_SUPPKEY'].isin(final_partsupp_df['PS_SUPPKEY'])]

# Export the final result to CSV
final_supplier_df[['S_NAME', 'S_ADDRESS']].sort_values(by='S_NAME').to_csv('query_output.csv', index=False)
