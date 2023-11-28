import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# MySQL query
mysql_query = """
SELECT l.L_ORDERKEY, l.L_PARTKEY, l.L_SUPPKEY, l.L_EXTENDEDPRICE, l.L_DISCOUNT, l.L_QUANTITY, p.PS_SUPPLYCOST
FROM lineitem l JOIN partsupp p ON l.L_PARTKEY = p.PS_PARTKEY AND l.L_SUPPKEY = p.PS_SUPPKEY;
"""
mysql_cursor.execute(mysql_query)
lineitem_partsupp = pd.DataFrame(mysql_cursor.fetchall(), columns=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_QUANTITY', 'PS_SUPPLYCOST'])

mysql_conn.close()

# MongoDB connection and query
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
part_collection = mongo_db["part"]

# MongoDB query
part_docs = part_collection.find({"P_NAME": {"$regex": "dim"}}, {"_id": 0})
parts = pd.DataFrame(list(part_docs))

# Redis connection and query
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Redis queries
nation_df = pd.read_json(redis_client.get('nation'))
supplier_df = pd.read_json(redis_client.get('supplier'))
orders_df = pd.read_json(redis_client.get('orders'))

# Merging datasets
merged_df = lineitem_partsupp.merge(parts, left_on='L_PARTKEY', right_on='P_PARTKEY')\
                             .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')\
                             .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')\
                             .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculating profit
merged_df['EXTENDEDPRICE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
merged_df['PROFIT'] = merged_df['EXTENDEDPRICE'] - (merged_df['L_QUANTITY'] * merged_df['PS_SUPPLYCOST'])
merged_df['YEAR'] = pd.to_datetime(merged_df['O_ORDERDATE']).dt.year

# Grouping and sorting
result_df = merged_df.groupby(['N_NAME', 'YEAR'], as_index=False)['PROFIT'].sum()
result_df.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False], inplace=True)

# Writing to CSV
result_df.to_csv('query_output.csv', index=False)
