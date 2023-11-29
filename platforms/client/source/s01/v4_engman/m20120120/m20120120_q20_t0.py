import pymysql
import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Fetch Partsupp and Lineitem from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM partsupp")
    partsupp_df = pd.DataFrame(cursor.fetchall(), columns=[
                               'PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])
    cursor.execute("SELECT * FROM lineitem WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01'")
    lineitem_df = pd.DataFrame(cursor.fetchall(), columns=[
                               'L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Fetch Part from MongoDB
part_collection = mongo_db['part']
part_df = pd.DataFrame(list(part_collection.find({"P_NAME": {"$regex": "^forest"}})))

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch Nation and Supplier from Redis
nation_df = pd.read_json(redis_conn.get('nation'), typ='frame')
supplier_df = pd.read_json(redis_conn.get('supplier'), typ='frame')

# Close MySQL connection
mysql_conn.close()

# Relevant data processing to get potential part promotion
suppliers_in_canada = supplier_df[supplier_df['S_NATIONKEY'] == nation_df[nation_df['N_NAME'] == 'CANADA'].iloc[0]['N_NATIONKEY']]
shipped_parts = lineitem_df.merge(partsupp_df, how='inner', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
shipped_parts = shipped_parts.merge(part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
shipped_parts_suppliers = shipped_parts.groupby('L_SUPPKEY').agg({'L_QUANTITY': 'sum'}).reset_index()
shipped_parts_suppliers = shipped_parts_suppliers[shipped_parts_suppliers['L_QUANTITY'] > shipped_parts['L_QUANTITY'].sum() / 2]

result = suppliers_in_canada.merge(shipped_parts_suppliers, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
result = result[['S_SUPPKEY', 'S_NAME', 'S_ACCTBAL', 'S_COMMENT', 'L_QUANTITY']]

# Write the result to query_output.csv
result.to_csv('query_output.csv', index=False)
