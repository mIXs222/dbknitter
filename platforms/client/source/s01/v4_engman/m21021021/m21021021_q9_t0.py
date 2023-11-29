import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

part_query = """
SELECT P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE
FROM part
WHERE P_NAME LIKE '%%dim%%';
"""
parts = pd.read_sql(part_query, mysql_conn)
mysql_conn.close()

# MongoDB connection and queries
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

partsupp = pd.DataFrame(list(mongo_db.partsupp.find()))
lineitem = pd.DataFrame(list(mongo_db.lineitem.find()))

# Redis connection and query
redis_db = 0
redis_client = DirectRedis(host='redis', port=6379, db=redis_db)

nation = pd.read_json(redis_client.get('nation'), orient='records')
supplier = pd.read_json(redis_client.get('supplier'), orient='records')
orders = pd.read_json(redis_client.get('orders'), orient='records')

# Joining and filtering the data
merged_data = (
    lineitem.merge(partsupp, how='left', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
    .merge(parts, how='left', left_on='L_PARTKEY', right_on='P_PARTKEY')
    .merge(supplier, how='left', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(nation, how='left', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(orders, how='left', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
)

filtered_data = merged_data[merged_data['P_NAME'].str.contains('dim')]

# Calculating profit
filtered_data['PROFIT'] = (
    (filtered_data['L_EXTENDEDPRICE'] * (1 - filtered_data['L_DISCOUNT'])) -
    (filtered_data['PS_SUPPLYCOST'] * filtered_data['L_QUANTITY'])
)

# Extracting year from date
filtered_data['YEAR'] = pd.to_datetime(filtered_data['O_ORDERDATE']).dt.year

# Grouping by nation and year
grouped_data = filtered_data.groupby(['N_NAME', 'YEAR']).agg({'PROFIT': 'sum'}).reset_index()

# Sorting the result
result = grouped_data.sort_values(['N_NAME', 'YEAR'], ascending=[True, False])

# Writing the results to a csv file
result.to_csv('query_output.csv', index=False)
