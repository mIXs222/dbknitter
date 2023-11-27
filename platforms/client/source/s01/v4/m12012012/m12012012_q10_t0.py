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
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# MySQL query
mysql_query = """
SELECT
    C_CUSTKEY,
    C_NAME,
    C_ACCTBAL,
    C_ADDRESS,
    C_PHONE,
    C_COMMENT
FROM
    customer
"""
customers = pd.read_sql(mysql_query, mysql_conn)

# MongoDB queries
nation_collection = mongodb_db['nation']
nation = pd.DataFrame(list(nation_collection.find()))

orders_collection = mongodb_db['orders']
orders = pd.DataFrame(list(orders_collection.find({
    'O_ORDERDATE': {'$gte': datetime(1993, 10, 1), '$lt': datetime(1994, 1, 1)}
})))

# Redis query
lineitems_data = redis_conn.get('lineitem')
lineitems = pd.read_json(lineitems_data, orient='split')

# Converting keys to integers for proper merging
orders['O_CUSTKEY'] = orders['O_CUSTKEY'].astype(int)
lineitems['L_ORDERKEY'] = lineitems['L_ORDERKEY'].astype(int)

# Filtering lineitems with L_RETURNFLAG = 'R'
lineitems = lineitems[lineitems['L_RETURNFLAG'] == 'R']

# Merging DataFrames
result = customers.merge(orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result = result.merge(lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
result = result.merge(nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Calculating revenue
result['REVENUE'] = result.apply(lambda r: r['L_EXTENDEDPRICE'] * (1 - r['L_DISCOUNT']), axis=1)

# Selecting needed columns and renaming as per SQL query column aliases
result = result[['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]

# Group by and sort as per the query
result_grouped = result.groupby([
    'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT'
], as_index=False).agg({'REVENUE': 'sum'})

result_sorted = result_grouped.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False])

# Writing to query_output.csv
result_sorted.to_csv('query_output.csv', index=False)

# Close database connections
mysql_conn.close()
mongodb_client.close()
redis_conn.close()
