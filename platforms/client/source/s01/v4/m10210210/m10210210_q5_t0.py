import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
my_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
# Execute the MySQL query
my_query = """
SELECT region.R_REGIONKEY, nation.N_NATIONKEY, supplier.S_SUPPKEY, lineitem.*
FROM lineitem
INNER JOIN supplier ON lineitem.L_SUPPKEY = supplier.S_SUPPKEY
INNER JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY
INNER JOIN region ON nation.N_REGIONKEY = region.R_REGIONKEY
WHERE region.R_NAME = 'ASIA'
"""
lineitem_df = pd.read_sql(my_query, my_conn)
my_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
nation = pd.DataFrame(list(mongo_db.nation.find()))
supplier = pd.DataFrame(list(mongo_db.supplier.find()))
orders = pd.DataFrame(list(mongo_db.orders.find({"O_ORDERDATE": {"$gte": "1990-01-01", "$lt": "1995-01-01"}})))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
customer_df = pd.read_msgpack(redis_client.get('customer'))

# Merge dataframes to simulate the join operation
merged_df = (
    customer_df.merge(orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(nation, left_on=['C_NATIONKEY', 'S_NATIONKEY'], right_on=['N_NATIONKEY', 'N_NATIONKEY'])
)

# Perform the calculation from the SQL query
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
result = merged_df.groupby('N_NAME')['REVENUE'].sum().reset_index()
result_sorted = result.sort_values('REVENUE', ascending=False)

# Write output to CSV
result_sorted.to_csv('query_output.csv', index=False)
