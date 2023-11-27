import pymysql
import pymongo
import pandas as pd
from sqlalchemy import create_engine
from direct_redis import DirectRedis

# Connection information
mysql_conn_info = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "database": "tpch",
}

mongodb_conn_info = {
    "hostname": "mongodb",
    "port": 27017,
    "database": "tpch",
}

redis_conn_info = {
    "hostname": "redis",
    "port": 6379,
    "database": "0",
}

# Establish connections
# MySQL
mysql_conn = pymysql.connect(**mysql_conn_info)

# MongoDB
mongo_client = pymongo.MongoClient(mongodb_conn_info["hostname"], mongodb_conn_info["port"])
mongo_db = mongo_client[mongodb_conn_info["database"]]

# Redis
redis_conn = DirectRedis(host=redis_conn_info["hostname"], port=redis_conn_info["port"], db=redis_conn_info["database"])

# Load the data from MySQL
nation_query = "SELECT N_NATIONKEY, N_NAME FROM nation"
df_nation = pd.read_sql(nation_query, mysql_conn)

mysql_conn.close()

# Load customer data from MongoDB
customers = mongo_db["customer"]
df_customer = pd.DataFrame(list(customers.find({}, {'_id': 0})))

# Convert the Redis data to dataframes
df_orders = pd.read_msgpack(redis_conn.get('orders'))
df_lineitem = pd.read_msgpack(redis_conn.get('lineitem'))

# Merge dataframes
df_merged = pd.merge(df_customer, df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df_merged = pd.merge(df_merged, df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df_merged = pd.merge(df_merged, df_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
df_filtered = df_merged.query("O_ORDERDATE >= '1993-10-01' and O_ORDERDATE < '1994-01-01' and L_RETURNFLAG == 'R'")

# Perform the groupby and aggregation
df_grouped = df_filtered.groupby(
    ['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT'], as_index=False
).agg(
    {"REVENUE": lambda x: sum(x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']))}
).sort_values(
    ["REVENUE", "C_CUSTKEY", "C_NAME", "C_ACCTBAL"], ascending=[False, True, True, False]
)

# Output to CSV
df_grouped.to_csv("query_output.csv", index=False)
