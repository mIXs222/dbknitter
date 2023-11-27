import pandas as pd
import pymysql
from pymongo import MongoClient
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongodb_client = MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client['tpch']

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read the data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute(
        "SELECT N_NATIONKEY, N_NAME FROM nation"
    )
    nation = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME'])

# Read the data from MongoDB
orders = pd.DataFrame(list(mongodb_db.orders.find(
    {
        "O_ORDERDATE": {"$gte": "1993-10-01", "$lt": "1994-01-01"}
    },
    {"_id": 0, "O_ORDERKEY": 1, "O_CUSTKEY": 1}
)))

lineitem = pd.DataFrame(list(mongodb_db.lineitem.find(
    {"L_RETURNFLAG": "R"},
    {"_id": 0, "L_ORDERKEY": 1, "L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1}
)))

# Read the data from Redis
customer = pd.read_pickle(redis_conn.get('customer'))

# Perform the join between the dataframes as required by the query
result = (
    customer
    .merge(orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
)

# Calculate the 'REVENUE'
result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Perform grouping and aggregations
output = (
    result.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT'])
    .agg({'REVENUE': 'sum'})
    .reset_index()
    .sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False])
)

# Write the results to a CSV file
output.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongodb_client.close()
redis_conn.close()
