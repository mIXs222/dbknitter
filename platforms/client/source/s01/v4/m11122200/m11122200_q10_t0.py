import pandas as pd
import pymysql
import pymongo
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4'
)

# Read orders and lineitem tables using Pandas
query_mysql = """
SELECT
    O_CUSTKEY,
    L_ORDERKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    O_ORDERDATE,
    L_RETURNFLAG
FROM
    orders,
    lineitem
WHERE
    L_ORDERKEY = O_ORDERKEY
    AND O_ORDERDATE >= '1993-10-01'
    AND O_ORDERDATE < '1994-01-01'
    AND L_RETURNFLAG = 'R'
"""
orders_lineitem_df = pd.read_sql(query_mysql, mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
nation_collection = mongo_db['nation']

# Retrieve all documents from nation collection
nations_df = pd.DataFrame(list(nation_collection.find()))

# Close MongoDB connection
mongo_client.close()

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read customer data from Redis
customer_df = pd.read_json(redis_client.get("customer"))

# Merge customers with orders & lineitems on C_CUSTKEY and O_CUSTKEY
merged_df = pd.merge(customer_df, orders_lineitem_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Merge the current merged dataframe with nations on C_NATIONKEY and N_NATIONKEY
final_df = pd.merge(merged_df, nations_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Group by required fields and calculate REVENUE
result_df = final_df.groupby(
    ['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT']
).apply(lambda x: sum(x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']))).reset_index(name='REVENUE')

# Sorting the result as required
sorted_result_df = result_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False])

# Write to CSV
sorted_result_df.to_csv('query_output.csv', index=False)
