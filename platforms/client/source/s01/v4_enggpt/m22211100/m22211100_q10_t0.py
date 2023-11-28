import pymysql
import pymongo
import pandas as pd
import direct_redis

# Connection setup
mysql_conn = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    db="tpch",
    charset="utf8"
)

mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongodb_client["tpch"]

redis_client = direct_redis.DirectRedis(host="redis", port=6379, db=0)

# Mysql queries
mysql_query_orders = """
SELECT O_CUSTKEY, O_ORDERKEY
FROM orders
WHERE O_ORDERDATE BETWEEN '1993-10-01' AND '1993-12-31'
"""

mysql_query_lineitem = """
SELECT L_ORDERKEY,
       SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM lineitem
WHERE L_RETURNFLAG = 'R'
GROUP BY L_ORDERKEY
"""

# Execute MySQL queries
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query_orders)
    orders = cursor.fetchall()

with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query_lineitem)
    lineitem = cursor.fetchall()

# Close MySQL connection
mysql_conn.close()

# Convert query results to pandas DataFrames
df_orders = pd.DataFrame(orders, columns=['C_CUSTKEY', 'O_ORDERKEY'])
df_lineitem = pd.DataFrame(lineitem, columns=['O_ORDERKEY', 'REVENUE'])

# Merge on order key
merged_df = pd.merge(df_orders, df_lineitem, on='O_ORDERKEY')

# Query MongoDB
mongo_customers = mongo_db.customer.find(
    {}, {"_id": 0, "C_CUSTKEY": 1, "C_NAME": 1, "C_ACCTBAL": 1, 
         "C_ADDRESS": 1, "C_PHONE": 1, "C_COMMENT": 1}
)
df_customers = pd.DataFrame(list(mongo_customers))

# Merge with customer data
merged_df = pd.merge(merged_df, df_customers, on='C_CUSTKEY')

# Extract data from Redis
nation_data = redis_client.get('nation')
df_nations = pd.read_json(nation_data)

# Merge with nation data
merged_df = pd.merge(merged_df, df_nations, left_on='C_CUSTKEY', right_on='N_NATIONKEY')

# Select relevant columns
result = merged_df[[
    'C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL',
    'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'
]]

# Sort the results
sorted_result = result.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME'], 
                                   ascending=[True, True, True]).sort_values(
                                   by=['C_ACCTBAL'], ascending=False)

# Write to CSV
sorted_result.to_csv("query_output.csv", index=False)
