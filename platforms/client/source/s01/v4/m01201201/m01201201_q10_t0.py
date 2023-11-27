import pymysql
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis


# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# MongoDB Connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem_col = mongodb_db['lineitem']

# Redis Connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# MySQL Queries
customer_sql = """
SELECT C_CUSTKEY, C_NAME, C_ACCTBAL, C_PHONE, C_ADDRESS, C_COMMENT, C_NATIONKEY
FROM customer
"""
orders_sql = """
SELECT O_CUSTKEY, O_ORDERKEY
FROM orders
WHERE O_ORDERDATE >= '1993-10-01' AND O_ORDERDATE < '1994-01-01'
"""
nation_sql = "SELECT N_NATIONKEY, N_NAME FROM nation"

# Execute MySQL Queries
mysql_cursor.execute(customer_sql)
customers = pd.DataFrame(mysql_cursor.fetchall(), columns=['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'C_ADDRESS', 'C_COMMENT', 'C_NATIONKEY'])

mysql_cursor.execute(orders_sql)
orders = pd.DataFrame(mysql_cursor.fetchall(), columns=['O_CUSTKEY', 'O_ORDERKEY'])

mysql_cursor.execute(nation_sql)
nations = pd.DataFrame(mysql_cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME'])

# Fetch data from MongoDB
lineitem_cursor = lineitem_col.find({
    "L_RETURNFLAG": "R",
    "L_SHIPDATE": {
        "$gte": datetime(1993, 10, 1),
        "$lt": datetime(1994, 1, 1)
    }
})
lineitems = pd.DataFrame(list(lineitem_cursor))

# Fetch data from Redis
df_customer = pd.read_pickle(redis_client.get('customer'))

# Join data from MySQL, MongoDB, and Redis
combined_df = (
    customers
    .merge(orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
)

# Calculate aggregate columns and filter by required columns
result_df = combined_df.groupby(
    ['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT'],
    as_index=False
).agg(
    REVENUE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc=lambda x: (x * (1 - combined_df.loc[x.index, 'L_DISCOUNT'])).sum())
)

result_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False], inplace=True)

# Write results to CSV
result_df.to_csv('query_output.csv', index=False)

# Close all database connections
mysql_conn.close()
mongodb_client.close()
redis_client.close()
