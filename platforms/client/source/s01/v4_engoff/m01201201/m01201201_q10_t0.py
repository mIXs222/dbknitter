import pandas as pd
import pymysql
import pymongo
import direct_redis
from datetime import datetime

# Establish connection to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Establish connection to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Establish connection to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch relevant data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation")
    nations = cursor.fetchall()
    nations_df = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME'])
    
    cursor.execute("SELECT O_ORDERKEY, O_CUSTKEY FROM orders "
                   "WHERE O_ORDERDATE >= '1993-10-01' AND O_ORDERDATE < '1994-01-01'")
    orders = cursor.fetchall()
    orders_df = pd.DataFrame(orders, columns=['O_ORDERKEY', 'O_CUSTKEY'])

# Fetch relevant data from MongoDB
lineitem_docs = mongo_db.lineitem.find({
    "L_RETURNFLAG": "R",
    "L_SHIPDATE": {"$gte": datetime(1993, 10, 1), "$lt": datetime(1994, 1, 1)}
})
lineitem_df = pd.DataFrame(list(lineitem_docs))

# Fetch relevant data from Redis
customers_json = redis_conn.get('customer')
customers_df = pd.read_json(customers_json, lines=True)

# Join data
merged_df = (orders_df.merge(customers_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
             .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
             .merge(nations_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY'))

# Calculate lost revenue
merged_df['LOST_REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Summing lost revenue and perform the final selection
result_df = (merged_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT'])
                      .agg({'LOST_REVENUE': 'sum'})
                      .reset_index()
                      .sort_values(by=['LOST_REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, True]))

# Write the output to a CSV file
result_df.to_csv('query_output.csv', index=False)
