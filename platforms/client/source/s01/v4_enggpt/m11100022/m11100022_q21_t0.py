import pymysql
import pymongo
import pandas as pd
import csv
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL to get suppliers from Saudi Arabia
with mysql_conn.cursor() as cursor:
    cursor.execute("""
    SELECT s.S_SUPPKEY, s.S_NAME
    FROM supplier s
    INNER JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
    WHERE n.N_NAME = 'SAUDI ARABIA'
    """)
    suppliers_sa = cursor.fetchall()
    supplier_df = pd.DataFrame(suppliers_sa, columns=['S_SUPPKEY', 'S_NAME'])

# Query MongoDB to get nations
nations_col = mongodb["nation"]
nations_df = pd.DataFrame(list(nations_col.find()))

# Load orders and lineitem from Redis with DirectRedis
orders_df = pd.read_json(redis_conn.get('orders'), orient='records')
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Data processing and join operations to create the result
orders_df = orders_df[orders_df['O_ORDERSTATUS'] == 'F']

lineitem_with_receipt_after_commit = lineitem_df[lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']]
lineitem_grouped = lineitem_with_receipt_after_commit.groupby('L_SUPPKEY')['L_ORDERKEY'].count().reset_index()
lineitem_grouped.columns = ['S_SUPPKEY', 'NUMWAIT']

result_df = pd.merge(supplier_df, lineitem_grouped, on='S_SUPPKEY', how='inner')
result_df = result_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close connections
mysql_conn.close()
mongo_client.close()
redis_conn.close()
