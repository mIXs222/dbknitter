import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()
# Execute MySQL query to get orders with details
mysql_cursor.execute("SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_TOTALPRICE FROM orders")
orders_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
# Query MongoDB for lineitem information
lineitem_docs = mongodb['lineitem'].aggregate([
    {"$group": {
        "_id": "$L_ORDERKEY",
        "total_quantity": {"$sum": "$L_QUANTITY"}
    }},
    {"$match": {"total_quantity": {"$gt": 300}}}
])
lineitem_df = pd.DataFrame(list(lineitem_docs))

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)
# Read customer data from Redis stored as a DataFrame
customer_df = pd.read_json(redis.get('customer'))

# Dataframe merging and calculations to replicate the SQL join, where, and group by clauses
result_df = orders_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
result_df = result_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='_id')
result_df = result_df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).agg({'total_quantity': 'sum'}).reset_index()
result_df = result_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Output the data to CSV
result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
