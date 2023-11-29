import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['customer']

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL to fetch lineitems within the date range and calculate revenue lost
mysql_cursor.execute("SELECT L_ORDERKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue_lost "
                     "FROM lineitem "
                     "WHERE L_RETURNFLAG = 'R' AND L_SHIPDATE BETWEEN '1993-10-01' AND '1994-01-01' "
                     "GROUP BY L_ORDERKEY")
lineitems_with_revenue = mysql_cursor.fetchall()

# Fetch customers from MongoDB and create DataFrame
customers_mongo_df = pd.DataFrame(list(mongo_collection.find({}, {'_id': 0})))

# Fetch nations from Redis and create DataFrame
nations_data = redis_conn.get('nation')
nations_df = pd.read_csv(pd.compat.StringIO(nations_data.decode('utf-8')))

# Convert lineitems_with_revenue to DataFrame
lineitems_df = pd.DataFrame(lineitems_with_revenue, columns=['O_ORDERKEY', 'revenue_lost'])

# Execute the Join operations
result_df = (lineitems_df
             .merge(customers_mongo_df, left_on='O_ORDERKEY', right_on='C_CUSTKEY', how='inner')
             .merge(nations_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='left'))

# Select and sort
result_df = result_df[['C_CUSTKEY', 'C_NAME', 'revenue_lost', 'C_ACCTBAL',
                       'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']] \
    .sort_values(by=['revenue_lost', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False]) \
    .rename(columns={'N_NAME': 'nation'})

# Write to CSV
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
redis_conn.close()
