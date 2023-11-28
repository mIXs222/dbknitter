import pymysql
import pymongo
import pandas as pd
from bson.objectid import ObjectId
from datetime import datetime
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_customers = mongo_db['customer']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for nations
mysql_cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation")
nations = pd.DataFrame(mysql_cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME'])

# Fetch customer data from MongoDB
customers = pd.DataFrame(list(mongo_customers.find({})),
                         columns=['_id', 'C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT'])

# Fetch orders and lineitem data from Redis
orders_df = pd.read_msgpack(redis_client.get('orders'))
lineitem_df = pd.read_msgpack(redis_client.get('lineitem'))

# Closing the MySQL cursor and connection
mysql_cursor.close()
mysql_conn.close()

# Filter orders by date
start_date = datetime(1993, 10, 1)
end_date = datetime(1993, 12, 31)
orders_filtered = orders_df[(orders_df['O_ORDERDATE'] >= start_date) & (orders_df['O_ORDERDATE'] <= end_date)]

# Filter lineitem by return flag 'R'
lineitem_filtered = lineitem_df[lineitem_df['L_RETURNFLAG'] == 'R']

# Merge the dataframes to combine the information
merged_df = pd.merge(customers, orders_filtered, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, lineitem_filtered, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = pd.merge(merged_df, nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Calculate revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Group by customer properties and calculate total revenue
result = merged_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT']).agg(TOTAL_REVENUE=('REVENUE', 'sum')).reset_index()

# Sort the result as per instructions
result.sort_values(by=['TOTAL_REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False], inplace=True)

# Output to CSV file
result.to_csv('query_output.csv', index=False)
