import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
import datetime

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_connection.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
mongo_customer = mongo_db["customer"]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch lineitem data from MySQL
lineitem_query = """
SELECT 
    L_ORDERKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    (L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as REVENUE,
    L_RETURNFLAG
FROM
    lineitem
WHERE
    L_RETURNFLAG = 'R'
"""
mysql_cursor.execute(lineitem_query)
lineitems = mysql_cursor.fetchall()
lineitem_df = pd.DataFrame(lineitems, columns=['L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'REVENUE', 'L_RETURNFLAG'])

# Fetch order data from Redis
orders_df = pd.read_msgpack(redis_client.get('orders'))

# Fetch customer data from MongoDB
customers = list(mongo_customer.find({}, {'_id': 0}))
customer_df = pd.DataFrame(customers)

# Fetch nation data from Redis
nation_df = pd.read_msgpack(redis_client.get('nation'))

# Filter orders within the desired date range
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
filtered_orders_df = orders_df[
    (orders_df['O_ORDERDATE'] >= datetime.datetime(1993, 10, 1)) &
    (orders_df['O_ORDERDATE'] <= datetime.datetime(1993, 12, 31))
]

# Merge dataframes to match line items with orders
merged_df = pd.merge(lineitem_df, filtered_orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Match customers with their orders
merged_df = pd.merge(merged_df, customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Match customers with their nations
merged_df = pd.merge(merged_df, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Selecting relevant columns and grouping by customer attributes
grouped_df = merged_df.groupby([
    'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'
]).agg({'REVENUE': 'sum'}).reset_index()

# Sorting the result
grouped_df = grouped_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False])

# Write result to a file
grouped_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_cursor.close()
mysql_connection.close()
mongo_client.close()
