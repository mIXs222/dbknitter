import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
from pandas import DataFrame

# Connection to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connection to MongoDB database
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client["tpch"]
nation_collection = mongodb_db["nation"]

# Connection to Redis database
redis_client = DirectRedis(host='redis', port=6379)

# Fetch nation data from MongoDB
nation_data = list(nation_collection.find({}, {'_id': False}))

# Fetch customer data from MySQL db
mysql_customer_query = """
SELECT C_CUSTKEY, C_NAME, C_ACCTBAL, C_PHONE, C_ADDRESS, C_COMMENT, C_NATIONKEY
FROM customer
"""
mysql_cursor.execute(mysql_customer_query)
customer_data = mysql_cursor.fetchall()
customer_df = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'C_ADDRESS', 'C_COMMENT', 'C_NATIONKEY'])

# Fetch orders and lineitem data from Redis database
orders_df = DataFrame(eval(redis_client.get('orders')))
lineitem_df = DataFrame(eval(redis_client.get('lineitem')))

# Filter the orders and lineitem data
start_date = datetime(1993, 10, 1)
end_date = datetime(1994, 1, 1)

orders_df = orders_df[(orders_df['O_ORDERDATE'] >= start_date.strftime('%Y-%m-%d')) & 
                      (orders_df['O_ORDERDATE'] < end_date.strftime('%Y-%m-%d'))]

lineitem_df = lineitem_df[lineitem_df['L_RETURNFLAG'] == 'R']

# Merge dataframes
merged_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = pd.merge(merged_df, pd.DataFrame(nation_data), left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Perform the aggregation
grouped_df = merged_df.groupby(
    by=["C_CUSTKEY", "C_NAME", "C_ACCTBAL", "C_PHONE", "N_NAME", "C_ADDRESS", "C_COMMENT"]
).apply(
    lambda x: pd.Series({
        "REVENUE": (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum()
    })
).reset_index()

# Order the results
ordered_df = grouped_df.sort_values(by=["REVENUE", "C_CUSTKEY", "C_NAME", "C_ACCTBAL"], ascending=[False, True, True, False])

# Write to CSV
ordered_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongodb_client.close()
