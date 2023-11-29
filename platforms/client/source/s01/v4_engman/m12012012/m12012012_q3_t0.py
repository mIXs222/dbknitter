# multi_db_query.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL to get customer details
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')
try:
    with mysql_connection.cursor() as cursor:
        cursor.execute("""
            SELECT C_CUSTKEY, C_MKTSEGMENT 
            FROM customer 
            WHERE C_MKTSEGMENT = 'BUILDING';
        """)
        customers = cursor.fetchall()
finally:
    mysql_connection.close()

customer_df = pd.DataFrame(customers, columns=['C_CUSTKEY', 'C_MKTSEGMENT'])

# Connect to MongoDB to get orders details
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client['tpch']
orders_collection = mongodb['orders']

query1 = {
    'O_ORDERDATE': {'$lt': '1995-03-05'}
}
orders = list(orders_collection.find(query1))
orders_df = pd.DataFrame(orders)

# Connect to Redis to get lineitem details
redis_client = DirectRedis(host='redis', port=6379, db=0)
lineitem_df = pd.DataFrame(redis_client.get('lineitem'))

# Close the mongo connection
mongo_client.close()

# Merge dataframes based on customer and orders
merged_df = orders_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
shipping_priority_df = merged_df[merged_df['L_SHIPDATE'] > '1995-03-15']

# Calculate revenue
shipping_priority_df['REVENUE'] = shipping_priority_df['L_EXTENDEDPRICE'] * (1 - shipping_priority_df['L_DISCOUNT'])

# Filter columns and sort by revenue
output_df = shipping_priority_df[['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]
output_df = output_df.sort_values(by='REVENUE', ascending=False)

# Write to CSV
output_df.to_csv('query_output.csv', index=False)
