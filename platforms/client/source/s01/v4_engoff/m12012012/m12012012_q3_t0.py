import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(decode_responses=True)

# Query MySQL for customer data with market segment 'BUILDING'
query_mysql = """
SELECT C_CUSTKEY
FROM customer
WHERE C_MKTSEGMENT = 'BUILDING';
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(query_mysql)
    customer_data = cursor.fetchall()
    cust_keys = [row[0] for row in customer_data]

# Query MongoDB for orders data
query_mongo = {
    'O_ORDERDATE': {'$lt': '1995-03-15'},
    'O_CUSTKEY': {'$in': cust_keys}
}
orders_data = mongodb.orders.find(query_mongo, {'_id': 0, 'O_ORDERKEY': 1, 'O_SHIPPRIORITY': 1})
orders_df = pd.DataFrame(list(orders_data))

# Query Redis for lineitem data
lineitem_df = pd.DataFrame()
for order_key in orders_df['O_ORDERKEY']:
    lineitem_data = redis_client.get(f'lineitem:{order_key}')
    if lineitem_data:
        lineitem_df_data = pd.DataFrame([eval(lineitem_data)])
        lineitem_df = lineitem_df.append(lineitem_df_data, ignore_index=True)

# Calculate potential revenue
lineitem_df['POTENTIAL_REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Merge data
result = orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
result_grouped = result.groupby(['O_ORDERKEY', 'O_SHIPPRIORITY']).agg({'POTENTIAL_REVENUE': 'sum'}).reset_index()

# Get orders with the largest revenue that had not been shipped
max_revenue = result_grouped['POTENTIAL_REVENUE'].max()
largest_revenue_orders_df = result_grouped[result_grouped['POTENTIAL_REVENUE'] == max_revenue]

# Sort by revenue in descending order
final_result_df = largest_revenue_orders_df.sort_values(by='POTENTIAL_REVENUE', ascending=False)

# Write results to CSV
final_result_df.to_csv('query_output.csv', index=False)

# Cleanup connections
mysql_conn.close()
mongo_client.close()
