import pandas as pd
import pymysql
import pymongo
import direct_redis

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

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from MySQL with a subquery to sum quantity and filter
lineitem_query = """
SELECT
    L_ORDERKEY,
    SUM(L_QUANTITY) as TOTAL_QUANTITY
FROM
    lineitem
GROUP BY
    L_ORDERKEY
HAVING
    SUM(L_QUANTITY) > 300
"""
mysql_cursor.execute(lineitem_query)
order_keys_quantity_over_300 = mysql_cursor.fetchall()

# Convert order keys to a list for use in MongoDB query
order_keys_list = [item[0] for item in order_keys_quantity_over_300]

# Query orders in MongoDB using order_keys_list
orders_coll = mongo_db['orders']
orders_query = {'O_ORDERKEY': {'$in': order_keys_list}}
orders_docs = orders_coll.find(orders_query)

# Create a DataFrame from the orders
orders_data = pd.DataFrame(list(orders_docs))

# Convert orders data to use it for merging
orders_data.rename(columns={'O_ORDERKEY': 'L_ORDERKEY'}, inplace=True)

# Get customer data from Redis
customer_data = pd.read_json(redis_conn.get('customer'), orient='split')

# Merging the dataframes
combined_data = orders_data.merge(customer_data, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
combined_data = combined_data.merge(pd.DataFrame(order_keys_quantity_over_300, columns=['L_ORDERKEY', 'TOTAL_QUANTITY']), on='L_ORDERKEY')

# Selecting the required columns and renaming them as per the requirement
selected_data = combined_data[['C_NAME', 'C_CUSTKEY', 'L_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY']]

# Group by the required columns
grouped_data = selected_data.groupby(['C_NAME', 'C_CUSTKEY', 'L_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'], as_index=False).sum()

# Sort by total price in descending order and then by order date
final_data = grouped_data.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write to the CSV file
final_data.to_csv('query_output.csv', index=False)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
redis_conn.close()
