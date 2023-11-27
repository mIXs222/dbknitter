import pymysql
import pymongo
from bson import ObjectId
import pandas as pd
import csv
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for lineitem data
li_query = "SELECT L_ORDERKEY, SUM(L_QUANTITY) as TOTAL_QUANTITY FROM lineitem GROUP BY L_ORDERKEY HAVING TOTAL_QUANTITY > 300;"
with mysql_conn.cursor() as cursor:
    cursor.execute(li_query)
    lineitem_records = cursor.fetchall()

# Get order keys with quantity greater than 300
order_keys = [record[0] for record in lineitem_records]

# Query MongoDB for orders data
orders_query = {'O_ORDERKEY': {'$in': order_keys}}
orders_data = mongodb.orders.find(orders_query)
orders_records = list(orders_data)

# Fetch customer keys from orders
customer_keys = [record['O_CUSTKEY'] for record in orders_records]

# Query Redis for customers data
customer_data = redis_client.get('customer')
customers_df = pd.read_json(customer_data)

# Filter customers based on customer keys
large_volume_customers = customers_df[customers_df['C_CUSTKEY'].isin(customer_keys)]

# Merge the orders and lineitem data
orders_df = pd.DataFrame(orders_records)
merged_df = orders_df.merge(large_volume_customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Select required columns
result_df = merged_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']]
result_df.columns = ['Customer Name', 'Customer Key', 'Order Key', 'Order Date', 'Total Price']

# Include TOTAL_QUANTITY for each order
result_df['Quantity'] = result_df['Order Key'].apply(lambda x: next((record[1] for record in lineitem_records if record[0] == x), 0))

# Write to CSV
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)

# Close connections
mysql_conn.close()
mongo_client.close()
redis_client.close()
