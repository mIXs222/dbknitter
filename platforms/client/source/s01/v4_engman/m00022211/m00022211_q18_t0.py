# large_volume_customer_query.py
from pymongo import MongoClient
import direct_redis
import pandas as pd

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']
lineitem_collection = mongo_db['lineitem']

# Aggregating line items to calculate total quantity per order
pipeline = [
    {
        '$group': {
            '_id': '$L_ORDERKEY',
            'total_quantity': {
                '$sum': '$L_QUANTITY'
            }
        }
    },
    {
        '$match': {
            'total_quantity': {
                '$gt': 300
            }
        }
    }
]
large_orders = list(lineitem_collection.aggregate(pipeline))
large_order_keys = [order['_id'] for order in large_orders]

# Fetch large orders from orders collection
orders_query = {
    'O_ORDERKEY': {
        '$in': large_order_keys
    }
}
orders_projection = {
    '_id': 0,
    'O_ORDERKEY': 1,
    'O_CUSTKEY': 1,
    'O_ORDERDATE': 1,
    'O_TOTALPRICE': 1
}
large_orders = orders_collection.find(orders_query, orders_projection)

# Convert large orders to DataFrame
orders_df = pd.DataFrame(list(large_orders))

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)
# Fetch customer data and load into DataFrame
customer_data = redis_connection.get('customer')
customers_df = pd.read_json(customer_data)

# Merge orders with customers
result_df = orders_df.merge(customers_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner')

# Select and rename the columns
final_df = result_df[['C_NAME', 'O_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']]
final_df.columns = ['Customer Name', 'Customer Key', 'Order Key', 'Order Date', 'Total Price']

# Sort by Total Price in descending order and Order Date in ascending order
final_df = final_df.sort_values(by=['Total Price', 'Order Date'], ascending=[False, True])

# Output the result to a CSV file
final_df.to_csv('query_output.csv', index=False)

print("Query output saved to query_output.csv.")
