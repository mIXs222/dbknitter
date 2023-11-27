import pymongo
import redis
import pandas as pd

# MongoDB connection and query
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client['tpch']
customer_table = mongodb['customer']
customers = pd.DataFrame(list(customer_table.find({}, {
    "C_CUSTKEY": 1,
    "C_NAME": 1,
    "_id": 0
})))

# Redis connection and query
class DirectRedis(redis.Redis):
    def get(self, name):
        value = super().get(name)
        if value is not None:
            return pd.read_msgpack(value)

redis_client = DirectRedis(host='redis', port=6379, db=0)
orders = redis_client.get('orders')
lineitem = redis_client.get('lineitem')

# Filter for lineitem with quantity larger than 300
large_lineitem = lineitem[lineitem['L_QUANTITY'] > 300]

# Join operations
large_orders = orders.merge(large_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
large_order_customers = large_orders.merge(customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Select required fields
result = large_order_customers[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Write the result to a CSV file
result.to_csv('query_output.csv', index=False)
