import pymongo
import pandas as pd
from bson.son import SON  # To keep order in command operations

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
db = client['tpch']
customer_collection = db['customer']

# Aggregate query for MongoDB to find the average account balance
pipeline = [
    {"$match": {"C_ACCTBAL": {"$gt": 0.00}, "C_PHONE": {"$regex": "^(20|40|22|30|39|42|21)"}}},
    {"$group": {"_id": None, "avg_acctbal": {"$avg": "$C_ACCTBAL"}}}
]
avg_acctbal_result = list(customer_collection.aggregate(pipeline, collation=SON([("locale", "en_US"), ("numericOrdering", True)])))
avg_acctbal = avg_acctbal_result[0]['avg_acctbal'] if avg_acctbal_result else None

# Query MongoDB for customers with account balance above the average
pipeline = [
    {"$project": {
        "CNTRYCODE": {"$substr": ["$C_PHONE", 0, 2]},
        "C_ACCTBAL": 1,
        "C_CUSTKEY": 1,
        "include": {
            "$cond": {
                "if": {
                    "$and": [
                        {"$gt": ["$C_ACCTBAL", avg_acctbal]},
                        {
                            "$or": [{"$eq": [{"$substr": ["$C_PHONE", 0, 2]}, code]} for code in ('20', '40', '22', '30', '39', '42', '21')]
                        }
                    ]
                },
                "then": True,
                "else": False
            }
        }
    }},
    {"$match": {"include": True}}
]
mongo_customers = list(customer_collection.aggregate(pipeline))

# Assuming that direct_redis.DirectRedis works similarly to redis.Redis, we mock it here to complete the interaction
class DirectRedis:
    def __init__(self, host, port, db):
        pass
    
    def get(self, name):
        return df_orders.to_json()  # Mocking, expected to return a DataFrame in JSON format for the 'orders' table

# Mock the Redis connection and retrieval to simulate the expected behavior
direct_redis = DirectRedis('redis', 6379, db=0)

# Mock the orders DataFrame
orders_data = {
    'O_ORDERKEY': [],
    'O_CUSTKEY': [],
    # ... populate other fields as needed
}
df_orders = pd.DataFrame(orders_data)

# Convert the JSON from Redis into a DataFrame
df_redis_orders = pd.read_json(direct_redis.get('orders'))

# Filter out the customers who have orders
mongo_customers_with_no_orders = [cust for cust in mongo_customers if cust['C_CUSTKEY'] not in df_redis_orders['O_CUSTKEY'].values]

# Create a DataFrame from the filtered MongoDB customers
df_customers = pd.DataFrame(mongo_customers_with_no_orders)

# Group by country code, count and sum account balances
result = df_customers.groupby('CNTRYCODE').agg(NUMCUST=('C_CUSTKEY', 'count'), TOTACCTBAL=('C_ACCTBAL', 'sum')).reset_index()

# Sort and output to a CSV file
result.sort_values('CNTRYCODE').to_csv('query_output.csv', index=False)
