# python_code.py

from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import direct_redis

# Connection to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
customer_collection = mongodb['customer']

# Filter and get the relevant customer data from MongoDB
seven_years_ago = datetime.now() - timedelta(days=7*365)
pipeline = [
    {"$project": {"C_CUSTKEY": 1, "C_PHONE": {"$substr": ["$C_PHONE", 0, 2]}, "C_ACCTBAL": 1}},
    {"$match": {"C_PHONE": {"$in": ['20', '40', '22', '30', '39', '42', '21']}}},
    {"$group": {"_id": "$C_PHONE", "avg_acctbal": {"$avg": {"$cond": [{"$gt": ["$C_ACCTBAL", 0.00]}, "$C_ACCTBAL", None]}}}}
]
countries_customers = list(customer_collection.aggregate(pipeline))
average_balances = {_['id']: _['avg_acctbal'] for _ in countries_customers}

# Initiating connection to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Getting order data from Redis, assumes each order is a separate key in the Redis database
orders_df = pd.DataFrame(r.get('orders'))  # Use only if order data is stored in 'orders' key

# Process order data
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
recent_orders = orders_df['O_CUSTKEY'][orders_df['O_ORDERDATE'] > seven_years_ago].unique()

# Exclude customers who placed recent orders and whose account balance is greater than 0
filtered_customers = [
    {
        "id": _["_id"],
        "num_customers": _["num_customers"],
        "total_balance": _["total_balance"]
    }
    for _ in countries_customers if _["_id"] not in recent_orders and _["avg_acctbal"] > 0
]

# Output the results to CSV
output_df = pd.DataFrame(filtered_customers, columns=['CNTRYCODE', 'num_customers', 'total_balance'])
output_df.sort_values('CNTRYCODE', ascending=True, inplace=True)
output_df.to_csv('query_output.csv', index=False)
