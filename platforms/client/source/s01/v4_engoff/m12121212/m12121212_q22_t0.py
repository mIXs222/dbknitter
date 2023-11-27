import pymongo
import pandas as pd
from datetime import datetime, timedelta
from direct_redis import DirectRedis

# MongoDB connection and query
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
orders_collection = mongo_db["orders"]

# Current date minus 7 years
cutoff_date = datetime.now() - timedelta(days=7*365)
match_stage = {
    "$match": {
        "O_ORDERDATE": {"$lte": cutoff_date},
        "O_TOTALPRICE": {"$gt": 0.00}
    }
}
group_stage = {
    "$group": {
        "_id": "$O_CUSTKEY",
        "avgTotalPrice": {"$avg": "$O_TOTALPRICE"}
    }
}
project_stage = {
    "$project": {
        "custkey": "$_id",
        "avgTotalPrice": 1,
        "_id": 0
    }
}

# Execute the aggregation pipeline
mongo_orders = list(orders_collection.aggregate([match_stage, group_stage, project_stage]))
mongo_custkeys = set([doc.get('custkey') for doc in mongo_orders])

# Redis connection and retrieval
redis_client = DirectRedis(host="redis", port=6379, db=0)
customer_data = redis_client.get('customer')
df_customer = pd.read_json(customer_data)

# Filter the dataframe for customers with specific country codes
country_codes = {'20', '40', '22', '30', '39', '42', '21'}
df_customer['country_code'] = df_customer['C_PHONE'].str[:2]
df_customer = df_customer[df_customer['country_code'].isin(country_codes)]

# Exclude customers who have placed orders in the last 7 years
df_customer = df_customer[~df_customer['C_CUSTKEY'].isin(mongo_custkeys)]

# Filter customers with average account balance greater than 0
df_filtered = df_customer[df_customer['C_ACCTBAL'] > 0.00]

# Group by the country code
result = df_filtered.groupby('country_code')['C_ACCTBAL'].agg(['count', 'mean']).reset_index()

# Rename columns and output to a CSV file
result.columns = ['Country Code', 'Customer Count', 'Average Account Balance']
result.to_csv('query_output.csv', index=False)
