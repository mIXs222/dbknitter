import pymongo
from bson.objectid import ObjectId
import pandas as pd
import direct_redis
import datetime

# Function to connect to MongoDB and retrieve data
def get_mongo_data():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client.tpch

    customers = pd.DataFrame(list(db.customer.find()))
    orders = pd.DataFrame(list(db.orders.find({
        "O_ORDERDATE": {"$gte": datetime.datetime(1993, 10, 1), "$lt": datetime.datetime(1994, 1, 1)}})))
    lineitems = pd.DataFrame(list(db.lineitem.find({
        "L_RETURNFLAG": "R" })))

    return customers, orders, lineitems

# Function to connect to Redis and retrieve data
def get_redis_data():
    r = direct_redis.DirectRedis(host="redis", port=6379, db=0)
    nation_data = r.get('nation')
    nation = pd.read_json(nation_data)

    return nation

# Get MongoDB data
customers, orders, lineitems = get_mongo_data()

# Get Redis data
nation = get_redis_data()

# Merge Redis data with MongoDB data
customer_nation = customers.merge(nation, how='left', left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Join orders and lineitems
orders_lineitems = orders.merge(lineitems, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate lost revenue
orders_lineitems['LOST_REVENUE'] = orders_lineitems['L_EXTENDEDPRICE'] * (1 - orders_lineitems['L_DISCOUNT'])

# Merge with customers
result = customer_nation.merge(orders_lineitems, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Group by customer and sum lost revenue
grouped_result = result.groupby(['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT'])['LOST_REVENUE'].sum().reset_index()

# Sort the results
sorted_result = grouped_result.sort_values(by=['LOST_REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, True])

# Output the required columns
final_result = sorted_result[['C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT', 'LOST_REVENUE']]

# Write results to CSV file
final_result.to_csv('query_output.csv', index=False)
