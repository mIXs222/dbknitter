import pymongo
import pandas as pd
import direct_redis

# Connection details for MongoDB
mongodb_conn = {
    "hostname": "mongodb",
    "port": 27017,
    "database": "tpch"
}

# Connect to MongoDB using pymongo
mongodb_client = pymongo.MongoClient(mongodb_conn["hostname"], mongodb_conn["port"])
mongodb_db = mongodb_client[mongodb_conn["database"]]

# Get data from MongoDB collections
nation = pd.DataFrame(list(mongodb_db.nation.find()))
region = pd.DataFrame(list(mongodb_db.region.find()))
supplier = pd.DataFrame(list(mongodb_db.supplier.find()))

# Filter data as per the query conditions
supplier_nation = supplier.merge(nation, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
region_asia = region[region["R_NAME"] == "ASIA"]
supplier_nation_region = supplier_nation.merge(region_asia, left_on="N_REGIONKEY", right_on="R_REGIONKEY")

# Connection details for Redis
redis_conn = {
    "hostname": "redis",
    "port": 6379,
    "database": "0"
}

# Connect to Redis using direct_redis.DirectRedis
redis_client = direct_redis.DirectRedis(host=redis_conn["hostname"], port=redis_conn["port"], db=redis_conn["database"])

# Get data from Redis
customer = pd.read_json(redis_client.get('customer'), orient='records')
orders = pd.read_json(redis_client.get('orders'), orient='records')
lineitem = pd.read_json(redis_client.get('lineitem'), orient='records')

# Filter orders by date range
orders = orders[(orders['O_ORDERDATE'] >= '1990-01-01') & (orders['O_ORDERDATE'] < '1995-01-01')]

# Combine data from Redis
customer_orders = customer.merge(orders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
customer_orders_lineitem = customer_orders.merge(lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")

# Combine with MongoDB data
combined_data = customer_orders_lineitem.merge(supplier_nation_region, left_on="C_NATIONKEY", right_on="S_NATIONKEY")

# Perform the aggregation
result = combined_data.groupby("N_NAME").apply(
    lambda df: pd.Series({
        "REVENUE": (df["L_EXTENDEDPRICE"] * (1 - df["L_DISCOUNT"])).sum()
    })
).reset_index()

# Sort the results by REVENUE in descending order
result = result.sort_values(by="REVENUE", ascending=False)

# Write the result to CSV file
result.to_csv("query_output.csv", index=False)
