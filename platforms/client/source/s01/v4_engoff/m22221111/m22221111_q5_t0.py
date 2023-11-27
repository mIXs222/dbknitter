import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Initialize connections to databases
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
tpch_db = mongo_client["tpch"]

redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch required data from MongoDB
customers = pd.DataFrame(list(tpch_db.customer.find()))
orders = pd.DataFrame(list(tpch_db.orders.find({
    "O_ORDERDATE": {
        "$gte": datetime(1990, 1, 1),
        "$lt": datetime(1995, 1, 1)
    }
})))
lineitems = pd.DataFrame(list(tpch_db.lineitem.find()))

# Fetch required data from Redis
nation_df = pd.read_json(redis_client.get("nation"), orient='records')
region_df = pd.read_json(redis_client.get("region"), orient='records')
supplier_df = pd.read_json(redis_client.get("supplier"), orient='records')

# Perform necessary joins
# First, identify the nations within the ASIA region
asia_region_keys = region_df[region_df['R_NAME'] == 'ASIA']['R_REGIONKEY']
asia_nations = nation_df[nation_df['N_REGIONKEY'].isin(asia_region_keys)]

# Filter suppliers and customers by nations in ASIA
asia_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(asia_nations['N_NATIONKEY'])]
asia_customers = customers[customers['C_NATIONKEY'].isin(asia_nations['N_NATIONKEY'])]

# Join the orders with ASIA customers
asia_orders = orders[orders['O_CUSTKEY'].isin(asia_customers['C_CUSTKEY'])]

# Compute revenue for lineitems associated with ASIA suppliers and customers
qualified_lineitems = lineitems[lineitems['L_ORDERKEY'].isin(asia_orders['O_ORDERKEY']) & lineitems['L_SUPPKEY'].isin(asia_suppliers['S_SUPPKEY'])]
qualified_lineitems['revenue'] = qualified_lineitems['L_EXTENDEDPRICE'] * (1 - qualified_lineitems['L_DISCOUNT'])

# Aggregate revenue by nation
nation_revenue = qualified_lineitems.groupby('L_SUPPKEY')['revenue'].sum().reset_index()
nation_revenue = nation_revenue.merge(asia_suppliers[['S_SUPPKEY', 'S_NATIONKEY']], on='S_SUPPKEY', how='left')
nation_revenue = nation_revenue.groupby('S_NATIONKEY')['revenue'].sum().reset_index()

# Join with nation names and sort by revenue
final_result = nation_revenue.merge(asia_nations[['N_NATIONKEY', 'N_NAME']], left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='left')
final_result = final_result[['N_NAME', 'revenue']].sort_values(by='revenue', ascending=False)

# Output the result to query_output.csv
final_result.to_csv('query_output.csv', index=False)
