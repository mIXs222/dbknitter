import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Function to connect to MongoDB and execute Mongo query
def mongo_query():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client.tpch
    
    # Extracting nation and orders data from MongoDB
    nations = pd.DataFrame(list(db.nation.find()))
    orders = pd.DataFrame(list(db.orders.find({"O_ORDERDATE": {"$gte": datetime(1990, 1, 1), "$lt": datetime(1995, 1, 1)}})))
    
    # Filtering nations in the 'ASIA' region
    asia_nations = nations[nations['N_REGIONKEY'] == 'ASIA']
    
    # Joining orders with asia_nations on nation key
    orders = orders.merge(asia_nations, how='inner', left_on='O_CUSTKEY', right_on='N_NATIONKEY')
    
    return orders[['N_NAME', 'O_ORDERKEY', 'O_CUSTKEY']]

# Function to connect to Redis and execute Redis query
def redis_query():
    redis_client = DirectRedis(host='redis', port=6379, db=0)

    # Extracting region data from Redis and filtering for 'ASIA'
    df_region = pd.DataFrame([eval(x) for x in redis_client.get('region') if x])  # convert bytes to str, then str to dict
    asia_regions = df_region[df_region['R_NAME'] == 'ASIA']
    
    # Extracting supplier, customer, and lineitem data from Redis
    df_supplier = pd.DataFrame([eval(x) for x in redis_client.get('supplier')])
    df_customer = pd.DataFrame([eval(x) for x in redis_client.get('customer')])
    df_lineitem = pd.DataFrame([eval(x) for x in redis_client.get('lineitem')])

    # Filtering suppliers and customers within 'ASIA'
    asia_suppliers = df_supplier[df_supplier['S_NATIONKEY'].isin(asia_regions['R_REGIONKEY'])]
    asia_customers = df_customer[df_customer['C_NATIONKEY'].isin(asia_regions['R_REGIONKEY'])]
    
    # Joining lineitem with asia_suppliers and asia_customers on supplier and customer keys
    df_lineitem = df_lineitem.merge(asia_suppliers, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    df_lineitem = df_lineitem.merge(asia_customers, how='inner', left_on='L_ORDERKEY', right_on='C_CUSTKEY')

    return df_lineitem

# Executing Mongo and Redis queries
orders = mongo_query()
lineitems = redis_query()

# Now join the results from MongoDB and Redis on the order key
final_result = orders.merge(lineitems, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate revenue
final_result['REVENUE'] = final_result['L_EXTENDEDPRICE'] * (1 - final_result['L_DISCOUNT'])

# Group by nation and sum revenue
grouped_result = final_result.groupby('N_NAME', as_index=False)['REVENUE'].sum()

# Sort by revenue in descending order
sorted_result = grouped_result.sort_values(by='REVENUE', ascending=False)

# Output to CSV
sorted_result.to_csv('query_output.csv', index=False)
