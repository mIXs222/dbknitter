import pymongo
import direct_redis
import pandas as pd

# Function to get data from MongoDB
def get_mongodb_data(host, port, db_name):
    mongo_client = pymongo.MongoClient(host=host, port=port)
    db = mongo_client[db_name]
    customer_data = pd.DataFrame(list(db.customer.find({}, {
        '_id': 0,
        'C_CUSTKEY': 1,
        'C_NAME': 1
    })))
    mongo_client.close()
    return customer_data

# Function to get and transform Redis data
def get_redis_data(host, port, db_name):

    r = direct_redis.DirectRedis(host=host, port=port)
    orders_str = r.get('orders')
    lineitem_str = r.get('lineitem')

    # Convert to DataFrame 
    orders_df = pd.read_json(orders_str)
    lineitem_df = pd.read_json(lineitem_str)

    # Aggregating lineitem data to get the total quantity per order.    
    lineitem_agg = lineitem_df.groupby('L_ORDERKEY')['L_QUANTITY'].sum().reset_index()
    lineitem_agg = lineitem_agg[lineitem_agg['L_QUANTITY'] > 300] # Only consider large orders
    
    orders_large = orders_df.merge(lineitem_agg, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    
    # including necessary fields and renaming the columns to match with SQL query columns
    orders_large = orders_large[['O_ORDERKEY', 'O_CUSTKEY', 'O_TOTALPRICE', 'O_ORDERDATE', 'L_QUANTITY']]
    orders_large.columns = ['OrderKey', 'CustomerKey', 'TotalPrice', 'OrderDate', 'TotalQuantity']
    
    return orders_large

# Gathering data
mongodb_data = get_mongodb_data(host='mongodb', port=27017, db_name='tpch')
redis_data = get_redis_data(host='redis', port=6379, db_name='0')

# Merging data from different sources
merged_data = pd.merge(mongodb_data, redis_data, how='inner', left_on='C_CUSTKEY', right_on='CustomerKey')
merged_data = merged_data[['C_NAME', 'C_CUSTKEY', 'OrderKey', 'OrderDate', 'TotalPrice', 'TotalQuantity']]

# Sort the data as per the SQL Query requirement
sorted_data = merged_data.sort_values(by=['TotalPrice', 'OrderDate'], ascending=[False, True])

# Write the result to CSV file
sorted_data.to_csv('query_output.csv', index=False)
