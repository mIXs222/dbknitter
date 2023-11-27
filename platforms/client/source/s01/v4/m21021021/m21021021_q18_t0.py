import pymysql
import pymongo
import pandas as pd
import direct_redis

# Establish connection to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

with mysql_conn.cursor() as cursor:
    # Query in MySQL for customer information
    cursor.execute("SELECT C_CUSTKEY, C_NAME FROM customer")
    customer_data = cursor.fetchall()
    
# Convert to DataFrame
customer_df = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_NAME'])

# Establish connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Query in MongoDB for lineitem information
pipeline = [
    {"$group": {
        "_id": "$L_ORDERKEY",
        "total_quantity": {"$sum": "$L_QUANTITY"}
    }},
    {"$match": {"total_quantity": {"$gt": 300}}}
]
matching_orderkeys = list(mongodb.lineitem.aggregate(pipeline))

# Extract the order keys
orderkeys_list = [doc['_id'] for doc in matching_orderkeys]

# For Redis, replace 'redis.Redis' with 'direct_redis.DirectRedis'
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'orders' table stored as a DataFrame in Redis
orders_df_pickle = redis_conn.get('orders')
orders_df = pd.read_pickle(orders_df_pickle)

# Filter orders data based on list of order keys
filtered_orders_df = orders_df[orders_df['O_ORDERKEY'].isin(orderkeys_list)]

# Merge customer and orders data frames on C_CUSTKEY
merged_df = pd.merge(customer_df, filtered_orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# We cannot perform the join with lineitem directly since it's in MongoDB and not in DataFrame format yet,
# So we'll filter it in pandas instead using the list of order keys.
# Close the connections
mysql_conn.close()
mongo_client.close()
redis_conn.close()

# Write final dataframe to a CSV file
merged_df.to_csv('query_output.csv', index=False)
