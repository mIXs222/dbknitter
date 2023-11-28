import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Fetch Orders data from MySQL
orders_query = '''
SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_TOTALPRICE
FROM orders
WHERE O_ORDERKEY IN (
    SELECT L_ORDERKEY
    FROM lineitem
    GROUP BY L_ORDERKEY
    HAVING SUM(L_QUANTITY) > 300
)
'''
orders_df = pd.read_sql(orders_query, mysql_conn)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
customer_collection = mongo_db['customer']

# Fetch Customer data from MongoDB
customer_df = pd.DataFrame(list(customer_collection.find({}, {'_id': 0})))

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch LineItem data from Redis and convert to DataFrame
lineitem_raw = redis_conn.get('lineitem')
lineitem_df = pd.DataFrame(eval(lineitem_raw))

# Merge the dataframes
merged_df = (
    orders_df.merge(customer_df, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')
)

# Calculate the sum of quantities and filter the orders with quantity greater than 300
def filter_orders(group):
    if group['L_QUANTITY'].sum() > 300:
        return True
    return False

filtered_df = merged_df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])\
    .filter(filter_orders)

# Selecting required columns and sort as per the criteria
result_df = filtered_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']]\
    .groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])\
    .sum().reset_index().sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write to CSV file
result_df.to_csv('query_output.csv', index=False)

# Clean up connections
mysql_conn.close()
redis_conn.close()
mongo_client.close()
