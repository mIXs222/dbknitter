# Python code (query_script.py)
import pymysql
import pymongo
import pandas as pd
from bson.objectid import ObjectId
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Execute MySQL query to gather orders
with mysql_conn.cursor() as cursor:
    mysql_query = """
    SELECT O_ORDERKEY, O_SHIPPRIORITY
    FROM orders
    WHERE O_ORDERDATE < '1995-03-15' AND O_ORDERSTATUS = 'O'
    """
    cursor.execute(mysql_query)
    orders_data = cursor.fetchall()

# Process orders data into DataFrame
orders_df = pd.DataFrame(orders_data, columns=['O_ORDERKEY', 'O_SHIPPRIORITY'])

# Get lineitem data from MongoDB
lineitem_data = mongo_db['lineitem'].find({})
lineitem_df = pd.DataFrame(list(lineitem_data))

# Calculate potential revenue
lineitem_df['POTENTIAL_REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Merge data
merged_data = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Get customer data from Redis
customer_data = redis_conn.get('customer')
customer_df = pd.read_json(customer_data)

# Filter customers by market segment
customer_df = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']

# Merge with customer data
final_data = pd.merge(merged_data, customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Group by order key and calculate total revenue, sort by revenue descending
result_df = final_data.groupby('O_ORDERKEY').agg(
    TOTAL_REVENUE=pd.NamedAgg(column='POTENTIAL_REVENUE', aggfunc='sum'),
    O_SHIPPRIORITY=pd.NamedAgg(column='O_SHIPPRIORITY', aggfunc='min')
).reset_index()

result_df = result_df.sort_values(by='TOTAL_REVENUE', ascending=False)

# Write result to CSV
result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
redis_conn.close()
