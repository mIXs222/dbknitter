import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Connection to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Connection to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Getting data from MySQL
with mysql_conn.cursor() as cursor:
    mysql_query = """
    SELECT C_CUSTKEY, C_NAME
    FROM customer
    """
    cursor.execute(mysql_query)
    customers = pd.DataFrame(cursor.fetchall(), columns=['C_CUSTKEY', 'C_NAME'])

# Getting data from MongoDB
orders = pd.DataFrame(list(mongodb_db['orders'].find()))

# Get lineitem data from Redis and convert to Pandas DataFrame
lineitem_pandas = pd.read_json(redis_client.get('lineitem').decode('utf-8'))

# Close MySQL connection
mysql_conn.close()

# Aggregate by orderkey to compute total quantity
lineitem_aggregate = lineitem_pandas.groupby('L_ORDERKEY').agg({'L_QUANTITY': 'sum'}).reset_index()
large_orders = lineitem_aggregate[lineitem_aggregate['L_QUANTITY'] > 300]

# Merge with order details
large_order_details = pd.merge(large_orders, orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Merge with customer details
result = pd.merge(customers, large_order_details, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Select and order the final columns
final_result = result[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]
final_result_sorted = final_result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write to CSV
final_result_sorted.to_csv('query_output.csv', index=False)
