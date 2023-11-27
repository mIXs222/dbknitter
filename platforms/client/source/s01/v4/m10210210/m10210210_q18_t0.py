import pandas as pd
import pymysql
import pymongo
import direct_redis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
mongodb = client['tpch']
orders_collection = mongodb['orders']

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch data from Redis
customer_dataframe = pd.read_json(redis_connection.get('customer').decode('utf-8'))

# Fetch data from MongoDB
orders_cursor = orders_collection.find()
orders_dataframe = pd.DataFrame(list(orders_cursor))

# Fetch data from MySQL
lineitem_sql = """
    SELECT L_ORDERKEY, SUM(L_QUANTITY) as TOTAL_QUANTITY
    FROM lineitem
    GROUP BY L_ORDERKEY
    HAVING SUM(L_QUANTITY) > 300
"""
lineitem_dataframe = pd.read_sql(lineitem_sql, mysql_connection)

# Close the MySQL connection
mysql_connection.close()

# Merge the orders with lineitem to filter by QUANTITY > 300
filtered_orders = pd.merge(orders_dataframe, lineitem_dataframe, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Merge the previous result with customers to get final result
result_dataframe = pd.merge(filtered_orders, customer_dataframe, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Group by the required fields and calculate the SUM(L_QUANTITY)
result_dataframe = result_dataframe.groupby(
    ['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'],
    as_index=False
)['TOTAL_QUANTITY'].sum()

# Order the result
result_dataframe = result_dataframe.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write the result to a CSV file
result_dataframe.to_csv('query_output.csv', index=False)
