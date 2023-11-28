import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    database='tpch', user='root', password='my-secret-pw', host='mysql')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Getting the qualified order keys from Redis where total quantity exceeds 300
lineitem_df = redis_conn.get("lineitem")
lineitem_df = pd.read_json(lineitem_df, orient='split')

# Calculating the sum of quantities for each order
order_quantity = (
    lineitem_df.groupby("L_ORDERKEY")["L_QUANTITY"]
    .sum().reset_index()
)
qualified_order_keys = order_quantity[
    order_quantity["L_QUANTITY"] > 300]["L_ORDERKEY"].tolist()

# Fetching qualified customer and orders from MongoDB
orders_query = {"O_ORDERKEY": {"$in": qualified_order_keys}}
orders_projection = {
    "_id": 0,
    "O_CUSTKEY": 1,
    "O_ORDERKEY": 1,
    "O_ORDERDATE": 1,
    "O_TOTALPRICE": 1
}
orders_df = pd.DataFrame(list(
    mongodb_db.orders.find(orders_query, orders_projection))
)

# Fetching all customers from MySQL
mysql_cursor.execute(
    "SELECT C_CUSTKEY, C_NAME FROM customer"
)
customers_data = mysql_cursor.fetchall()
customers_df = pd.DataFrame(customers_data, columns=["C_CUSTKEY", "C_NAME"])

# Merging and calculating final results
final_result = orders_df.merge(customers_df, left_on="O_CUSTKEY", right_on="C_CUSTKEY")
final_result = final_result.merge(
    order_quantity, left_on="O_ORDERKEY", right_on="L_ORDERKEY"
)
final_result.rename(columns={"L_QUANTITY": "TOTAL_QUANTITY"}, inplace=True)

# Selecting relevant columns
final_result = final_result[[
    "C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE", "TOTAL_QUANTITY"
]]

# Sorting as asked in the query
final_result.sort_values(by=["O_TOTALPRICE", "O_ORDERDATE"], ascending=[False, True], inplace=True)

# Writing to CSV
final_result.to_csv('query_output.csv', index=False)

# Closing all connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
