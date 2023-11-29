import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Get lineitem data from MySQL
mysql_cursor.execute(
    "SELECT L_ORDERKEY, SUM(L_QUANTITY) AS TOTAL_QUANTITY, SUM(L_EXTENDEDPRICE) AS TOTAL_PRICE FROM lineitem GROUP BY L_ORDERKEY HAVING SUM(L_QUANTITY) > 300"
)
lineitem_data = mysql_cursor.fetchall()

# Convert to a DataFrame
lineitem_df = pd.DataFrame(lineitem_data, columns=['O_ORDERKEY', 'TOTAL_QUANTITY', 'O_TOTALPRICE'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']

# Get orders data from MongoDB
orders_data = list(orders_collection.find({}))
orders_df = pd.DataFrame(orders_data)

# Drop unnecessary columns from orders
orders_df = orders_df[['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE']]

# Combine lineitem and orders data
result_df = pd.merge(orders_df, lineitem_df, on='O_ORDERKEY')

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get customer data and load into DataFrame
customer_data = redis_client.get('customer')
customer_df = pd.read_json(customer_data, orient='index')

# Combine result with customer data
final_df = pd.merge(result_df, customer_df, left_on='O_CUSTKEY', right_index=True)

# Filter the columns
final_df = final_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY']]

# Sort by O_TOTALPRICE and O_ORDERDATE
final_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Write the result to a CSV file
final_df.to_csv('query_output.csv', index=False)

# Close all connections
mysql_conn.close()
mongo_client.close()
redis_client.close()
