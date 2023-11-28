# Import required libraries
import pymysql
import pymongo
import pandas as pd
from sqlalchemy import create_engine
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis database
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query the 'orders' table in MySQL to get orders with total item quantity > 300
with mysql_conn.cursor() as cursor:
    subquery = """
    SELECT O_ORDERKEY
    FROM lineitem
    INNER JOIN orders ON O_ORDERKEY = L_ORDERKEY
    GROUP BY O_ORDERKEY
    HAVING SUM(L_QUANTITY) > 300
    """
    cursor.execute(subquery)
    order_keys = [item[0] for item in cursor.fetchall()]

# Query the 'lineitem' collection in MongoDB and filter by the order keys
lineitem_df = pd.DataFrame(list(mongo_db.lineitem.find({'L_ORDERKEY': {'$in': order_keys}})))

# Query 'orders' table in MySQL and join with the results from 'lineitem' collection
orders_query = f"SELECT * FROM orders WHERE O_ORDERKEY IN ({','.join(map(str, order_keys))})"
orders_df = pd.read_sql(orders_query, mysql_conn)

# Join 'orders' and 'lineitem' dataframes
joined_df = orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Get 'customer' data from Redis
customer_df = pd.read_json(redis_client.get('customer'))

# Join 'customer' dataframe with 'joined' dataframe
final_df = joined_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Calculate the sum of quantities in line items and group by relevant fields
final_df['TOTAL_QUANTITY'] = final_df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])['L_QUANTITY'].transform('sum')

# Filter grouped dataframe based on total quantity criterion and order by total price and order date
result_df = final_df[final_df['TOTAL_QUANTITY'] > 300].groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).first().reset_index()

# Sort the results and select relevant columns
output_df = result_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])
output_df = output_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY']]

# Write the query output to a CSV file
output_df.to_csv('query_output.csv', index=False)

# Close the database connections
mysql_conn.close()
mongo_client.close()
