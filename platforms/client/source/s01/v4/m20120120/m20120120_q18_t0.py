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

# Execute query to get lineitem data with SUM(L_QUANTITY) > 300
mysql_cursor.execute("""
    SELECT L_ORDERKEY, SUM(L_QUANTITY) as SUM_QTY
    FROM lineitem
    GROUP BY L_ORDERKEY
    HAVING SUM_QTY > 300
""")
qualified_orders = mysql_cursor.fetchall()

# Create a DataFrame for lineitem data
df_lineitem = pd.DataFrame(qualified_orders, columns=['L_ORDERKEY', 'SUM_QTY'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
mongo_collection = mongo_db["customer"]

# Get the customer data
customer_data = list(mongo_collection.find({}, {'_id':0}))
df_customer = pd.DataFrame(customer_data)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get orders data
orders_data = redis_client.get('orders')
df_orders = pd.read_json(orders_data)

# Join dataframes to mimic the SQL query
df_query = (
    df_customer.merge(df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])
    .agg({'L_QUANTITY': 'sum'})
    .reset_index()
    .sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])
)

# Write to CSV
df_query.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
