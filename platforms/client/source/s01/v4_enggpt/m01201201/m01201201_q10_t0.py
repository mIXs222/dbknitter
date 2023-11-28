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
    charset='utf8mb4')

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connection to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MySQL
with mysql_conn.cursor() as cursor:
    order_query = """
    SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE FROM orders
    WHERE O_ORDERDATE BETWEEN '1993-10-01' AND '1993-12-31';
    """
    nation_query = "SELECT N_NATIONKEY, N_NAME FROM nation;"
    cursor.execute(order_query)
    orders = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE'])
    cursor.execute(nation_query)
    nations = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME'])

mysql_conn.close()

# Retrieve data from MongoDB
lineitems = pd.DataFrame(list(mongodb.lineitem.find(
    {'L_RETURNFLAG': 'R', 'L_SHIPDATE': {'$gte': '1993-10-01', '$lt': '1994-01-01'}}
)))

# Join the dataframes for orders and lineitems
merged_df = pd.merge(orders, lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')

# Calculate Revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Retrieve data from Redis and convert to DataFrame
customer_data = redis.get('customer')
customers = pd.read_json(customer_data)

# Join the dataframes to include customer details
result_df = pd.merge(merged_df, customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner')

# Join with nation information
result_df = pd.merge(result_df, nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='inner')

# Group by and calculate total revenue per customer
grouped = result_df.groupby(
    ['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT']
).agg({'REVENUE': 'sum'}).reset_index()

# Order the results
sorted_df = grouped.sort_values(
    by=['REVENUE', 'C_CUSTKEY', 'C_NAME', -grouped['C_ACCTBAL'].astype(float)],
    ascending=[True, True, True, False]
)

# Output to CSV
sorted_df.to_csv('query_output.csv', index=False)
