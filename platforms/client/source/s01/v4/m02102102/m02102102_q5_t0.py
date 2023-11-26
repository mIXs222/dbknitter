import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

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
customer_collection = mongo_db['customer']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for nation and supplier
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT S_SUPPKEY, N_NATIONKEY, N_NAME
        FROM supplier
        JOIN nation ON S_NATIONKEY = N_NATIONKEY
    """)
    supplier_nation = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'N_NATIONKEY', 'N_NAME'])

# Query MySQL for orders
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT O_ORDERKEY, O_CUSTKEY
        FROM orders
        WHERE O_ORDERDATE >= '1990-01-01' AND O_ORDERDATE < '1995-01-01'
    """)
    orders = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'O_CUSTKEY'])

# Close MySQL connection
mysql_conn.close()

# Query MongoDB for customer
customer = pd.DataFrame(customer_collection.find(
    {'C_NATIONKEY': {'$exists': True}}
), columns=['C_CUSTKEY', 'C_NATIONKEY'])

# Query Redis for region and lineitem
region = pd.DataFrame(redis_client.get('region'))
lineitem = pd.DataFrame(redis_client.get('lineitem'))

# Filter the region by R_NAME 'ASIA'
region_asia = region[region['R_NAME'] == 'ASIA']

# Start joining the dataframes
result = (
    customer.merge(orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='inner')
    .merge(supplier_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='inner')
    .merge(lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
    .merge(region_asia, left_on='N_REGIONKEY', right_on='R_REGIONKEY', how='inner')
)

# Calculate revenue
result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Group by nation name and calculate sum of revenue
final_result = result.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Sort by revenue in descending order
final_result.sort_values(by='REVENUE', ascending=False, inplace=True)

# Write to CSV
final_result.to_csv('query_output.csv', index=False)
