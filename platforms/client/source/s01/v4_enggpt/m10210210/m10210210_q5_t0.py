import pandas as pd
import pymysql
import pymongo
import direct_redis
import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
# Query MySQL for region and lineitem
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT R_REGIONKEY, R_NAME FROM region WHERE R_NAME = 'ASIA';")
    regions = pd.DataFrame(cursor.fetchall(), columns=["R_REGIONKEY", "R_NAME"])
    cursor.execute("SELECT * FROM lineitem;")
    lineitems = pd.DataFrame(cursor.fetchall(), columns=["L_ORDERKEY", "L_EXTENDEDPRICE", "L_DISCOUNT"])
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
# Query MongoDB for nation, supplier, and orders
nations = pd.DataFrame(list(mongo_db.nation.find()))
suppliers = pd.DataFrame(list(mongo_db.supplier.find()))
orders = pd.DataFrame(list(mongo_db.orders.find({"O_ORDERDATE": {
    "$gte": datetime.datetime(1990, 1, 1),
    "$lte": datetime.datetime(1994, 12, 31)
}})))
mongo_client.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
customers_str = redis_conn.get('customer').decode('utf-8')
customers = pd.read_json(customers_str)

# Join operations
orders_customers = orders.merge(customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
lineitem_orders = lineitems.merge(orders_customers, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
nation_region = nations.merge(regions, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
result = lineitem_orders.merge(nation_region, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Calculate total revenue
result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Group by nation name and calculate total revenue
final_result = result.groupby('N_NAME').agg(TOTAL_REVENUE=('REVENUE', 'sum')).reset_index()

# Order by revenue descending
final_result = final_result.sort_values(by='TOTAL_REVENUE', ascending=False)

# Write to CSV
final_result.to_csv('query_output.csv', index=False)
