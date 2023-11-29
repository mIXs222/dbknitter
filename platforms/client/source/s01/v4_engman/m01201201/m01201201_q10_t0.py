import pandas as pd
import pymysql
import pymongo
from datetime import datetime
import direct_redis

# Connecting to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn:
    with mysql_conn.cursor() as cursor:
        # Fetch nation and orders data within the specified date range
        cursor.execute("""
            SELECT n.N_NATIONKEY, n.N_NAME, o.O_CUSTKEY,
                   o.O_TOTALPRICE, o.O_ORDERDATE
            FROM nation n
            INNER JOIN orders o ON n.N_NATIONKEY = o.O_ORDERKEY
            WHERE o.O_ORDERDATE >= '1993-10-01' AND o.O_ORDERDATE < '1994-01-01'
        """)
        orders_nation_data = cursor.fetchall()
        orders_nation_columns = ['N_NATIONKEY', 'N_NAME', 'O_CUSTKEY', 'O_TOTALPRICE', 'O_ORDERDATE']
        df_orders_nation = pd.DataFrame(orders_nation_data, columns=orders_nation_columns)

# Connecting to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['lineitem']
# Fetch lineitem data
lineitem_data = list(mongo_collection.find(
    {'L_SHIPDATE': {'$gte': datetime(1993, 10, 1), '$lt': datetime(1994, 1, 1)}},
    {'_id': 0}
))
df_lineitem = pd.DataFrame(lineitem_data)

# Connecting to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
customer_data = redis_client.get('customer')
df_customer = pd.read_json(customer_data)

# Data manipulation
# Merge relevant data sets
df_merged = df_orders_nation.merge(df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
df_merged = df_merged.merge(df_customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner')

# Calculate revenue lost
df_merged['REVENUE_LOST'] = df_merged['L_EXTENDEDPRICE'] * (1 - df_merged['L_DISCOUNT'])

# Group by customer and calculate the total revenue lost
df_result = df_merged.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']).agg({
    'REVENUE_LOST': 'sum'
}).reset_index()

# Sort the result as per the requirements
df_result_sorted = df_result.sort_values(by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
                                         ascending=[True, True, True, False])

# Write the result to csv file
df_result_sorted.to_csv('query_output.csv', index=False)
