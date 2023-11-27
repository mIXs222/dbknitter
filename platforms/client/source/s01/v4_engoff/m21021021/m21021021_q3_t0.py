# query.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
mongodb = client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL to get customer information with market segment "BUILDING"
mysql_query = """
SELECT C_CUSTKEY, C_MKTSEGMENT
FROM customer
WHERE C_MKTSEGMENT = 'BUILDING';
"""
customers = pd.read_sql(mysql_query, mysql_connection)

# Query MongoDB to get lineitem information with conditions
lineitems = pd.DataFrame(list(mongodb.lineitem.find(
    {
        "L_SHIPDATE": {"$gt": "1995-03-15"},
        "L_RETURNFLAG": {"$ne": "Y"}  # Assuming "Y" means shipped
    },
    {
        "L_ORDERKEY": 1,
        "L_EXTENDEDPRICE": 1,
        "L_DISCOUNT": 1
    }
)))

# Get orders from Redis
orders_df = pd.read_json(redis_client.get('orders'), orient='records')

# Close MySQL connection
mysql_connection.close()

# Filter orders for customers in the "BUILDING" market segment
building_orders = orders_df[orders_df['O_CUSTKEY'].isin(customers['C_CUSTKEY'])]

# Merge lineitem and orders data
merged_data = building_orders.merge(lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate the potential revenue
merged_data['REVENUE'] = merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT'])

# Group by order key and sum up potential revenue, also get shipping priority
grouped_data = merged_data.groupby('O_ORDERKEY').agg({'REVENUE': 'sum', 'O_SHIPPRIORITY': 'first'}).reset_index()

# Get the largest revenue orders
largest_revenue_orders = grouped_data.sort_values('REVENUE', ascending=False).head()

# Save to CSV
largest_revenue_orders.to_csv('query_output.csv', index=False)
