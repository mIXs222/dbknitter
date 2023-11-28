import pymysql
import pymongo
import pandas as pd
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# MySQL Query
mysql_query = """
SELECT C_CUSTKEY, C_NAME
FROM customer
WHERE C_MKTSEGMENT = 'BUILDING'
"""

mysql_cursor.execute(mysql_query)
customers = mysql_cursor.fetchall()

# Filter customers with 'BUILDING' market segment
building_cust_keys = [c[0] for c in customers]

# Fetch orders from MongoDB corresponding to the BUILDING segment customers
orders_collection = mongodb['orders']
orders_query = {
    'O_CUSTKEY': {'$in': building_cust_keys},
    'O_ORDERDATE': {'$lt': datetime.strptime('1995-03-15', '%Y-%m-%d')}
}
orders = list(orders_collection.find(orders_query, {'_id': 0}))

# Fetch lineitems from MongoDB corresponding to the orders fetched and shipped after 15th March 1995
lineitem_collection = mongodb['lineitem']
lineitems = []
for order in orders:
    lineitem_query = {
        'L_ORDERKEY': order['O_ORDERKEY'],
        'L_SHIPDATE': {'$gt': datetime.strptime('1995-03-15', '%Y-%m-%d')}
    }
    lineitems.extend(list(lineitem_collection.find(lineitem_query, {'_id': 0})))

# Compute revenue
revenue_data = []
for order in orders:
    for lineitem in lineitems:
        if lineitem['L_ORDERKEY'] == order['O_ORDERKEY']:
            adjusted_price = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
            revenue_data.append((order['O_ORDERKEY'], order['O_ORDERDATE'], order['O_SHIPPRIORITY'], adjusted_price))

# Convert to DataFrame
df = pd.DataFrame(revenue_data, columns=['OrderKey', 'OrderDate', 'ShippingPriority', 'Revenue'])
df['Revenue'] = df['Revenue'].astype(float)
df = df.groupby(['OrderKey', 'OrderDate', 'ShippingPriority']).agg({'Revenue': 'sum'}).reset_index()
df = df.sort_values(by=['Revenue', 'OrderDate'], ascending=[False, True])

# Output the result to a CSV file
df.to_csv('query_output.csv', index=False)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
