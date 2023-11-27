import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("""
SELECT o.O_CUSTKEY, 
       l.L_EXTENDEDPRICE, 
       l.L_DISCOUNT, 
       l.L_SUPPKEY 
FROM orders o 
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY 
WHERE o.O_ORDERDATE >= '1990-01-01' 
  AND o.O_ORDERDATE < '1995-01-01';
""")
orders_lineitem = pd.DataFrame(mysql_cursor.fetchall(),
                               columns=['O_CUSTKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SUPPKEY'])
mysql_conn.close()

# MongoDB connection and query
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
nation = pd.DataFrame(list(mongo_db.nation.find({'N_NAME': 'ASIA'})))
region = pd.DataFrame(list(mongo_db.region.find({'R_NAME': 'ASIA'})))
mongo_client.close()

# Redis connection and data retrieval
redis_conn = DirectRedis(host='redis', port=6379, db=0)
supplier_data = redis_conn.get('supplier')
customer_data = redis_conn.get('customer')
supplier = pd.read_json(supplier_data)
customer = pd.read_json(customer_data)

# Filter customers and suppliers from ASIA region
customers_in_asia = customer[customer['C_NATIONKEY'].isin(nation['N_NATIONKEY'])]
suppliers_in_asia = supplier[supplier['S_NATIONKEY'].isin(nation['N_NATIONKEY'])]

# Filter orders and lineitems with qualifying customers and suppliers
qualifying_lineitems = orders_lineitem[
    (orders_lineitem['O_CUSTKEY'].isin(customers_in_asia['C_CUSTKEY'])) & 
    (orders_lineitem['L_SUPPKEY'].isin(suppliers_in_asia['S_SUPPKEY']))
]

# Calculate the revenue volume
qualifying_lineitems['REVENUE'] = qualifying_lineitems['L_EXTENDEDPRICE'] * (1 - qualifying_lineitems['L_DISCOUNT'])

# Sum revenue by nation
revenue_by_nation = qualifying_lineitems.groupby(customers_in_asia['C_NATIONKEY'])['REVENUE'].sum().reset_index()
revenue_by_nation = revenue_by_nation.merge(nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='inner')

# Select relevant columns and sort by revenue
result = revenue_by_nation[['N_NAME', 'REVENUE']].sort_values(by='REVENUE', ascending=False)

# Write the query's output to a csv file
result.to_csv('query_output.csv', index=False)
