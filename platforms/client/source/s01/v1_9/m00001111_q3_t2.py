from pymongo import MongoClient
from sqlalchemy import create_engine
import pandas as pd

# connect to MySQL database
engine = create_engine('mysql+mysqlconnector://root:my-secret-pw@mysql:3306/tpch')

# connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client.tpch

# load data from MongoDB
partsupp = pd.DataFrame(list(db.partsupp.find()))
customer = pd.DataFrame(list(db.customer.find()))
orders = pd.DataFrame(list(db.orders.find()))
lineitem = pd.DataFrame(list(db.lineitem.find()))

# load data from MySQL
query = 'SELECT * FROM customer'
customer_mysql = pd.read_sql_query(query, engine)
query = 'SELECT * FROM orders'
orders_mysql = pd.read_sql_query(query, engine)
query = 'SELECT * FROM lineitem'
lineitem_mysql = pd.read_sql_query(query, engine)

# merge MongoDB and MySQL data
customer_combined = pd.concat([customer, customer_mysql])
orders_combined = pd.concat([orders, orders_mysql])
lineitem_combined = pd.concat([lineitem, lineitem_mysql])

# combine tables
data = pd.merge(customer_combined, orders_combined, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
data = pd.merge(data, lineitem_combined, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# filter and aggregate data
data = data[data['C_MKTSEGMENT'] == 'BUILDING']
data = data[data['O_ORDERDATE'] < '1995-03-15']
data = data[data['L_SHIPDATE'] > '1995-03-15']
data['REVENUE'] = data['L_EXTENDEDPRICE'] * (1 - data['L_DISCOUNT'])
grouped = data.groupby(['L_ORDERKEY', 'O_ORDERDATE','O_SHIPPRIORITY'])
result = grouped['REVENUE'].sum().reset_index()

# sort data
result = result.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# write data to csv
result.to_csv('query_output.csv', index=False)
