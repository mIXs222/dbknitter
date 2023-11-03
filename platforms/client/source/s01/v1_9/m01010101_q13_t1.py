import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient
import urllib.parse

# MySQL connection
engine = create_engine('mysql://root:my-secret-pw@mysql:3306/tpch')

# Query MySQL to get customers and orders
with engine.connect() as con:
    df_customers = pd.read_sql('SELECT * FROM CUSTOMER', con)
    df_orders = pd.read_sql('SELECT * FROM ORDERS', con)

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['tpch']

# Query MongoDB to get customers and orders
df_m_customers = pd.DataFrame(list(db.customer.find()))
df_m_orders = pd.DataFrame(list(db.orders.find()))

# Merge Mongodb data onto original MySQL data
df_customers = df_customers.append(df_m_customers)
df_orders = df_orders.append(df_m_orders)

# Merge customer and orders
df = pd.merge(df_customers, df_orders, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Filter condition
df = df[~df['O_COMMENT'].str.contains('pending', na=False) 
        & ~df['O_COMMENT'].str.contains('deposits', na=False)]

C_ORDERS = df.groupby('C_CUSTKEY')['O_ORDERKEY'].count().reset_index(name='C_COUNT')

result = C_ORDERS.groupby('C_COUNT')['C_CUSTKEY'].count().reset_index(name='CUSTDIST')

result = result.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Export to csv
result.to_csv('query_output.csv', index=False)
