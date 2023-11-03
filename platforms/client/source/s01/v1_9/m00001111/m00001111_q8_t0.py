import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient

# Setup MySQL connection
mysql_engine = create_engine("mysql+pymysql://root:my-secret-pw@mysql:3306/tpch")

# Setup MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['tpch']

# Read MySQL tables
nation = pd.read_sql_query('SELECT * FROM Nation', mysql_engine)
region = pd.read_sql_query('SELECT * FROM Region', mysql_engine)
part = pd.read_sql_query('SELECT * FROM Part', mysql_engine)
supplier = pd.read_sql_query('SELECT * FROM Supplier', mysql_engine)

# Read MongoDB tables
partsupp = pd.DataFrame(list(db.partsupp.find()))
customer = pd.DataFrame(list(db.customer.find()))
orders = pd.DataFrame(list(db.orders.find()))
lineitem = pd.DataFrame(list(db.lineitem.find()))

# Merge DataFrames as per conditions provided in the query
df = pd.merge(part, lineitem, left_on='P_PARTKEY', right_on='L_PARTKEY')
df = pd.merge(df, supplier, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
df = pd.merge(df, orders, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df = pd.merge(df, customer, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df = pd.merge(df, nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY', suffixes=('','_supplier'))
df = pd.merge(df, nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY', suffixes=('','_customer'))
df = pd.merge(df, region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# The query conditions
df = df[df['R_NAME'] == 'ASIA']
df = df[(df['O_ORDERDATE'] >= '1995-01-01') & (df['O_ORDERDATE'] <= '1996-12-31')]
df = df[df['P_TYPE'] == 'SMALL PLATED COPPER']
df['O_YEAR'] = df['O_ORDERDATE'].dt.year
df['VOLUME'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])

mkt_share = df[df['N_NAME'] == 'INDIA']['VOLUME'].sum() / df['VOLUME'].sum()
df = df.loc[:, ['O_YEAR', 'VOLUME', 'N_NAME']]

# Save the result to a csv file
df.to_csv('query_output.csv', index=False)
