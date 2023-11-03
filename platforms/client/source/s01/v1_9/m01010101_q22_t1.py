import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
mongo_db = client["tpch"]

# Connect to MySQL
engine = create_engine('mysql://root:my-secret-pw@localhost:3306/tpch')

# Load data from MongoDB
df_cust = pd.DataFrame(list(mongo_db.customer.find({}, {"_id":0})))
df_order = pd.DataFrame(list(mongo_db.orders.find({}, {"_id":0})))

# Load data from MySQL
df_order_sql = pd.read_sql_query('SELECT * FROM ORDERS', engine)

# Combine orders from MySQL and MongoDB
df_order = pd.concat([df_order, df_order_sql])

# Perform transformations and the original query
df_cust['CNTRYCODE'] = df_cust['C_PHONE'].str.slice(0, 2)
df_cust = df_cust[df_cust['CNTRYCODE'].isin(['20', '40', '22', '30', '39', '42', '21']) & (df_cust['C_ACCTBAL'] > df_cust.query('C_ACCTBAL > 0 and CNTRYCODE in ["20", "40", "22", "30", "39", "42", "21"]')['C_ACCTBAL'].mean()) & ~df_cust.C_CUSTKEY.isin(df_order["O_CUSTKEY"])]
result = df_cust.groupby('CNTRYCODE').agg(NUMCUST=('CNTRYCODE', 'count'), TOTACCTBAL=('C_ACCTBAL', 'sum')).reset_index()

# Output to CSV
result.to_csv('query_output.csv', index=False)
