import pandas as pd
from pymongo import MongoClient
import mysql.connector

def mongodb_to_dataframe(host, port, username, password, db, collection, query={}, no_id=True):
    """ Create a Pandas DataFrame from MongoDB collection """
    client = MongoClient(host, port)
    db = client[db]
    cursor = db[collection].find(query)
    df = pd.DataFrame(list(cursor))
    if no_id: del df['_id']
    return df

def mysql_to_dataframe(user, password, host, database, query):
    """ Create a Pandas DataFrame from MySQL query"""
    cnx = mysql.connector.connect(user=user, password=password,
                                 host=host, database=database)
    df = pd.read_sql_query(query, con=cnx)
    return df

# Let's fetch data from MongoDB
customer_mongo = mongodb_to_dataframe(
    host='mongodb', 
    port=27017, 
    username='', 
    password='', 
    db='tpch',
    collection='customer'
)

orders_mongo = mongodb_to_dataframe(
    host='mongodb', 
    port=27017, 
    username='', 
    password='', 
    db='tpch',
    collection='orders'
)

# Let's fetch data from MySQL
# Replace 'mysql_host', 'user', 'password', 'database' and 'query' with your actual MySQL details and query
customer_mysql = mysql_to_dataframe(
    user='', 
    password='', 
    host='mysql_host', 
    database='database', 
    query='SELECT * FROM customer'
)

orders_mysql = mysql_to_dataframe(
    user='', 
    password='', 
    host='mysql_host', 
    database='database', 
    query='SELECT * FROM orders'
)

# Concatenate MongoDB and MySQL data
customer_df = pd.concat([customer_mongo, customer_mysql])
orders_df = pd.concat([orders_mongo, orders_mysql])

# Employee the provided query
custsale = customer_df[customer_df['C_PHONE'].str[:2].isin(['20', '40', '22', '30', '39', '42', '21']) & (customer_df['C_ACCTBAL'] > customer_df[customer_df['C_ACCTBAL'] > 0.00]['C_ACCTBAL'].mean()) & (~customer_df['C_CUSTKEY'].isin(orders_df['O_CUSTKEY']))]
custsale['CNTRYCODE'] = custsale['C_PHONE'].str[:2]
result = custsale.groupby('CNTRYCODE')['C_ACCTBAL'].agg(NUMCUST='count', TOTACCTBAL='sum')
result.reset_index().to_csv("query_output.csv", index=False)
