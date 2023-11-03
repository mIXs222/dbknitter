
import pandas as pd
import pymysql
from pymongo import MongoClient
from pandas.io.json import json_normalize

#Establish connection for MySQL 
mysql_conn = pymysql.connect(
  host='mysql',
  user='root',
  password='my-secret-pw',
  db='tpch'
)

# Query for MySQL DB

sql_query = '''SELECT
    C_CUSTKEY, C_NAME, C_ACCTBAL, C_ADDRESS, C_PHONE, C_COMMENT, N_NAME
    FROM customer, nation WHERE C_NATIONKEY = N_NATIONKEY'''

mysql_df = pd.read_sql_query(sql_query, mysql_conn)

# Establish connection for MongoDB

mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Fetch the data from MongoDB
orders = list(mongo_db.orders.find({'O_ORDERDATE':{'$gte':'1993-10-01', 
                                                          '$lt':'1994-01-01'}}))
lineitems = list(mongo_db.lineitem.find({'L_RETURNFLAG':'R'}))

# Convert fetched data to dataframe

orders_df = json_normalize(orders)
lineitem_df = json_normalize(lineitems)

# Merge dataframes
df = pd.merge(orders_df, lineitem_df, how='inner', left_on='O_ORDERKEY', 
                   right_on='L_ORDERKEY')
df = pd.merge(df, mysql_df, how='inner', left_on='O_CUSTKEY', 
                   right_on='C_CUSTKEY')
# Perform the query

df['REVENUE'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])
group_keys = ['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 
              'N_NAME', 'C_ADDRESS', 'C_COMMENT']

df = df.groupby(group_keys).sum().reset_index()

df = df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False,True,True,False])

# Save to file
df.to_csv('query_output.csv', index=False)

