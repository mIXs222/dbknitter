import pandas as pd
from pymongo import MongoClient
import mysql.connector

# MySQL Connection
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

mycursor = mydb.cursor()

# MongoDB Connection
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Querying MySQL
mycursor.execute("SELECT * FROM NATION")
nation = pd.DataFrame(mycursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

mycursor.execute("SELECT * FROM PART")
part = pd.DataFrame(mycursor.fetchall(), columns=['P_PARTKEY', 'P_NAME', 'P_MFGR',
                                                  'P_BRAND', 'P_TYPE', 'P_SIZE', 
                                                  'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

mycursor.execute("SELECT * FROM SUPPLIER")
supplier = pd.DataFrame(mycursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 
                                                      'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Querying MongoDB
partsupp = pd.DataFrame(list(db.partsupp.find({}, {'_id': 0})))
orders = pd.DataFrame(list(db.orders.find({}, {'_id': 0})))
lineitem = pd.DataFrame(list(db.lineitem.find({}, {'_id': 0})))

df = (part.merge(supplier, left_on='P_PARTKEY', right_on='S_SUPPKEY')
        .merge(lineitem, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
        .merge(partsupp, left_on=['S_SUPPKEY', 'P_PARTKEY'], right_on=['PS_SUPPKEY', 'PS_PARTKEY'])
        .merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
        .merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY'))

df = df[df['P_NAME'].str.contains('dim')]

df['O_YEAR'] = pd.DatetimeIndex(df['O_ORDERDATE']).year
df['AMOUNT'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT']) - df['PS_SUPPLYCOST'] * df['L_QUANTITY']

grouped = df.groupby(['N_NAME', 'O_YEAR'])['AMOUNT'].sum().reset_index()

grouped.columns = ['NATION', 'O_YEAR', 'SUM_PROFIT']

grouped.sort_values(by=['NATION', 'O_YEAR'], ascending=[True, False], inplace=True)

grouped.to_csv('query_output.csv', index=False)

mycursor.close()
mydb.close()
