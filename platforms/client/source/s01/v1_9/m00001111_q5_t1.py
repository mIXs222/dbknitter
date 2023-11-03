import mysql.connector
from pymongo import MongoClient
import pandas as pd
from datetime import datetime

mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

mycursor = mydb.cursor()

mycursor.execute("""
    SELECT * FROM nation
    UNION ALL
    SELECT * FROM region
    UNION ALL
    SELECT * FROM part
    UNION ALL
    SELECT * FROM supplier
""")
mysql_data = pd.DataFrame(mycursor.fetchall())

# For MongoDB
mongo_client = MongoClient('mongodb://mongodb:27017/')

m_db = mongo_client["tpch"]

# Store the tables in MongoDB as dataframes
partsupp_data = pd.DataFrame(list(m_db.partsupp.find()))
customer_data = pd.DataFrame(list(m_db.customer.find()))
orders_data = pd.DataFrame(list(m_db.orders.find()))
lineitem_data = pd.DataFrame(list(m_db.lineitem.find()))

# Merge all dataframes, this is essentially what the initial query does.
all_data = pd.concat([mysql_data, partsupp_data, customer_data, orders_data, lineitem_data], axis=1)

all_data = all_data[
  (all_data['C_CUSTKEY'] == all_data['O_CUSTKEY']) & 
  (all_data['L_ORDERKEY'] == all_data['O_ORDERKEY']) &
  (all_data['L_SUPPKEY'] == all_data['S_SUPPKEY']) &
  (all_data['C_NATIONKEY'] == all_data['S_NATIONKEY']) &
  (all_data['S_NATIONKEY'] == all_data['N_NATIONKEY']) &
  (all_data['N_REGIONKEY'] == all_data['R_REGIONKEY']) &
  (all_data['R_NAME'] == 'ASIA') &
  (all_data['O_ORDERDATE'] >= datetime.strptime('1990-01-01', '%Y-%m-%d')) &
  (all_data['O_ORDERDATE'] < datetime.strptime('1995-01-01', '%Y-%m-%d'))
]

grouped_data = all_data.groupby(['N_NAME']).sum()

grouped_data['REVENUE'] = grouped_data['L_EXTENDEDPRICE'] * (1 - grouped_data['L_DISCOUNT'])

grouped_data = grouped_data.sort_values(by=['REVENUE'], ascending=False)[['REVENUE']]

grouped_data.to_csv(r'query_output.csv')
