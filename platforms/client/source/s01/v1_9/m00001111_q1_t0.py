import mysql.connector
from pymongo import MongoClient
import pandas as pd
from pandas import DataFrame


# Create a connection to MySQL database
mysql_conn = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# create a cursor object
mysql_cur = mysql_conn.cursor()

# get nation, region, part and supplier data from MySQL
mysql_cur.execute("SELECT * FROM NATION")
nation = mysql_cur.fetchall()

mysql_cur.execute("SELECT * FROM REGION")
region = mysql_cur.fetchall()

mysql_cur.execute("SELECT * FROM PART")
part = mysql_cur.fetchall()

mysql_cur.execute("SELECT * FROM SUPPLIER")
supplier = mysql_cur.fetchall()

mysql_conn.close()

# convert MySQL data into pandas dataframes
df_nation = DataFrame(nation,columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])
df_region = DataFrame(region,columns=['R_REGIONKEY', 'R_NAME', 'R_COMMENT'])
df_part = DataFrame(part, columns=['P_PARTKEY','P_NAME','P_MFGR','P_BRAND','P_TYPE','P_SIZE','P_CONTAINER','P_RETAILPRICE','P_COMMENT'])
df_supplier = DataFrame(supplier,columns=['S_SUPPKEY','S_NAME','S_ADDRESS','S_NATIONKEY','S_PHONE','S_ACCTBAL','S_COMMENT'])

# connect to MongoDB and get collections
mclient = MongoClient('mongodb://mongodb:27017/')
mdb = mclient['tpch']
psupp_coll = mdb['partsupp']
cust_coll = mdb['customer']
orders_coll = mdb['orders']
lineitem_coll = mdb['lineitem']

# convert collection data into pandas dataframes
df_partsupp = DataFrame(list(psupp_coll.find()))
df_customer = DataFrame(list(cust_coll.find()))
df_orders = DataFrame(list(orders_coll.find()))
df_lineitem = DataFrame(list(lineitem_coll.find()))


# filter lineitems by shipdate
df_lineitem = df_lineitem[df_lineitem['L_SHIPDATE'] <= '1998-09-02']

# perform groupby operation on filtered data 
result = df_lineitem.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
    {
        'L_QUANTITY': ['sum', 'mean'],
        'L_EXTENDEDPRICE': ['sum', 'mean'],
        'L_DISCOUNT': 'mean',
        'L_ORDERKEY': 'count',
        'L_EXTENDEDPRICE': lambda x: (x * (1 - df_lineitem.loc[x.index, 'L_DISCOUNT'])).sum(),
        'L_EXTENDEDPRICE': lambda x: (x * (1 - df_lineitem.loc[x.index, 'L_DISCOUNT']) * (1 + df_lineitem.loc[x.index, 'L_TAX'])).sum()
    }
)

# rename columns for clarity
result.columns = ['SUM_QTY', 'AVG_QTY', 'SUM_BASE_PRICE', 'AVG_PRICE', 'AVG_DISC','COUNT_ORDER', 'SUM_DISC_PRICE', 'SUM_CHARGE']
result.reset_index()

# write to csv file
result.to_csv('query_output.csv', index=False)
