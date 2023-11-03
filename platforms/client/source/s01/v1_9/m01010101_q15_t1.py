import pyodbc
import pymongo
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime

# Connect to MySQL
conn_str = (
    r'DRIVER={MySQL ODBC 8.0 Unicode Driver};'
    r'SERVER=mysql;'
    r'DATABASE=tpch;'
    r'USER=root;'
    r'PASSWORD=my-secret-pw;'
)
cnxn = pyodbc.connect(conn_str)

# MongoDB Connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

supplier = pd.DataFrame(list(db.supplier.find({})))
supplier = supplier[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE']]

lineitem = pd.DataFrame(list(db.lineitem.find({"L_SHIPDATE": {"$gte" : datetime.now() - relativedelta(months=3)}})))
lineitem['TOTAL_REVENUE'] = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
revenue0 = lineitem.groupby('L_SUPPKEY').agg({'TOTAL_REVENUE':'sum'}).reset_index()
revenue0.columns = ['SUPPLIER_NO', 'TOTAL_REVENUE']
revenue0['TOTAL_REVENUE'] = revenue0['TOTAL_REVENUE'].astype(float)

merged = pd.merge(supplier, revenue0, how='left', left_on='S_SUPPKEY', right_on='SUPPLIER_NO')
merged['TOTAL_REVENUE'].fillna(0, inplace=True)

max_revenue = merged['TOTAL_REVENUE'].max()
final_df = merged[merged['TOTAL_REVENUE'] == max_revenue]

final_df.to_csv('query_output.csv')

cnxn.close()
client.close()
