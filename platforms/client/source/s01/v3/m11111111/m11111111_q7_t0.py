import pandas as pd
import pymongo
from datetime import datetime

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["tpch"]

# Fetch tables from MongoDB
nation = pd.DataFrame(list(db.nation.find()))
supplier = pd.DataFrame(list(db.supplier.find()))
customer = pd.DataFrame(list(db.customer.find()))
orders = pd.DataFrame(list(db.orders.find()))
lineitem = pd.DataFrame(list(db.lineitem.find()))

# Merge tables
merge1 = pd.merge(supplier, lineitem, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
merge2 = pd.merge(merge1, orders, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merge3 = pd.merge(merge2, customer, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
nation1 = nation.rename(columns={'N_NATIONKEY': 'S_NATIONKEY', 'N_NAME': 'SUPP_NATION'})
nation2 = nation.rename(columns={'N_NATIONKEY': 'C_NATIONKEY', 'N_NAME': 'CUST_NATION'})
merge4 = pd.merge(merge3, nation1, on='S_NATIONKEY')
final_merge = pd.merge(merge4, nation2, on='C_NATIONKEY')

# Apply conditions and compute VOLUME
final_merge = final_merge[((final_merge['SUPP_NATION']=='JAPAN') & (final_merge['CUST_NATION']=='INDIA')) | 
                          ((final_merge['SUPP_NATION']=='INDIA') & (final_merge['CUST_NATION']=='JAPAN'))]
final_merge['L_SHIPDATE'] = pd.to_datetime(final_merge['L_SHIPDATE'])
start_date = pd.to_datetime('1995-01-01')
end_date = pd.to_datetime('1996-12-31')
final_merge = final_merge[(final_merge['L_SHIPDATE']>=start_date) & (final_merge['L_SHIPDATE']<=end_date)]
final_merge['L_YEAR'] = pd.DatetimeIndex(final_merge['L_SHIPDATE']).year
final_merge['VOLUME'] = final_merge['L_EXTENDEDPRICE'] * (1 - final_merge['L_DISCOUNT'])

# Perform groupby and write output to CSV
output = final_merge.groupby(['SUPP_NATION','CUST_NATION','L_YEAR'])['VOLUME'].sum().reset_index()
output.to_csv('query_output.csv', index=False)
