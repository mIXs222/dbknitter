from pymongo import MongoClient
from datetime import datetime
import pandas as pd

client = MongoClient('mongodb', 27017)
db = client.tpch

nation_df = pd.DataFrame(list(db.nation.find({}, {"_id":0})))
supplier_df = pd.DataFrame(list(db.supplier.find({}, {"_id":0})))
customer_df = pd.DataFrame(list(db.customer.find({}, {"_id":0})))
orders_df = pd.DataFrame(list(db.orders.find({}, {"_id":0})))
lineitem_df = pd.DataFrame(list(db.lineitem.find({}, {"_id":0})))

supp_nation_df = supplier_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY', suffixes=('_supp', '_nation'))
cust_orders_df = customer_df.merge(orders_df, on='C_CUSTKEY')
shipping_df = lineitem_df.merge(supp_nation_df, on='S_SUPPKEY').merge(cust_orders_df, on='O_ORDERKEY')

shipping_df['L_YEAR'] = shipping_df['L_SHIPDATE'].apply(lambda x: x.year)
shipping_df['VOLUME'] = shipping_df['L_EXTENDEDPRICE'] * (1 - shipping_df['L_DISCOUNT'])

filter_countries = ((shipping_df['N_NAME_supp'] == 'JAPAN') & (shipping_df['N_NAME'] == 'INDIA')) | \
                   ((shipping_df['N_NAME_supp'] == 'INDIA') & (shipping_df['N_NAME'] == 'JAPAN'))

filter_dates = (shipping_df['L_SHIPDATE'] >= datetime(1995, 1, 1)) & (shipping_df['L_SHIPDATE'] <= datetime(1996, 12, 31))
shipping_df = shipping_df[filter_countries & filter_dates]

result_df = shipping_df.groupby(['N_NAME_supp', 'N_NAME', 'L_YEAR'])['VOLUME'].sum().reset_index()
result_df.columns = ['SUPP_NATION', 'CUST_NATION', 'L_YEAR', 'REVENUE']
result_df = result_df.sort_values(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

result_df.to_csv('query_output.csv', index=False)
