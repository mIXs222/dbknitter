from pymongo import MongoClient
from dateutil import parser
from datetime import datetime
import pandas as pd

client = MongoClient('mongodb', 27017)
db = client['tpch']

# Fetch records from orders and lineitem collections and convert them into pandas dataframe
orders_df = pd.DataFrame(list(db.orders.find({},{'_id': 0})))
lineitem_df = pd.DataFrame(list(db.lineitem.find({},{'_id': 0})))

final_df = pd.merge(orders_df, lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')
final_df[['O_ORDERDATE', 'L_COMMITDATE', 'L_SHIPDATE', 'L_RECEIPTDATE']] = final_df[
    ['O_ORDERDATE', 'L_COMMITDATE', 'L_SHIPDATE', 'L_RECEIPTDATE']].apply(pd.to_datetime, format='%Y-%m-%d')

date_filter = (final_df['L_COMMITDATE'] < final_df['L_RECEIPTDATE']) & (final_df['L_SHIPDATE'] < 
                final_df['L_COMMITDATE']) & (final_df['L_RECEIPTDATE'] >= '1994-01-01') & (
                final_df['L_RECEIPTDATE'] < '1995-01-01') & final_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])

result_filterd_data = final_df[date_filter]

result_filterd_data['order_priority'] = result_filterd_data['O_ORDERPRIORITY'].apply(lambda x: 'HIGH' if x in [
                                                                                                             '1-URGENT',
                                                                                                             '2-HIGH'] else 'LOW')

groupby_result = result_filterd_data.groupby(['L_SHIPMODE', 'order_priority']).size().reset_index(level=[0,1])
groupby_result.columns = ['L_SHIPMODE', 'order_priority', 'count']
result = groupby_result.pivot_table(index='L_SHIPMODE', columns='order_priority', values='count', fill_value=0)
result = result.reset_index()

result.columns.name = None
result.to_csv('query_output.csv',sep=',',index=False)
