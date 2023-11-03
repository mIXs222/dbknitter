from pymongo import MongoClient
import pandas as pd
from pandas.io.json import json_normalize
import datetime

client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

def get_data(collection):
    data = db[collection].find()
    return pd.json_normalize(data)

def main():
    customer = get_data('customer')
    orders = get_data('orders')
    lineitem = get_data('lineitem')

    merged_df = pd.merge(customer, orders, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    merged_df = pd.merge(merged_df, lineitem, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    filtered_df = merged_df[(merged_df['C_MKTSEGMENT'] == 'BUILDING') & 
                            (merged_df['O_ORDERDATE'] < datetime.datetime(1995, 3, 15)) &
                            (merged_df['L_SHIPDATE'] > datetime.datetime(1995, 3, 15))]

    grouped_df = filtered_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
    grouped_df['REVENUE'] = grouped_df['L_EXTENDEDPRICE'] * (1 - grouped_df['L_DISCOUNT'])
    final_df = grouped_df[['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']].sort_values(['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

    final_df.to_csv('query_output.csv')

if __name__ == "__main__":
    main()
