#!/usr/bin/python
import pandas as pd
import redis
from pandas.io.json import json_normalize

def get_data_from_redis(db_num, table_name):
    r = redis.Redis(host='redis', port=6379, db=db_num)
    data = r.get(table_name)
    return pd.DataFrame(data)

def process_query():
    # Load data
    df_customer = get_data_from_redis(0, 'customer')
    df_orders = get_data_from_redis(0, 'orders')

    # Filter data
    df_customer['CNTRYCODE'] = df_customer['C_PHONE'].str.slice(0,2)
    df_customer = df_customer[df_customer['CNTRYCODE'].isin(['20', '40', '22', '30', '39', '42', '21'])]
    avg_acctbal = df_customer[(df_customer['C_ACCTBAL'] > 0.00)]['C_ACCTBAL'].mean()
    df_customer = df_customer[df_customer['C_ACCTBAL'] > avg_acctbal]
    df_customer = df_customer[~df_customer['C_CUSTKEY'].isin(df_orders['O_CUSTKEY'])]

    # Aggregate data
    result = df_customer.groupby('CNTRYCODE').agg({'C_CUSTKEY':'count', 'C_ACCTBAL':'sum'}).reset_index()
    result.columns = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']

    # Order data
    result = result.sort_values('CNTRYCODE')

    # Write to file
    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    process_query()
