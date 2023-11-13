from pymongo import MongoClient
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def get_data_from_mongodb():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['tpch']
    customers = pd.DataFrame(list(db.customer.find()))
    orders = pd.DataFrame(list(db.orders.find()))
    lineitem = pd.DataFrame(list(db.lineitem.find()))
    nation = pd.DataFrame(list(db.nation.find()))
    
    return customers, orders, lineitem, nation

def join_data(customers, orders, lineitem, nation):
    df = pd.merge(customers, orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    df = pd.merge(df, lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    df = pd.merge(df, nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
    df['O_ORDERDATE'] = pd.to_datetime(df['O_ORDERDATE'])
    start_date = '1993-10-01'
    end_date = '1994-01-01'
    mask = (df['O_ORDERDATE'] >= start_date) & (df['O_ORDERDATE'] < end_date)
    df = df.loc[mask]
    df = df[df['L_RETURNFLAG'] == 'R']
    df['REVENUE'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])
    df = df[['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]
    df = df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT']).agg(np.sum)
    df = df.sort_values(['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending = [False,True,True,False])
    
    return df

def save_to_csv(df):
    df.to_csv('query_output.csv')

def main():
    customers, orders, lineitem, nation = get_data_from_mongodb()
    df = join_data(customers, orders, lineitem, nation)
    save_to_csv(df)

if __name__ == "__main__":
    main()
