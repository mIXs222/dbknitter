from pymongo import MongoClient
import pandas as pd

def query_mongodb():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['tpch']
    table = db['lineitem']

    cursor = table.find({"L_SHIPDATE": {"$lte": pd.Timestamp("1998-09-02")}})
    df =  pd.DataFrame(list(cursor))
    
    # group by 'L_RETURNFLAG' and 'L_LINESTATUS'
    df = df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
        SUM_QTY=('L_QUANTITY', 'sum'),
        SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
        SUM_DISC_PRICE=('L_EXTENDEDPRICE', lambda x: (x * (1 - df['L_DISCOUNT'])).sum()),
        SUM_CHARGE=('L_EXTENDEDPRICE', lambda x: (x * (1 - df['L_DISCOUNT']) * (1 + df['L_TAX'])).sum()),
        AVG_QTY=('L_QUANTITY', 'mean'),
        AVG_PRICE=('L_EXTENDEDPRICE', 'mean'),
        AVG_DISC=('L_DISCOUNT', 'mean'),
        COUNT_ORDER=('L_ORDERKEY', 'count')
    ).reset_index()

    df.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'], inplace=True)
    df.to_csv('query_output.csv')

if __name__ == "__main__":
    query_mongodb()
