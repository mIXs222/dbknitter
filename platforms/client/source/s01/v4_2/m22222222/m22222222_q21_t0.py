import pandas as pd
import redis

def execute_query():
    r = redis.Redis(host='redis', port=6379, db=0)

    supplier = pd.DataFrame(r.get('supplier'))
    lineitem = pd.DataFrame(r.get('lineitem'))
    orders = pd.DataFrame(r.get('orders'))
    nation = pd.DataFrame(r.get('nation'))

    L1 = lineitem.copy()
    L2 = lineitem.copy()
    L3 = lineitem.copy()

    df = supplier.merge(L1, left_on='S_SUPPKEY', right_on='L1.L_SUPPKEY').merge(
        orders, left_on='O_ORDERKEY', right_on='L1.L_ORDERKEY').merge(
        nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

    df = df[(df['O_ORDERSTATUS'] == 'F') & (df['L1.L_RECEIPTDATE'] > df['L1.L_COMMITDATE']) 
            & (df['N_NAME'] == 'SAUDI ARABIA')]

    df2 = df[df['L2.L_ORDERKEY'] == df['L1.L_ORDERKEY'] & (df['L2.L_SUPPKEY'] != df['L1.L_SUPPKEY'])]

    df3 = df2[~((df2['L3.L_ORDERKEY'] == df2['L1.L_ORDERKEY']) & 
                (df2['L3.L_SUPPKEY'] != df2['L1.L_SUPPKEY']) & 
                (df2['L3.L_RECEIPTDATE'] > df2['L3.L_COMMITDATE']))]

    result = df3.groupby('S_NAME').size().reset_index(name='NUMWAIT')
    result.sort_values(['NUMWAIT', 'S_NAME'], ascending=[False, True], inplace=True)

    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    execute_query()
