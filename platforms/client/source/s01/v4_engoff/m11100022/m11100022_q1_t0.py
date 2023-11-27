# query.py
import pandas as pd
from direct_redis import DirectRedis

def load_data_from_redis(host, port, dbname, tablename):
    dr = DirectRedis(host=host, port=port, db=dbname)
    df = pd.read_json(dr.get(tablename))
    return df

def main():
    redis_host = "redis"
    redis_port = 6379
    redis_dbname = 0
    tablename = "lineitem"

    df_lineitem = load_data_from_redis(redis_host, redis_port, redis_dbname, tablename)

    df_filtered = df_lineitem[df_lineitem['L_SHIPDATE'] < '1998-09-02']
    
    # Calculate the aggregated columns
    df_summary = df_filtered.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
        Total_Quantity=('L_QUANTITY', 'sum'),
        Total_ExtendedPrice=('L_EXTENDEDPRICE', 'sum'),
        Total_DiscPrice=('L_EXTENDEDPRICE', lambda x: (x * (1 - df_filtered.loc[x.index, 'L_DISCOUNT'])).sum()),
        Total_DiscPrice_Plus_Tax=('L_EXTENDEDPRICE', lambda x: (x * (1 - df_filtered.loc[x.index, 'L_DISCOUNT']) * (1 + df_filtered.loc[x.index, 'L_TAX'])).sum()),
        Avg_Quantity=('L_QUANTITY', 'mean'),
        Avg_ExtendedPrice=('L_EXTENDEDPRICE', 'mean'),
        Avg_Discount=('L_DISCOUNT', 'mean'),
        Lineitem_Count=('L_ORDERKEY', 'count')
    )

    df_summary.reset_index(inplace=True)
    df_summary.sort_values(['L_RETURNFLAG', 'L_LINESTATUS'], ascending=True, inplace=True)

    df_summary.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
