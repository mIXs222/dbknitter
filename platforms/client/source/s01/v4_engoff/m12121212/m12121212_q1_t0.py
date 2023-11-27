import pandas as pd
from direct_redis import DirectRedis

def execute_query():
    # Connect to Redis
    r = DirectRedis(host='redis', port=6379, db=0)
    
    # Fetch lineitem table
    lineitem_str = r.get('lineitem')
    lineitem_df = pd.read_json(lineitem_str)
    
    # Filter records before the given date
    filtered_df = lineitem_df[lineitem_df['L_SHIPDATE'] < '1998-09-02']
    
    # Aggregate functions
    result_df = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
        QUANTITY_TOTAL=pd.NamedAgg(column='L_QUANTITY', aggfunc='sum'),
        EXTENDED_PRICE_TOTAL=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='sum'),
        DISCOUNT_PRICE_TOTAL=pd.NamedAgg(column='L_DISCOUNT', aggfunc=lambda x: (x*filtered_df['L_EXTENDEDPRICE']).sum()),
        DISCOUNT_TAX_PRICE_TOTAL=pd.NamedAgg(column='L_DISCOUNT', aggfunc=lambda x: (x*filtered_df['L_EXTENDEDPRICE']*(1+filtered_df['L_TAX'])).sum()),
        AVG_QTY=pd.NamedAgg(column='L_QUANTITY', aggfunc='mean'),
        AVG_PRICE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='mean'),
        AVG_DISCOUNT=pd.NamedAgg(column='L_DISCOUNT', aggfunc='mean'),
        COUNT_ORDER=pd.NamedAgg(column='L_ORDERKEY', aggfunc='count')
    )
    
    # Sort by RETURNFLAG and LINESTATUS
    result_df = result_df.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

    # Write to CSV
    result_df.to_csv('query_output.csv')

if __name__ == "__main__":
    execute_query()
