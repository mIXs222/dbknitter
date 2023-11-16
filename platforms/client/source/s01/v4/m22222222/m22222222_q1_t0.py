import pandas as pd
import direct_redis
import datetime

def query_redis():
    # Connection to the redis database
    client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Retrieve the 'lineitem' table data
    lineitem_data = client.get('lineitem')
    
    # Convert the retrieved data to a DataFrame
    lineitem_df = pd.read_json(lineitem_data)

    # Perform the query on the DataFrame
    output_df = lineitem_df.loc[lineitem_df['L_SHIPDATE'] <= datetime.date(1998, 9, 2)].groupby(
        ['L_RETURNFLAG', 'L_LINESTATUS']).agg(
        SUM_QTY=pd.NamedAgg(column='L_QUANTITY', aggfunc='sum'),
        SUM_BASE_PRICE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='sum'),
        SUM_DISC_PRICE=pd.NamedAgg(column='L_EXTENDEDPRICE',
                                   aggfunc=lambda x: (x * (1 - lineitem_df['L_DISCOUNT'])).sum()),
        SUM_CHARGE=pd.NamedAgg(column='L_EXTENDEDPRICE',
                               aggfunc=lambda x: (x * (1 - lineitem_df['L_DISCOUNT']) * (1 + lineitem_df['L_TAX'])).sum()),
        AVG_QTY=pd.NamedAgg(column='L_QUANTITY', aggfunc='mean'),
        AVG_PRICE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='mean'),
        AVG_DISC=pd.NamedAgg(column='L_DISCOUNT', aggfunc='mean'),
        COUNT_ORDER=pd.NamedAgg(column='L_ORDERKEY', aggfunc='count')
    ).reset_index().sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

    # Write the result to 'query_output.csv'
    output_df.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    query_redis()
