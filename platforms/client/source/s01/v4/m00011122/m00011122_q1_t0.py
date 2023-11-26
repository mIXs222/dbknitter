import pandas as pd
import direct_redis

def query_redis_and_save():
    # Connection information
    redis_host = 'redis'
    redis_port = 6379
    redis_db = 0

    # Connect to Redis using Direct Redis
    client = direct_redis.DirectRedis(host=redis_host, port=redis_port, db=redis_db)

    # Get the lineitem dataframe
    lineitem_df = pd.read_json(client.get('lineitem'))
    
    # Ensure columns are of the correct data type
    lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
    
    # Perform the query on the dataframe
    result = lineitem_df.loc[lineitem_df['L_SHIPDATE'] <= '1998-09-02'].groupby(
        ['L_RETURNFLAG', 'L_LINESTATUS']).agg(
        SUM_QTY=('L_QUANTITY', 'sum'),
        SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
        SUM_DISC_PRICE=(lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum()),
        SUM_CHARGE=(lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']) * (1 + x['L_TAX'])).sum()),
        AVG_QTY=('L_QUANTITY', 'mean'),
        AVG_PRICE=('L_EXTENDEDPRICE', 'mean'),
        AVG_DISC=('L_DISCOUNT', 'mean'),
        COUNT_ORDER=('L_ORDERKEY', 'count')
    ).reset_index().sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

    # Output the result to a csv file
    result.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    query_redis_and_save()
