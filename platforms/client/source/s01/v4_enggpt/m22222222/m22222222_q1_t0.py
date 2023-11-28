import pandas as pd
import direct_redis
from datetime import datetime

def main():
    connection_info = {
        'host': 'redis',
        'port': 6379,
        'db': 0
    }
    
    # Create a connection to the Redis database
    redis_connection = direct_redis.DirectRedis(**connection_info)

    # Retrieve the DataFrame from Redis
    lineitem_df = pd.read_json(redis_connection.get('lineitem'), orient='records')
    
    # Perform analysis following the user's provided SQL-like description
    # Filter rows where the shipping date is on or before '1998-09-02'
    lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
    filtered_df = lineitem_df[lineitem_df['L_SHIPDATE'] <= datetime(1998, 9, 2)]
    
    # Group by the return flag and line status
    aggregated_results = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
        SUM_QTY=('L_QUANTITY', 'sum'),
        SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
        SUM_DISC_PRICE=('L_EXTENDEDPRICE', lambda x: (x * (1 - filtered_df.loc[x.index, 'L_DISCOUNT'])).sum()),
        SUM_CHARGE=('L_EXTENDEDPRICE', lambda x: (x * (1 - filtered_df.loc[x.index, 'L_DISCOUNT'] + filtered_df.loc[x.index, 'L_TAX'])).sum()),
        AVG_QTY=('L_QUANTITY', 'mean'),
        AVG_PRICE=('L_EXTENDEDPRICE', 'mean'),
        AVG_DISC=('L_DISCOUNT', 'mean'),
        COUNT_ORDER=('L_ORDERKEY', 'count')
    ).reset_index()

    # Sort the results by return flag and line status
    sorted_results = aggregated_results.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

    # Write the results to a CSV file
    sorted_results.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
