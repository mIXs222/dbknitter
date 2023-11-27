import pandas as pd
from direct_redis import DirectRedis

def query_and_save_to_csv():
    # Connect to the Redis database
    r = DirectRedis(host='redis', port=6379, db=0)
    
    # Get the 'lineitem' table from Redis as a string and convert it to a Pandas DataFrame
    lineitem_data = r.get('lineitem')
    lineitem_df = pd.read_json(lineitem_data)
        
    # Perform the query on the dataframe
    filtered_df = lineitem_df[
        (lineitem_df['L_SHIPDATE'] >= '1994-01-01') &
        (lineitem_df['L_SHIPDATE'] < '1995-01-01') &
        (lineitem_df['L_DISCOUNT'] >= 0.05) &
        (lineitem_df['L_DISCOUNT'] <= 0.07) &
        (lineitem_df['L_QUANTITY'] < 24)
    ]

    # Calculate the revenue
    filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']
    
    # Sum the revenue
    result_df = filtered_df[['REVENUE']].sum().to_frame().T
    
    # Save the result to CSV
    result_df.to_csv('query_output.csv', index=False)


if __name__ == '__main__':
    query_and_save_to_csv()
