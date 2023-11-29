# query.py
import pandas as pd
from direct_redis import DirectRedis

def perform_query():
    # Connect to the redis database
    r = DirectRedis(host='redis', port=6379, db=0)

    # Reading the "lineitem" table as a pandas DataFrame from Redis
    lineitem_data = r.get('lineitem')
    lineitem_df = pd.read_json(lineitem_data)
    
    # Filtering the DataFrame based on the query's conditions
    filtered_df = lineitem_df[
        (lineitem_df['L_SHIPDATE'] > '1994-01-01') &
        (lineitem_df['L_SHIPDATE'] < '1995-01-01') &
        (lineitem_df['L_DISCOUNT'] >= (0.06 - 0.01)) &
        (lineitem_df['L_DISCOUNT'] <= (0.06 + 0.01)) &
        (lineitem_df['L_QUANTITY'] < 24)
    ]

    # Calculating the revenue
    filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']
    result_df = pd.DataFrame({
        'REVENUE': [filtered_df['REVENUE'].sum()]
    })

    # Writing the result to a CSV file
    result_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    perform_query()
