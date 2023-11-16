import pandas as pd
from direct_redis import DirectRedis

def execute_query():
    # Connect to Redis database
    redis_db = DirectRedis(host='redis', port=6379, db=0)

    # Read data from Redis
    part_df = pd.read_json(redis_db.get('part'))
    lineitem_df = pd.read_json(redis_db.get('lineitem'))

    # Merge the dataframes where part keys are the same
    merged_df = part_df.merge(lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

    # Filter the merged dataframe by brand and container
    filtered_df = merged_df[
        (merged_df['P_BRAND'] == 'Brand#23') &
        (merged_df['P_CONTAINER'] == 'MED BAG')
    ]

    # Calculate the average quantity of lineitem per part
    avg_quantity = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
    avg_quantity['avg_qty_20_pct'] = 0.2 * avg_quantity['L_QUANTITY']

    # Merge with the filtered dataframe on part key and apply the quantity filter
    final_df = filtered_df.merge(avg_quantity, left_on='P_PARTKEY', right_on='L_PARTKEY')
    final_df = final_df[final_df['L_QUANTITY_x'] < final_df['avg_qty_20_pct']]

    # Calculate the result of the query
    avg_yearly = final_df['L_EXTENDEDPRICE'].sum() / 7.0

    # Save the result to a CSV file
    pd.DataFrame({'AVG_YEARLY': [avg_yearly]}).to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    execute_query()
