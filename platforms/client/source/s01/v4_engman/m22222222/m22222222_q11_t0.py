import pandas as pd
from direct_redis import DirectRedis

def important_stock_query():
    # Connect to the Redis database
    redis_client = DirectRedis(host='redis', port=6379, db=0)

    # Retrieve data from Redis
    nation_df = pd.read_json(redis_client.get('nation'), orient='records')
    supplier_df = pd.read_json(redis_client.get('supplier'), orient='records')
    partsupp_df = pd.read_json(redis_client.get('partsupp'), orient='records')

    # Merge the nation and supplier dataframes on N_NATIONKEY
    merged_df = pd.merge(supplier_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

    # Filter for suppliers in Germany
    german_suppliers_df = merged_df[merged_df['N_NAME'] == 'GERMANY']

    # Merge with partsupp to get the stock
    german_stock_df = pd.merge(german_suppliers_df, partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

    # Calculate the total value
    german_stock_df['TOTAL_VALUE'] = german_stock_df['PS_AVAILQTY'] * german_stock_df['PS_SUPPLYCOST']

    # Find the total value of all parts
    total_value = german_stock_df['TOTAL_VALUE'].sum()

    # Find parts that represent a significant percentage of the total value
    significant_parts = german_stock_df[german_stock_df['TOTAL_VALUE'] > (total_value * 0.0001)]

    # Select the required columns and sort by value in descending order
    significant_parts = significant_parts[['PS_PARTKEY', 'TOTAL_VALUE']].sort_values(by='TOTAL_VALUE', ascending=False)

    # Write the output to a CSV file
    significant_parts.to_csv('query_output.csv', index=False)

# Run the query function
if __name__ == '__main__':
    important_stock_query()
