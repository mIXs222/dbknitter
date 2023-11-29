import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
import csv

# Connection information
redis_host = 'redis'
redis_port = 6379
redis_db_name = 0

def query_redis():
    try:
        # Connect to Redis using the direct_redis library
        r = DirectRedis(host=redis_host, port=redis_port, db=redis_db_name)

        # Retrieve the lineitem table
        lineitem_data = r.get('lineitem')

        # Convert the retrieved data to a Pandas DataFrame
        lineitem_df = pd.read_json(lineitem_data)

        # Filter data for lineitems shipped before 1998-09-02
        lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
        filtered_df = lineitem_df[lineitem_df['L_SHIPDATE'] < datetime(1998, 9, 2)]

        # Calculate required aggregates
        summary_df = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
            Total_Quantity=('L_QUANTITY', 'sum'),
            Total_ExtendedPrice=('L_EXTENDEDPRICE', 'sum'),
            Total_Disc_Price=('L_EXTENDEDPRICE', lambda x: sum(x * (1 - filtered_df['L_DISCOUNT']))),
            Total_Disc_Price_Plus_Tax=('L_EXTENDEDPRICE', lambda x: sum(x * (1 - filtered_df['L_DISCOUNT']) * (1 + filtered_df['L_TAX']))),
            Avg_Quantity=('L_QUANTITY', 'mean'),
            Avg_ExtendedPrice=('L_EXTENDEDPRICE', 'mean'),
            Avg_Discount=('L_DISCOUNT', 'mean'),
            Count_Order=('L_ORDERKEY', 'count')
        ).reset_index()

        # Sort the results
        summary_df = summary_df.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

        # Write the DataFrame to a CSV file
        summary_df.to_csv('query_output.csv', index=False)

    except Exception as e:
        print(f"An error occurred: {e}")

# Execute the query
query_redis()
