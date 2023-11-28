# query.py
import pandas as pd
import direct_redis

def main():
    # Connect to the Redis instance
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Retrieve the lineitem table from Redis as a pandas DataFrame
    lineitem_data_json = r.get('lineitem')
    lineitem_df = pd.read_json(lineitem_data_json, orient='records')

    # Filter the DataFrame according to the given conditions
    filtered_lineitem_df = lineitem_df[
        (lineitem_df['L_SHIPDATE'] >= '1994-01-01') &
        (lineitem_df['L_SHIPDATE'] <= '1994-12-31') &
        (lineitem_df['L_DISCOUNT'] >= 0.05) &
        (lineitem_df['L_DISCOUNT'] <= 0.07) &
        (lineitem_df['L_QUANTITY'] < 24)
    ]

    # Calculate the total revenue with the specified discount criteria applied
    filtered_lineitem_df['REVENUE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])
    total_revenue = filtered_lineitem_df['REVENUE'].sum()

    # Write the result to query_output.csv
    with open('query_output.csv', 'w') as f:
        f.write(f"Total Revenue,{total_revenue}\n")

if __name__ == "__main__":
    main()
