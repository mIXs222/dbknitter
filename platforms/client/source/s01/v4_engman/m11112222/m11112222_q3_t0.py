# shipping_priority_query.py
import pandas as pd
from direct_redis import DirectRedis

def retrieve_and_process_data():
    # Initialize the connection to the Redis database
    redis_client = DirectRedis(host='redis', port=6379, db='0')

    # Retrieve data tables from Redis as Pandas DataFrame
    customer_df = pd.DataFrame(eval(redis_client.get('customer')))
    orders_df = pd.DataFrame(eval(redis_client.get('orders')))
    lineitem_df = pd.DataFrame(eval(redis_client.get('lineitem')))

    # Convert date strings to datetime objects for comparison
    orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
    lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

    # Filter data according to the query conditions
    filtered_orders = orders_df[
        (orders_df['O_ORDERDATE'] < '1995-03-05') &
        (customer_df['C_MKTSEGMENT'] == 'BUILDING')
    ]

    # Join filtered orders with line items
    joined_data = filtered_orders.merge(
        lineitem_df[lineitem_df['L_SHIPDATE'] > '1995-03-15'],
        left_on='O_ORDERKEY',
        right_on='L_ORDERKEY'
    )

    # Calculate revenue
    joined_data['REVENUE'] = joined_data['L_EXTENDEDPRICE'] * (1 - joined_data['L_DISCOUNT'])

    # Group by order key and calculate the total revenue per order
    result = joined_data.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])['REVENUE'] \
        .sum().reset_index()

    # Sort the result by revenue in descending order
    result.sort_values(by='REVENUE', ascending=False, inplace=True)

    # Select and reorder the columns
    result = result[['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]

    # Write the output to a CSV file
    result.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    retrieve_and_process_data()
