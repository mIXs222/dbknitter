import pandas as pd
import direct_redis

# Function to convert string dates to pandas datetime objects
def convert_dates(df, *cols):
    for col in cols:
        df[col] = pd.to_datetime(df[col])

# Function to perform the query
def order_priority_checking_query(orders_df, lineitem_df):
    # Join orders and lineitem on order key
    merged_df = orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    # Filter orders by date range
    filtered_orders = merged_df[
        (merged_df['O_ORDERDATE'] >= '1993-07-01') &
        (merged_df['O_ORDERDATE'] <= '1993-10-01')
    ]

    # Add a flag for late receipt
    filtered_orders['late_receipt'] = filtered_orders['L_COMMITDATE'] < filtered_orders['L_RECEIPTDATE']

    # Count orders with at least one late receipt per priority
    result = filtered_orders[filtered_orders['late_receipt']].groupby(
        'O_ORDERPRIORITY')['O_ORDERKEY'].nunique().reset_index()

    # Rename columns
    result.columns = ['O_ORDERPRIORITY', 'ORDER_COUNT']

    # Sort by order priority
    result.sort_values(by='O_ORDERPRIORITY', inplace=True)

    # Write result to CSV
    result.to_csv('query_output.csv', index=False)

def main():
    # Establish connection to Redis
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Load tables from Redis
    orders_df = pd.read_json(r.get('orders').decode('utf-8'))
    lineitem_df = pd.read_json(r.get('lineitem').decode('utf-8'))

    # Convert date columns to datetime
    convert_dates(orders_df, 'O_ORDERDATE')
    convert_dates(lineitem_df, 'L_COMMITDATE', 'L_RECEIPTDATE')

    # Perform the query
    order_priority_checking_query(orders_df, lineitem_df)

if __name__ == '__main__':
    main()
