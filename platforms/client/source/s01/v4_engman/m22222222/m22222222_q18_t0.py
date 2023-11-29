import pandas as pd
from direct_redis import DirectRedis

def execute_query():
    redis_connection = DirectRedis(host='redis', port=6379, db=0)

    # Retrieve tables from Redis
    df_customer = pd.read_json(redis_connection.get('customer'))
    df_orders = pd.read_json(redis_connection.get('orders'))
    df_lineitem = pd.read_json(redis_connection.get('lineitem'))

    # Aggregate quantities by order
    df_qty_per_order = df_lineitem.groupby('L_ORDERKEY')['L_QUANTITY'].sum().reset_index()

    # Filter orders by total quantity
    df_large_orders = df_qty_per_order[df_qty_per_order['L_QUANTITY'] > 300]

    # Merge with the orders table
    df_orders_with_qty = df_large_orders.merge(df_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

    # Merge with customer table to get customer details
    df_final = df_orders_with_qty.merge(df_customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

    # Select relevant columns and sort
    df_final = df_final[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]
    df_final_sorted = df_final.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

    # Write to CSV
    df_final_sorted.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    execute_query()
