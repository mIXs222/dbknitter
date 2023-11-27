import pandas as pd
from direct_redis import DirectRedis

def main():
    redis_conn = DirectRedis(host='redis', port=6379, db=0)

    # Assuming that 'get' returns a JSON string that can be directly converted to a Pandas DataFrame
    df_customer = pd.read_json(redis_conn.get('customer'))
    df_orders = pd.read_json(redis_conn.get('orders'))
    df_lineitem = pd.read_json(redis_conn.get('lineitem'))

    # Filtering lineitem for large quantity orders
    df_large_lineitem = df_lineitem[df_lineitem['L_QUANTITY'] > 300]

    # Joining tables to get required results
    df_result = df_large_lineitem.merge(df_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner').merge(
        df_customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner')

    # Selecting required columns and naming them appropriately
    df_final_result = df_result[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]
    df_final_result.columns = ['Customer Name', 'Customer Key', 'Order Key', 'Order Date', 'Total Price', 'Quantity']

    # Writing results to file
    df_final_result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
