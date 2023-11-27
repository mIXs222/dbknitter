import pymysql
import pandas as pd
from sqlalchemy import create_engine
from direct_redis import DirectRedis

def main():
    # MySQL Connection
    mysql_conn = pymysql.connect(
        host='mysql', 
        user='root', 
        password='my-secret-pw', 
        db='tpch'
    )

    # Redis Connection
    redis_conn = DirectRedis(host='redis', port=6379, db=0)

    # Load tables from Redis
    nation = pd.read_json(redis_conn.get('nation'))
    supplier = pd.read_json(redis_conn.get('supplier'))

    # Filter nations
    nations_of_interest = nation[(nation['N_NAME'] == 'INDIA') | (nation['N_NAME'] == 'JAPAN')]

    with mysql_conn:
        # Load tables from MySQL
        customer_df = pd.read_sql('SELECT * FROM customer', mysql_conn)
        orders_df = pd.read_sql('SELECT * FROM orders', mysql_conn)
        lineitem_df = pd.read_sql('SELECT * FROM lineitem', mysql_conn)

        # Prevent empty dataframes
        if nations_of_interest.empty or supplier.empty or customer_df.empty or orders_df.empty or lineitem_df.empty:
            print("Error: One of the dataframes is empty.")
            return

    # Joining and filtering data
    supplier_nation = supplier.merge(nations_of_interest, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    customer_nation = customer_df.merge(nations_of_interest, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

    shipping_info = (
        lineitem_df
        .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
        .merge(customer_nation[['C_CUSTKEY', 'N_NAME']], left_on='O_CUSTKEY', right_on='C_CUSTKEY')
        .merge(supplier_nation[['S_SUPPKEY', 'N_NAME']], left_on='L_SUPPKEY', right_on='S_SUPPKEY')
        .rename(columns={'N_NAME_x': 'Customer_Nation', 'N_NAME_y': 'Supplier_Nation'})
    )

    # Filter date range and calculate revenue
    shipping_info['O_ORDERDATE'] = pd.to_datetime(shipping_info['O_ORDERDATE'])
    shipping_info = shipping_info[
        (shipping_info['O_ORDERDATE'].dt.year == 1995) | (shipping_info['O_ORDERDATE'].dt.year == 1996)
    ]

    shipping_info['Revenue'] = shipping_info['L_EXTENDEDPRICE'] * (1 - shipping_info['L_DISCOUNT'])
    shipping_info['Year'] = shipping_info['O_ORDERDATE'].dt.year

    # Aggregate data
    final_result = shipping_info.groupby(['Supplier_Nation', 'Customer_Nation', 'Year']).agg({'Revenue': 'sum'}).reset_index()

    # Ordering result
    final_result = final_result.sort_values(by=['Supplier_Nation', 'Customer_Nation', 'Year'])

    # Write to CSV
    final_result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
