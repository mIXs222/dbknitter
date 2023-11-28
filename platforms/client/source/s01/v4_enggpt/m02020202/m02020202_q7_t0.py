# python code
import pymysql
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Define the MySQL connection and query
def get_mysql_data():
    conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
    try:
        nation_query = "SELECT * FROM nation WHERE N_NAME='JAPAN' OR N_NAME='INDIA';"
        orders_query = "SELECT * FROM orders WHERE O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31';"

        with conn.cursor() as cursor:
            cursor.execute(nation_query)
            nations = cursor.fetchall()
            cursor.execute(orders_query)
            orders = cursor.fetchall()

        return nations, orders
    finally:
        conn.close()

# Retrieve data from Redis
def get_redis_data():
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    
    supplier_df = pd.read_json(redis_client.get('supplier'))
    customer_df = pd.read_json(redis_client.get('customer'))
    lineitem_df = pd.read_json(redis_client.get('lineitem'))

    return supplier_df, customer_df, lineitem_df

# Process data and generate report
def generate_report():
    nations, orders = get_mysql_data()
    supplier_df, customer_df, lineitem_df = get_redis_data()

    # Convert to DataFrame
    nation_df = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])
    orders_df = pd.DataFrame(orders, columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

    # Merge and filter data
    supplier_nation = nation_df[nation_df['N_NAME'] == 'JAPAN']
    customer_nation = nation_df[nation_df['N_NAME'] == 'INDIA']

    # Merge DataFrames on nation keys
    supplier_merged = pd.merge(supplier_df, supplier_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    customer_merged = pd.merge(customer_df, customer_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

    # Join with orders and lineitems
    lineitem_orders = pd.merge(lineitem_df, orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    results = pd.merge(lineitem_orders, supplier_merged, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    final_results = pd.merge(results, customer_merged, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

    # Calculate adjusted price
    final_results['adjusted_price'] = final_results['L_EXTENDEDPRICE'] * (1 - final_results['L_DISCOUNT'])
    
    # Group and sort the results
    revenue_report = final_results.groupby(['N_NAME_x', 'N_NAME_y', final_results['O_ORDERDATE'].dt.year]).agg({'adjusted_price': 'sum'}).reset_index()

    # Rename columns
    revenue_report.columns = ['supplier_nation', 'customer_nation', 'year', 'revenue']

    # Sort based on requirements
    revenue_report = revenue_report.sort_values(['supplier_nation', 'customer_nation', 'year'])

    # Writing the output to a CSV file
    revenue_report.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    generate_report()
