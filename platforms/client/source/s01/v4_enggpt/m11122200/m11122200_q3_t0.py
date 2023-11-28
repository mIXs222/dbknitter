import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}
mysql_connection = pymysql.connect(**mysql_conn_info)

# Connect to Redis
redis_conn_info = {
    'host': 'redis',
    'port': 6379
}
redis_connection = DirectRedis(**redis_conn_info)

try:
    # Query MySQL for orders and lineitem tables
    query_orders = """
    SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_SHIPPRIORITY
    FROM orders 
    WHERE O_ORDERDATE < '1995-03-15'
    """
    query_lineitem = """
    SELECT L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE
    FROM lineitem
    WHERE L_SHIPDATE > '1995-03-15'
    """
    orders_df = pd.read_sql(query_orders, mysql_connection)
    lineitem_df = pd.read_sql(query_lineitem, mysql_connection)
    
    # Query Redis for customer table
    customer_df = pd.read_json(redis_connection.get('customer'), orient='split')

    # Close MySQL connection now that data is fetched
    mysql_connection.close()

    # Filter customers by 'BUILDING' market segment
    building_customers_df = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']

    # Merge DataFrames to consolidate required data
    combined_df = pd.merge(
        building_customers_df, 
        orders_df, 
        how='inner', 
        left_on='C_CUSTKEY', 
        right_on='O_CUSTKEY'
    )
    
    combined_df = pd.merge(
        combined_df, 
        lineitem_df, 
        how='inner', 
        left_on='O_ORDERKEY', 
        right_on='L_ORDERKEY'
    )

    # Calculate revenue
    combined_df['revenue'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])
    
    # Group by order key, order date, and shipping priority
    grouped = combined_df.groupby(by=['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
    
    # Sum revenue within each group and sort the results
    result = grouped['revenue'].sum().reset_index()
    result = result.sort_values(by=['revenue', 'O_ORDERDATE'], ascending=[False, True])
    
    # Write output to a CSV file
    result.to_csv('query_output.csv', index=False)

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if 'mysql_connection' in locals() and mysql_connection.open:
        mysql_connection.close()
    if 'redis_connection' in locals():
        redis_connection.close()
