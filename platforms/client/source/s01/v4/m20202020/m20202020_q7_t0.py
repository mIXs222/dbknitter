# python_code.py
import pandas as pd
import pymysql
import direct_redis
import datetime

# MySQL connection and query
def query_mysql():
    conn = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch'
    )
    mysql_query = """
    SELECT 
        supplier.S_SUPPKEY, 
        supplier.S_NATIONKEY AS S_NATIONKEY, 
        lineitem.L_ORDERKEY, 
        lineitem.L_EXTENDEDPRICE, 
        lineitem.L_DISCOUNT, 
        lineitem.L_SHIPDATE, 
        customer.C_CUSTKEY, 
        customer.C_NATIONKEY AS C_NATIONKEY
    FROM 
        supplier, 
        lineitem, 
        customer 
    WHERE 
        supplier.S_SUPPKEY = lineitem.L_SUPPKEY AND 
        lineitem.L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
    """
    df_mysql = pd.read_sql(mysql_query, conn)
    conn.close()
    return df_mysql

# Redis connection and data fetch
def fetch_redis_table(redis_key):
    redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    df_json = redis_conn.get(redis_key)
    df_redis = pd.read_json(df_json)
    return df_redis

# Main function to gather data and perform computation
def main():
    # Fetch data from MySQL
    mysql_data = query_mysql()
    
    # Fetch data from Redis
    orders_data = fetch_redis_table('orders')
    nation_data = fetch_redis_table('nation')
    
    # Merge MySQL data with orders from Redis
    merged_df = mysql_data.merge(orders_data, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    
    # Merge with nation data twice, once for suppliers and once for customers
    merged_df = merged_df.merge(nation_data, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    merged_df.rename(columns={'N_NAME': 'SUPP_NATION'}, inplace=True)
    
    merged_df = merged_df.merge(nation_data, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
    merged_df.rename(columns={'N_NAME': 'CUST_NATION'}, inplace=True)
    
    # Filter for nations India and Japan
    filtered_df = merged_df[
        ((merged_df['SUPP_NATION'] == 'JAPAN') & (merged_df['CUST_NATION'] == 'INDIA')) |
        ((merged_df['SUPP_NATION'] == 'INDIA') & (merged_df['CUST_NATION'] == 'JAPAN'))
    ]
    
    # Add L_YEAR column and compute VOLUME
    filtered_df['L_YEAR'] = filtered_df['L_SHIPDATE'].dt.year
    filtered_df['VOLUME'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
    
    # Group by the required fields and calculate REVENUE
    grouped_df = filtered_df.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR']).agg({'VOLUME': 'sum'}).reset_index()
    grouped_df.rename(columns={'VOLUME': 'REVENUE'}, inplace=True)
    
    # Write results to a CSV file
    grouped_df.to_csv('query_output.csv', index=False)

# Run main function
if __name__ == "__main__":
    main()
