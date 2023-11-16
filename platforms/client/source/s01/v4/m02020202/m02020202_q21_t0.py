import pymysql
import pandas as pd
import direct_redis

# MySQL connection setup
mysql_conn = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

try:
    # MySQL query without 'supplier' and 'nation' as these are in Redis
    mysql_query = """
        SELECT 
            O_ORDERKEY, 
            O_ORDERSTATUS 
        FROM 
            orders
        WHERE 
            O_ORDERSTATUS = 'F' 
    """
    # Execute MySQL query and get the results
    mysql_df = pd.read_sql_query(mysql_query, mysql_conn)
    
    # Redis connection setup
    redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    
    # Fetch 'supplier' and 'lineitem' DataFrames from Redis
    supplier_df = pd.read_json(redis_conn.get('supplier').decode('utf-8'))
    lineitem_df = pd.read_json(redis_conn.get('lineitem').decode('utf-8'))
    
    # Filtering for 'nation' as "SAUDI ARABIA" and joining with 'supplier'
    nation_df = pd.read_json(redis_conn.get('nation').decode('utf-8'))
    saudi_suppliers_df = supplier_df[supplier_df['S_NATIONKEY'].isin(
        nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']['N_NATIONKEY']
    )]
    
    # Merging DataFrames
    merged_df = (
        saudi_suppliers_df
        .merge(lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY', how='inner')
        .merge(mysql_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner')
    )
    
    # Applying filters and aggregations as per the original SQL query
    filtered_df = (
        merged_df
        .query("L_RECEIPTDATE > L_COMMITDATE")
        .groupby("S_NAME", as_index=False)
        .agg(NUMWAIT=('O_ORDERKEY', 'count'))
        .sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])
    )
    
    # Save the results to CSV
    filtered_df.to_csv('query_output.csv', index=False)
    
finally:
    # Close connections
    mysql_conn.close()
