import pandas as pd
import pymysql
import direct_redis

# MySQL connection
def get_mysql_connection():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        database='tpch',
    )

mysql_tables = ['nation', 'part', 'partsupp', 'orders']
redis_tables = ['supplier', 'lineitem']

# Redis connection
def get_redis_connection():
    return direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MySQL
def fetch_mysql_data(table_name, connection):
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, connection)

# Fetch data from Redis
def fetch_redis_data(table_name, connection):
    df_json = connection.get(table_name)
    return pd.read_json(df_json)

def main():
    with get_mysql_connection() as mysql_conn:
        mysql_dfs = {table: fetch_mysql_data(table, mysql_conn) for table in mysql_tables}
    
    with get_redis_connection() as redis_conn:
        redis_dfs = {table: fetch_redis_data(table, redis_conn) for table in redis_tables}
    
    # Combine data from different databases
    # Filter parts with 'dim' in name
    dim_parts = mysql_dfs['part'][mysql_dfs['part']['P_NAME'].str.contains('dim')]
    nation = mysql_dfs['nation']
    partsupp = mysql_dfs['partsupp']
    orders = mysql_dfs['orders']
    lineitem = redis_dfs['lineitem']
    supplier = redis_dfs['supplier']

    # Merge tables
    parts_line = pd.merge(dim_parts, lineitem, left_on='P_PARTKEY', right_on='L_PARTKEY')
    parts_line_supp = pd.merge(parts_line, partsupp, left_on=['P_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
    parts_line_supp_order = pd.merge(parts_line_supp, orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    parts_line_supp_order_nation = pd.merge(parts_line_supp_order, nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

    # Calculate profit
    parts_line_supp_order_nation['PROFIT'] = (
        parts_line_supp_order_nation['L_EXTENDEDPRICE'] *
        (1 - parts_line_supp_order_nation['L_DISCOUNT']) -
        parts_line_supp_order_nation['PS_SUPPLYCOST'] * parts_line_supp_order_nation['L_QUANTITY']
    )

    # Extract year from order date
    parts_line_supp_order_nation['YEAR'] = pd.DatetimeIndex(parts_line_supp_order_nation['O_ORDERDATE']).year

    # Group by nation and year
    group_by_nation_year = parts_line_supp_order_nation.groupby(['N_NAME', 'YEAR'])
    profit_sum = group_by_nation_year['PROFIT'].sum().reset_index()

    # Order by nation asc and year desc
    ordered_profit = profit_sum.sort_values(['N_NAME', 'YEAR'], ascending=[True, False])

    # Write to CSV
    ordered_profit.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
