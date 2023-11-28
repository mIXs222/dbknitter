import pandas as pd
import pymysql
import direct_redis

# Connection to MySQL Database tpch
conn_info_mysql = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connect to MySQL
mysql_conn = pymysql.connect(**conn_info_mysql)

# Query to get data from MySQL
query_mysql_parts = "SELECT * FROM part WHERE P_NAME LIKE '%dim%';"
query_mysql_supplier = "SELECT * FROM supplier;"
query_mysql_nation = "SELECT * FROM nation;"

parts = pd.read_sql(query_mysql_parts, mysql_conn)
supplier = pd.read_sql(query_mysql_supplier, mysql_conn)
nation = pd.read_sql(query_mysql_nation, mysql_conn)

# Close the MySQL connection
mysql_conn.close()

# Connection to Redis
conn_info_redis = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}
redis_conn = direct_redis.DirectRedis(**conn_info_redis)

# Get data from Redis
order_keys = ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT']
partsupp_keys = ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT']
lineitem_keys = ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT']

orders = pd.DataFrame(redis_conn.get('orders'), columns=order_keys)
partsupp = pd.DataFrame(redis_conn.get('partsupp'), columns=partsupp_keys)
lineitem = pd.DataFrame(redis_conn.get('lineitem'), columns=lineitem_keys)

# Analyze data
result = (parts.merge(lineitem, left_on='P_PARTKEY', right_on='L_PARTKEY')
               .merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
               .merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
               .merge(supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
               .merge(partsupp, left_on=['P_PARTKEY', 'S_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY']))

result['profit'] = (result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])) - (result['PS_SUPPLYCOST'] * result['L_QUANTITY'])
result['year'] = pd.to_datetime(result['O_ORDERDATE']).dt.year
result = result[['N_NAME', 'year', 'profit']]
result = result.groupby(['N_NAME', 'year']).sum().reset_index()
result = result.sort_values(by=['N_NAME', 'year'], ascending=[True, False])

# Writing output to CSV
result.to_csv('query_output.csv', index=False)
