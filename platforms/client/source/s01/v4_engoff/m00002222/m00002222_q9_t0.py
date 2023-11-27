import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query execution
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor,
)

with mysql_conn.cursor() as mysql_cursor:
    mysql_query = """
    SELECT n.N_NAME as nation, YEAR(o.O_ORDERDATE) as year, 
    SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) as profit
    FROM nation n
    JOIN supplier s ON s.S_NATIONKEY = n.N_NATIONKEY
    JOIN partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
    JOIN lineitem l ON l.L_SUPPKEY = s.S_SUPPKEY AND l.L_PARTKEY = ps.PS_PARTKEY
    JOIN orders o ON o.O_ORDERKEY = l.L_ORDERKEY
    JOIN part p ON p.P_PARTKEY = l.L_PARTKEY
    WHERE p.P_NAME LIKE '%dim%'
    GROUP BY nation, year
    ORDER BY nation ASC, year DESC;
    """
    mysql_cursor.execute(mysql_query)
    mysql_result = mysql_cursor.fetchall()
    mysql_columns = ['nation', 'year', 'profit']

# Convert MySQL data to DataFrame
mysql_df = pd.DataFrame(mysql_result, columns=mysql_columns)

# Redis connection and read data as DataFrame
redis_conn = DirectRedis(host='redis', port=6379, db=0)
parts_df = pd.read_json(redis_conn.get('part'), orient='records')
orders_df = pd.read_json(redis_conn.get('orders'), orient='records')
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')
partsupp_df = pd.read_json(redis_conn.get('partsupp'), orient='records')

# Merge the dataframes to compute the profit
merged_df = (
    mysql_df
    .merge(lineitem_df, left_on=['nation', 'year'], right_on=[lineitem_df.L_PARTKEY.map(lambda p: parts_df[parts_df.P_PARTKEY == p].P_NAME.str.contains('dim').any()), orders_df.O_ORDERDATE.dt.year], how='inner')
    .merge(part_df, left_on='P_PARTKEY', right_on='partsupp_df.PS_PARTKEY')
    .merge(partsupp_df, on='PS_SUPPKEY')
    .merge(orders_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    # Assuming you want to include 'nationality' from the MySQL query into Redis tables
    # Assuming you want to match MySQL 'year' to Redis 'O_ORDERDATE' (converted to year)
)

# Computation for profit
merged_df['profit'] = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])) - (merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY'])

# Sum by nation and year
result_df = merged_df.groupby(['nation', 'year'], as_index=False)['profit'].sum()

# Sort values
result_df.sort_values(['nation', 'year'], ascending=[True, False], inplace=True)

# Save to CSV
result_df.to_csv('query_output.csv', index=False)
