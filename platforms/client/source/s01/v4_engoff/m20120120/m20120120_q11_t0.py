import pymysql
import pandas as pd
import direct_redis

# MySQL connection and query
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

ps_query = """SELECT PS_PARTKEY, SUM(PS_AVAILQTY * PS_SUPPLYCOST) AS value
              FROM partsupp
              GROUP BY PS_PARTKEY
              HAVING SUM(PS_AVAILQTY * PS_SUPPLYCOST) > 0.0001 * (SELECT SUM(PS_AVAILQTY * PS_SUPPLYCOST) FROM partsupp)
              ORDER BY value DESC"""

mysql_partsupp = pd.read_sql(ps_query, mysql_connection)
mysql_connection.close()

# Redis connection and data retrieval
r_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

nation_df = pd.read_json(r_conn.get('nation'))
supplier_df = pd.read_json(r_conn.get('supplier'))

# Filter suppliers in GERMANY and merge with partsupp
german_nationkey = nation_df[nation_df['N_NAME'] == 'GERMANY']['N_NATIONKEY'].values[0]
supplier_germany = supplier_df[supplier_df['S_NATIONKEY'] == german_nationkey]

important_stock_df = supplier_germany.merge(mysql_partsupp, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Write results to CSV
important_stock_df.to_csv('query_output.csv', index=False)
