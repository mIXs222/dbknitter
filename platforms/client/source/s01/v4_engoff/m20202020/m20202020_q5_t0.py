import pymysql
import pandas as pd
import direct_redis
import datetime

# Connect to mysql
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Get relevant tables from MySQL
with mysql_conn.cursor() as cursor:
    # Get ASIA region keys
    cursor.execute("SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA'")
    asia_region_keys = [entry[0] for entry in cursor.fetchall()]
    
    # Get nations in ASIA region
    cursor.execute(
        "SELECT N_NATIONKEY " +
        "FROM nation " +
        "WHERE N_REGIONKEY IN (" + ','.join(map(str, asia_region_keys)) + ")"
    )
    asia_nation_keys = [entry[0] for entry in cursor.fetchall()]

    # Get suppliers in ASIA
    cursor.execute(
        "SELECT S_SUPPKEY FROM supplier WHERE S_NATIONKEY IN (" + ','.join(map(str, asia_nation_keys)) + ")"
    )
    asia_supplier_keys = [entry[0] for entry in cursor.fetchall()]
    
    # Get lineitem and customer orders within date range and with suppliers and customers in ASIA
    date_start = datetime.date(1990, 1, 1)
    date_end = datetime.date(1995, 1, 1)
    cursor.execute(
        "SELECT C_NATIONKEY, " +
        "SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue " +
        "FROM lineitem " +
        "JOIN orders ON L_ORDERKEY = O_ORDERKEY " +
        "JOIN customer ON O_CUSTKEY = C_CUSTKEY " +
        "WHERE L_SUPPKEY IN (" + ','.join(map(str, asia_supplier_keys)) + ") " +
        "AND C_NATIONKEY IN (" + ','.join(map(str, asia_nation_keys)) + ") " +
        "AND L_SHIPDATE BETWEEN %s AND %s " +
        "GROUP BY C_NATIONKEY " +
        "ORDER BY revenue DESC",
        (date_start, date_end)
    )
    result = cursor.fetchall()

# Convert result to DataFrame
df_mysql = pd.DataFrame(result, columns=['nation_key', 'revenue'])

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get nation data from Redis
df_nation = pd.read_json(redis_conn.get('nation'))

# Merge results from MySQL with nation data from Redis
final_result = df_mysql.merge(df_nation, how='left', left_on='nation_key', right_on='N_NATIONKEY')

# Select relevant columns and rename for final output
final_result = final_result[['N_NAME', 'revenue']]

# Write output to CSV
final_result.to_csv('query_output.csv', index=False)
