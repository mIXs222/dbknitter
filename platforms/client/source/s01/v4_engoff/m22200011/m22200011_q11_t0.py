import pymysql
import pandas as pd
import direct_redis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Execute query for MySQL database
with mysql_conn.cursor() as cursor:
    query = """
    SELECT s.S_SUPPKEY, PS_PARTKEY, (PS_AVAILQTY * PS_SUPPLYCOST) AS VALUE 
    FROM partsupp
    JOIN supplier s ON partsupp.PS_SUPPKEY = s.S_SUPPKEY
    WHERE s.S_NATIONKEY = (
        SELECT N_NATIONKEY 
        FROM nation 
        WHERE N_NAME = 'GERMANY'
    ) 
    HAVING VALUE > 0.0001 
    ORDER BY VALUE DESC
    """
    cursor.execute(query)
    mysql_result = cursor.fetchall()

# Columns for Pandas DataFrame
columns = ['S_SUPPKEY', 'PS_PARTKEY', 'VALUE']

# Convert MySQL result to DataFrame
df_mysql_result = pd.DataFrame(mysql_result, columns=columns)

# Close MySQL connection
mysql_conn.close()

# Redis connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch nation table from Redis
nation_data = r.get('nation')
df_nation = pd.read_json(nation_data)

# Filtering the nation for 'GERMANY'
germany_nation = df_nation[df_nation['N_NAME'] == 'GERMANY']
germany_nationkey = germany_nation['N_NATIONKEY'].iloc[0]

# Final filtering based on Germany's N_NATIONKEY
df_result = df_mysql_result[df_mysql_result['S_SUPPKEY'] == germany_nationkey]

# Write the result to a CSV file
df_result.to_csv('query_output.csv', index=False)
