import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Query MySQL data
with mysql_connection.cursor() as cursor:
    cursor.execute("""
    SELECT 
        c.C_CUSTKEY,
        c.C_NAME,
        SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue_lost,
        c.C_ACCTBAL,
        c.C_ADDRESS,
        c.C_PHONE,
        c.C_COMMENT
    FROM
        customer c
    JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY 
    JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY 
    WHERE 
        l.L_RETURNFLAG = 'R' 
        AND o.O_ORDERDATE >= '1993-10-01' 
        AND o.O_ORDERDATE < '1994-01-01'
    GROUP BY 
        c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL, c.C_ADDRESS, c.C_PHONE, c.C_COMMENT
    ORDER BY 
        revenue_lost ASC, c.C_CUSTKEY ASC, c.C_NAME ASC, c.C_ACCTBAL DESC;
    """)
    mysql_data = cursor.fetchall()

# Prepare the DataFrame
mysql_df = pd.DataFrame(mysql_data, columns=["C_CUSTKEY", "C_NAME", "revenue_lost", "C_ACCTBAL", "C_ADDRESS", "C_PHONE", "C_COMMENT"])

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query Redis data
nation_data = redis_connection.get('nation')
nation_df = pd.DataFrame(eval(nation_data), columns=["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"])

# Merge MySQL and Redis data
result_df = pd.merge(mysql_df, nation_df, how='left', left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
