import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Function to connect to MySQL and execute query
def query_mysql():
    connection = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch'
    )
    try:
        with connection.cursor() as cursor:
            subquery = """
            SELECT MIN(PS_SUPPLYCOST)
            FROM partsupp, supplier
            WHERE P_PARTKEY = PS_PARTKEY
            AND S_SUPPKEY = PS_SUPPKEY
            """
            cursor.execute(subquery)
            min_cost = cursor.fetchone()[0]

            query = f"""
            SELECT
                S_ACCTBAL,
                S_NAME,
                P_PARTKEY,
                PS_SUPPLYCOST,
                S_ADDRESS,
                S_PHONE,
                S_COMMENT,
                S_SUPPKEY
            FROM
                partsupp, supplier
            WHERE
                S_SUPPKEY = PS_SUPPKEY
                AND PS_SUPPLYCOST = {min_cost}
            """
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [
                'S_ACCTBAL', 'S_NAME', 'P_PARTKEY', 'PS_SUPPLYCOST',
                'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'S_SUPPKEY'
            ]
            df_mysql = pd.DataFrame(results, columns=columns)
            return df_mysql
    finally:
        connection.close()

# Function to get Redis data
def get_redis_data(tablename):
    r = DirectRedis(host='redis', port=6379, db=0)
    data = r.get(tablename)
    return pd.read_msgpack(data)

# Combine the MySQL and Redis data
df_nation = get_redis_data('nation')
df_region = get_redis_data('region')
df_part = get_redis_data('part')

df_mysql = query_mysql()
df_part_filtered = df_part[(df_part['P_SIZE'] == 15) & (df_part['P_TYPE'].str.contains('BRASS'))]
df_europe = df_nation.merge(df_region[df_region['R_NAME'] == 'EUROPE'], left_on='N_REGIONKEY', right_on='R_REGIONKEY')
df_result = df_mysql.merge(df_part_filtered, left_on='P_PARTKEY', right_on='P_PARTKEY')
df_result = df_result.merge(df_europe, left_on='S_SUPPKEY', right_on='N_NATIONKEY')

final_columns = [
    'S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT'
]
df_final = df_result.loc[:, final_columns].sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

df_final.to_csv('query_output.csv', index=False)
