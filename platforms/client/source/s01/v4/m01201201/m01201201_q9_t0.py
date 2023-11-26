import pymysql
import pymongo
from sqlalchemy import create_engine
import pandas as pd
import direct_redis

def execute_mysql():
    mysql_conn = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch')

    query = """
    SELECT 
        N_NAME AS NATION, 
        O_ORDERKEY, 
        S_SUPPKEY, 
        O_ORDERDATE, 
        O_TOTALPRICE 
    FROM 
        orders JOIN nation ON O_ORDERKEY = N_NATIONKEY 
        JOIN supplier ON S_NATIONKEY = N_NATIONKEY;
    """
    df_mysql = pd.read_sql_query(query, mysql_conn)
    mysql_conn.close()
    return df_mysql

def execute_mongodb():
    mongodb_client = pymongo.MongoClient('mongodb', 27017)
    db = mongodb_client['tpch']
    partsupp = pd.DataFrame(list(db.partsupp.find({})))
    lineitem = pd.DataFrame(list(db.lineitem.find({})))
    mongodb_client.close()
    return partsupp, lineitem

def execute_redis():
    redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    df_part = pd.read_msgpack(redis_conn.get('part'))
    redis_conn.close()
    return df_part

# Execute database queries
df_mysql = execute_mysql()
partsupp, lineitem = execute_mongodb()
df_part = execute_redis()

# Calculate the year from O_ORDERDATE
df_mysql['O_YEAR'] = pd.to_datetime(df_mysql['O_ORDERDATE']).dt.year

# Filter parts that contain 'dim'
df_part_dim = df_part[df_part['P_NAME'].str.contains('dim')]

# Join operations
df_joined = df_part_dim.merge(lineitem, left_on='P_PARTKEY', right_on='L_PARTKEY')\
                       .merge(partsupp, on=['PS_PARTKEY', 'PS_SUPPKEY'])\
                       .merge(df_mysql, left_on=['L_ORDERKEY', 'L_SUPPKEY'], right_on=['O_ORDERKEY', 'S_SUPPKEY'])

# Calculate amount
df_joined['AMOUNT'] = df_joined['L_EXTENDEDPRICE'] * (1 - df_joined['L_DISCOUNT']) - df_joined['PS_SUPPLYCOST'] * df_joined['L_QUANTITY']

# Group by nation and year
df_final = df_joined.groupby(['NATION', 'O_YEAR']).agg(SUM_PROFIT=('AMOUNT', 'sum')).reset_index()

# Sort by nation and year DESC
df_final = df_final.sort_values(by=['NATION', 'O_YEAR'], ascending=[True, False])

# Write to CSV
df_final.to_csv('query_output.csv', index=False)
