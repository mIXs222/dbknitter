# query.py

import pymysql
import pandas as pd
from pandas.io.sql import read_sql

conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor)

try:
    with conn.cursor() as cursor:
        sql = '''
            SELECT 
                PS_PARTKEY, 
                SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS VALUE 
            FROM 
                partsupp, 
                supplier, 
                nation
            WHERE 
                PS_SUPPKEY = S_SUPPKEY 
                AND S_NATIONKEY = N_NATIONKEY 
                AND N_NAME = 'GERMANY'
            GROUP BY 
                PS_PARTKEY 
            HAVING 
                SUM(PS_SUPPLYCOST * PS_AVAILQTY) >
                (
                SELECT 
                    SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001000000 
                FROM 
                    partsupp, 
                    supplier, 
                    nation 
                WHERE 
                    PS_SUPPKEY = S_SUPPKEY 
                    AND S_NATIONKEY = N_NATIONKEY 
                    AND N_NAME = 'GERMANY'
                )
            ORDER BY 
                VALUE DESC
        '''
        df = read_sql(sql, conn)
        df.to_csv('query_output.csv', index=False)
        
finally:
    conn.close()
