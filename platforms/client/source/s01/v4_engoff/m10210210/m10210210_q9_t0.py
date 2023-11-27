# python_code.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query execution
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_connection.cursor() as cursor:
    cursor.execute("""
        SELECT
            s.N_NAME,
            YEAR(l.L_SHIPDATE) AS year,
            SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) AS profit
        FROM 
            lineitem l
        JOIN
            partsupp ps ON l.L_PARTKEY = ps.PS_PARTKEY AND l.L_SUPPKEY = ps.PS_SUPPKEY
        JOIN
            supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
        WHERE
            l.L_SHIPDATE BETWEEN '1992-01-01' AND '1997-12-31'
        GROUP BY
            s.N_NAME, year
        ORDER BY
            s.N_NAME ASC, year DESC
    """)
    mysql_data = cursor.fetchall()
    mysql_columns = [column[0] for column in cursor.description]

# MongoDB connection and retrieval of 'nation' and 'supplier' tables
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
nation_collection = mongo_db["nation"]
supplier_collection = mongo_db["supplier"]
nations = pd.DataFrame(list(nation_collection.find()), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])
suppliers = pd.DataFrame(list(supplier_collection.find()), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Redis connection and retrieval of 'part' table
redis_client = DirectRedis(port=6379, host='redis')
part = pd.DataFrame(redis_client.get("part"))

# Combining the MySQL and MongoDB data
mysql_df = pd.DataFrame(mysql_data, columns=mysql_columns)
combined_df = pd.merge(mysql_df, suppliers, left_on="L_SUPPKEY", right_on="S_SUPPKEY", how="left")
combined_df = pd.merge(combined_df, nations, left_on="S_NATIONKEY", right_on="N_NATIONKEY", how="left")

# Output to query_output.csv
combined_df.to_csv('query_output.csv', index=False)
