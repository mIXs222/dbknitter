# query_multi_source.py

import pymongo
import pymysql
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
from sqlalchemy import create_engine

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')
mysql_cursor = mysql_connection.cursor()

# Retrieve data from MySQL
mysql_cursor.execute("""
SELECT
    O_ORDERKEY, O_CUSTKEY, strftime('%Y', O_ORDERDATE) AS O_YEAR, O_ORDERDATE,
    S_SUPPKEY, S_NATIONKEY,
    N_NATIONKEY, N_NAME
FROM
    orders, supplier, nation
WHERE
    S_SUPPKEY = O_CUSTKEY AND
    N_NATIONKEY = S_NATIONKEY AND
    O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
""")
mysql_data = mysql_cursor.fetchall()
mysql_columns = ['O_ORDERKEY', 'O_CUSTKEY', 'O_YEAR', 'O_ORDERDATE', 'S_SUPPKEY', 'S_NATIONKEY', 'N_NATIONKEY', 'N_NAME']
df_mysql = pd.DataFrame(mysql_data, columns=mysql_columns)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Retrieve data from MongoDB
lineitems = mongo_db['lineitem'].find({
    'L_SHIPDATE': {'$gte': datetime(1995, 1, 1), '$lte': datetime(1996, 12, 31)}
})
regions = mongo_db['region'].find({'R_NAME': 'ASIA'})

# Convert MongoDB data to DataFrame
df_lineitems = pd.DataFrame(list(lineitems))
df_regions = pd.DataFrame(list(regions))
df_lineitems['VOLUME'] = df_lineitems['L_EXTENDEDPRICE'] * (1 - df_lineitems['L_DISCOUNT'])

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from Redis
df_parts = pd.read_json(redis_client.get('part'))
df_customers = pd.read_json(redis_client.get('customer'))

# Close the clients
redis_client.close()
mongo_client.close()
mysql_cursor.close()
mysql_connection.close()

# Perform the join operations and compute the result
df = pd.merge(df_mysql, df_customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
df = pd.merge(df, df_parts, left_on='O_ORDERKEY', right_on='P_PARTKEY')
df = pd.merge(df, df_lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df = pd.merge(df, df_regions, left_on='S_NATIONKEY', right_on='R_REGIONKEY')
df_result = df[df['P_TYPE'] == 'SMALL PLATED COPPER'].groupby('O_YEAR').apply(
    lambda x: pd.Series({
        'MKT_SHARE': sum(x[x['N_NAME'] == 'INDIA']['VOLUME']) / sum(x['VOLUME'])
    })
).reset_index()

# Write the result to csv
df_result.to_csv('query_output.csv', index=False)
