import pymysql
import pymongo
from pymongo import MongoClient
import pandas as pd

# connect to mysql server
mysql_conn = pymysql.connect(host='mysql', 
                             user='root', 
                             password='my-secret-pw',
                             db='tpch')

# get data from mysql
mysql_query = '''
SELECT
    S_ACCTBAL,
    S_NAME,
    N_NAME,
    P_PARTKEY,
    P_MFGR,
    S_ADDRESS,
    S_PHONE,
    S_COMMENT
FROM
    part,
    supplier,
    nation,
    region
WHERE
    P_SIZE = 15
    AND P_TYPE LIKE '%BRASS'
    AND S_NATIONKEY = N_NATIONKEY
    AND N_REGIONKEY = R_REGIONKEY
    AND R_NAME = 'EUROPE'
'''

mysql_df = pd.read_sql(mysql_query, mysql_conn)

# connect to mongodb server
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']

# get data from mongodb
mongodb_df = pd.DataFrame(list(mongodb.partsupp.find({}, {'_id': 0})))

# join data from different sources
df = pd.merge(mysql_df, mongodb_df, left_on=['P_PARTKEY', 'S_NAME'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])

# filter the data by PS_SUPPLYCOST
min_supplycost = df['PS_SUPPLYCOST'].min()
df = df[df['PS_SUPPLYCOST'] == min_supplycost]

# sort the data
df = df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# write the output to the file
df.to_csv('query_output.csv', index=False)
