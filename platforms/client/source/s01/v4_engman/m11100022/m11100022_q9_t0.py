import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn:
    with mysql_conn.cursor() as cursor:
        cursor.execute("""
            SELECT s.S_NATIONKEY, YEAR(o.O_ORDERDATE) as year, SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) as profit
            FROM partsupp AS ps
            JOIN lineitem AS l ON ps.PS_PARTKEY = l.L_PARTKEY AND ps.PS_SUPPKEY = l.L_SUPPKEY
            JOIN orders AS o ON l.L_ORDERKEY = o.O_ORDERKEY
            JOIN supplier AS s ON l.L_SUPPKEY = s.S_SUPPKEY
            GROUP BY s.S_NATIONKEY, year
            """)
        mysql_results = cursor.fetchall()

# Transform MySQL results to DataFrame
mysql_df = pd.DataFrame(mysql_results, columns=['S_NATIONKEY', 'year', 'profit'])

# MongoDB connection and query
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
nation_col = mongo_db['nation']

# Load the nations
nations_df = pd.DataFrame(list(nation_col.find({}, {'_id': 0, 'N_NATIONKEY': 1, 'N_NAME': 1})))

# Merge the MySQL and MongoDB results on nation key
query_df = mysql_df.merge(nations_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)
orders_df = pd.read_json(redis_conn.get('orders').decode('utf-8'))
lineitem_df = pd.read_json(redis_conn.get('lineitem').decode('utf-8'))

# Perform Redis related operations if required (currently empty because Orders and Lineitem are SQL related)

# Sort the result and write to CSV
result = query_df.sort_values(['N_NAME', 'year'], ascending=[True, False])
result.to_csv('query_output.csv', index=False)
