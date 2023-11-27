import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL Connection and Query
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

query_mysql = """
SELECT 
    L_ORDERKEY, 
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE_LOST
FROM lineitem 
WHERE 
    L_RETURNFLAG = 'R' AND 
    L_SHIPDATE BETWEEN '1993-10-01' AND '1994-01-01'
GROUP BY L_ORDERKEY
"""

lineitem_df = pd.read_sql_query(query_mysql, mysql_conn)
mysql_conn.close()

# MongoDB Connection and Query
mongo_conn = pymongo.MongoClient('mongodb', 27017)

db = mongo_conn['tpch']
orders_col = db['orders']
orders_df = pd.DataFrame(list(orders_col.find(
    {
        "O_ORDERDATE": {"$gte": "1993-10-01", "$lt": "1994-01-01"}
    },
    {
        "O_ORDERKEY": 1,
        "O_CUSTKEY": 1,
        "_id": 0
    }
)))

nation_col = db['nation']
nation_df = pd.DataFrame(list(nation_col.find({}, {"_id": 0})))

mongo_conn.close()

# Redis Connection and Query
redis_conn = DirectRedis(host='redis', port=6379, db=0)
customer_json_str = redis_conn.get('customer')
customer_df = pd.read_json(customer_json_str)

# Merging DataFrames
result_df = pd.merge(lineitem_df, orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
result_df = pd.merge(result_df, customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
result_with_nation = pd.merge(result_df, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Selecting and Sorting Columns
final_df = result_with_nation[[
    'C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT', 'REVENUE_LOST'
]].copy()
final_df.sort_values(by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, True], inplace=True)

# Save to CSV
final_df.to_csv('query_output.csv', index=False)
