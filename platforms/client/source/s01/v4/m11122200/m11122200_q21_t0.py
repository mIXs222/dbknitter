import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query execution
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

mysql_cursor.execute("""
    SELECT
        O_ORDERKEY,
        O_ORDERSTATUS
    FROM
        orders
    WHERE
        O_ORDERSTATUS = 'F'
""")
orders = pd.DataFrame(mysql_cursor.fetchall(), columns=['O_ORDERKEY', 'O_ORDERSTATUS'])

mysql_cursor.execute("""
    SELECT
        L_ORDERKEY,
        L_SUPPKEY,
        L_RECEIPTDATE,
        L_COMMITDATE
    FROM
        lineitem
""")
lineitem = pd.DataFrame(mysql_cursor.fetchall(), columns=['L_ORDERKEY', 'L_SUPPKEY', 'L_RECEIPTDATE', 'L_COMMITDATE'])

mysql_cursor.close()
mysql_conn.close()

# MongoDB connection and query execution
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
nation_docs = list(mongodb.nation.find({'N_NAME': 'SAUDI ARABIA'}, {'_id': 0, 'N_NATIONKEY': 1}))
nation_df = pd.DataFrame(nation_docs)

# Redis connection and get supplier data
redis_client = DirectRedis(host='redis', port=6379, db=0)
supplier_df = pd.read_json(redis_client.get('supplier'))

# Data processing and querying
supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_df['N_NATIONKEY'])]

lineitem_receipt_commit = lineitem[lineitem['L_RECEIPTDATE'] > lineitem['L_COMMITDATE']]
lineitem_grouped = lineitem_receipt_commit.groupby(['L_ORDERKEY'])['L_SUPPKEY'].apply(set).reset_index()

final_df = supplier_df.merge(lineitem_grouped, how='inner', left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Filter the tables based on the conditions
merged_df = final_df.merge(orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Group by and count as required by the original query
result_df = merged_df.groupby(['S_NAME']).size().reset_index(name='NUMWAIT')

# Sort the result
result_df = result_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write output to CSV
result_df.to_csv('query_output.csv', index=False)
