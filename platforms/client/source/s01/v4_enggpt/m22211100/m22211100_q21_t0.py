import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379)

# Load data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM orders WHERE O_ORDERSTATUS = 'F'")
    orders = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

    cursor.execute("SELECT * FROM lineitem WHERE L_RECEIPTDATE > L_COMMITDATE")
    lineitem = pd.DataFrame(cursor.fetchall(), columns=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'])

mysql_conn.close()

# Load data from MongoDB
suppliers = pd.DataFrame(list(mongo_db.supplier.find()))

# Load nation data from Redis
nation = pd.read_json(redis_conn.get('nation'))

# Only consider suppliers from Saudi Arabia
nation = nation[nation['N_NAME'] == 'SAUDI ARABIA']

# Join and filter the data
results = suppliers.merge(nation, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Analyze only suppliers associated with 'F' orders and late receipt line items
results = results[results['S_SUPPKEY'].isin(lineitem['L_SUPPKEY']) & results['S_SUPPKEY'].isin(orders['O_ORDERKEY'])]

# Compute stats for suppliers
results['NUMWAIT'] = results['S_SUPPKEY'].apply(lambda x: lineitem[lineitem['L_SUPPKEY'] == x].shape[0])

# Sort the results
results = results[['S_NAME', 'NUMWAIT']].sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Output to CSV
results.to_csv('query_output.csv', index=False)
