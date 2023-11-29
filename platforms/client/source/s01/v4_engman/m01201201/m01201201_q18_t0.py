import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client.tpch

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL
with mysql_conn.cursor() as cur:
    cur.execute("""
        SELECT O_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE
        FROM orders
    """)
    orders = pd.DataFrame(cur.fetchall(), columns=['O_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])

mysql_conn.close()

# Query MongoDB
lineitem_docs = mongodb_db.lineitem.aggregate([
    {
        '$group': {
            '_id': '$L_ORDERKEY',
            'total_quantity': {'$sum': '$L_QUANTITY'}
        }
    },
    {'$match': {'total_quantity': {'$gt': 300}}}
])

lineitem = pd.DataFrame(list(lineitem_docs))
lineitem.rename(columns={'_id': 'L_ORDERKEY', 'total_quantity': 'L_TOTALQUANTITY'}, inplace=True)

# Query Redis
customer_data = redis_client.get('customer')
customer_df = pd.read_json(customer_data)

# Merge dataframes
result = (orders
          .merge(customer_df, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
          .merge(lineitem, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY'))

# Selecting required columns and sorting
result = result[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_TOTALQUANTITY']]
result = result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write result to CSV
result.to_csv('query_output.csv', index=False)
