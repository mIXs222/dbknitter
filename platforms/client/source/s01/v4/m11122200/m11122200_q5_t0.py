import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)
mysql_cur = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL
mysql_query = """
SELECT
    O_CUSTKEY,
    O_ORDERKEY,
    O_ORDERDATE
FROM
    orders
WHERE
    O_ORDERDATE >= '1990-01-01' AND O_ORDERDATE < '1995-01-01'
"""
mysql_cur.execute(mysql_query)
orders_data = pd.DataFrame(mysql_cur.fetchall(), columns=['O_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE'])

mysql_query = """
SELECT
    L_ORDERKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_SUPPKEY
FROM
    lineitem
"""
mysql_cur.execute(mysql_query)
lineitem_data = pd.DataFrame(mysql_cur.fetchall(), columns=['L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SUPPKEY'])
mysql_conn.close()

# Query MongoDB for nation and region
region_data = pd.DataFrame(list(mongo_db.region.find({'R_NAME': 'ASIA'}, {'_id': 0, 'R_REGIONKEY': 1})))
nation_data = pd.DataFrame(list(mongo_db.nation.find({'N_REGIONKEY': {'$in': region_data['R_REGIONKEY'].tolist()}}, {'_id': 0})))

# Query Redis for customer and supplier data
customer_data = pd.read_json(redis_conn.get('customer'), orient='records')
supplier_data = pd.read_json(redis_conn.get('supplier'), orient='records')

# Merging data
q_res = customer_data.merge(orders_data, left_on='C_CUSTKEY', right_on='O_CUSTKEY') \
    .merge(lineitem_data, left_on='O_ORDERKEY', right_on='L_ORDERKEY') \
    .merge(supplier_data, left_on=['C_NATIONKEY', 'L_SUPPKEY'], right_on=['S_NATIONKEY', 'S_SUPPKEY']) \
    .merge(nation_data, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculating revenue
q_res['REVENUE'] = q_res['L_EXTENDEDPRICE'] * (1 - q_res['L_DISCOUNT'])
result = q_res.groupby('N_NAME')['REVENUE'].sum().reset_index()
ordered_result = result.sort_values('REVENUE', ascending=False)

# Output the result to a CSV file
ordered_result.to_csv('query_output.csv', index=False)
