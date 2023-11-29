import pandas as pd
import pymysql
import pymongo
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
with mysql_conn.cursor() as cursor:
    # Query for `part` and `supplier` tables
    cursor.execute("""
    SELECT 
        p.P_PARTKEY, s.S_NATIONKEY 
    FROM 
        part p JOIN supplier s ON p.P_PARTKEY = s.S_SUPPKEY 
    WHERE 
        p.P_TYPE = 'SMALL PLATED COPPER';
    """)
    part_supplier = pd.DataFrame(cursor.fetchall(), columns=['P_PARTKEY', 'S_NATIONKEY'])

mysql_conn.close()

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
nation = pd.DataFrame(list(mongo_db.nation.find({"N_NAME": "INDIA", "N_REGIONKEY": {"$in": mongo_db.region.find({"R_NAME": "ASIA"}, {"R_REGIONKEY": 1})}})))
orders = pd.DataFrame(list(mongo_db.orders.find({"O_ORDERDATE": {"$regex": "199[5-6]"}}, {"O_ORDERKEY": 1, "O_TOTALPRICE": 1, "O_ORDERDATE": 1})))

# Redis connection
redis_conn = DirectRedis(host="redis", port=6379, db=0)
lineitem_df = pd.DataFrame(redis_conn.get(b'lineitem'))

# Merge data and perform calculations
combined_data = lineitem_df.merge(part_supplier, left_on='L_PARTKEY', right_on='P_PARTKEY')
combined_data = combined_data.merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
combined_data = combined_data.merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

combined_data['YEAR'] = combined_data['O_ORDERDATE'].str.extract(r'(\d{4})').astype(int)
combined_data['REVENUE'] = combined_data['L_EXTENDEDPRICE'] * (1 - combined_data['L_DISCOUNT'])
market_share_data = combined_data.groupby('YEAR')['REVENUE'].sum().reset_index()
market_share_data.columns = ['ORDER_YEAR', 'MARKET_SHARE']

# Write result to file
market_share_data.to_csv('query_output.csv', index=False)
