import pandas as pd
import pymysql
import pymongo
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("SELECT * FROM part WHERE P_BRAND != 'Brand#45' AND P_TYPE NOT LIKE 'MEDIUM POLISHED%' AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)")
parts = pd.DataFrame(mysql_cursor.fetchall(), columns=["P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND", "P_TYPE", "P_SIZE", "P_CONTAINER", "P_RETAILPRICE", "P_COMMENT"])
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_cursor = mongo_db['supplier'].find({"S_COMMENT": {"$not": {"$regex": ".*Customer Complaints.*"}}})
suppliers = pd.DataFrame(list(mongo_cursor))
mongo_client.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
partsupp_encoded = redis_conn.get('partsupp')
partsupp = pd.read_json(partsupp_encoded, orient='split')

# Filter relevant data and merge tables on their keys
partsupp_filtered = partsupp[partsupp.PS_PARTKEY.isin(parts.P_PARTKEY) & partsupp.PS_SUPPKEY.isin(suppliers.S_SUPPKEY)]
result = parts.merge(partsupp_filtered, left_on='P_PARTKEY', right_on='PS_PARTKEY')
result = result.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg({'PS_SUPPKEY': pd.Series.nunique}).rename(columns={'PS_SUPPKEY': 'SUPPLIER_CNT'}).reset_index()
result = result.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Output to CSV
result.to_csv('query_output.csv', index=False)
