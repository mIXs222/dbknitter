import pymysql
import pandas as pd
import pymongo
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host="mysql", user="root", password="my-secret-pw", database="tpch")
with mysql_conn.cursor() as mysql_cursor:
    # Query for MySQL
    mysql_query = """
    SELECT n.N_NAME, SUM(p.P_RETAILPRICE) AS revenue
    FROM nation n
    JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
    JOIN supplier s ON s.S_NATIONKEY = n.N_NATIONKEY
    JOIN part p ON p.P_TYPE = 'SMALL PLATED COPPER'
    WHERE r.R_NAME = 'ASIA' AND n.N_NAME = 'INDIA'
    GROUP BY n.N_NAME
    """
    mysql_cursor.execute(mysql_query)
    supplier_nation_data = pd.DataFrame(mysql_cursor.fetchall(), columns=['N_NAME', 'revenue'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
lineitem_collection = mongo_db['lineitem']

# Query for MongoDB
mongo_query = [
    {'$match': {
        'L_SHIPDATE': {'$regex': '^199[5-6]'},
        'L_PARTKEY': {'$in': supplier_nation_data['N_NAME'].tolist()}
    }},
    {'$group': {
        '_id': {'$substr': ['$L_SHIPDATE', 0, 4]},
        'total_revenue': {
            '$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]}
        }
    }},
    {'$sort': {'_id': 1}}
]
results = lineitem_collection.aggregate(mongo_query)
lineitem_data = pd.DataFrame(list(results))

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)
supplier_data = pd.read_json(redis.get('supplier').decode('utf-8'))
customer_data = pd.read_json(redis.get('customer').decode('utf-8'))

# Combine the results
combined_data = pd.merge(supplier_nation_data, lineitem_data, left_on='N_NAME', right_on='_id', how='inner')
final_data = combined_data[['total_revenue']].T
final_data.columns = ['1995', '1996']

# Save to CSV
final_data.to_csv('query_output.csv', index=False)
