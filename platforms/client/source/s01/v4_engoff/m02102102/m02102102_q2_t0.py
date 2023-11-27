import pymysql.cursors
import pymongo
import pandas as pd

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT n.N_NAME, s.S_SUPPKEY, s.S_ACCTBAL, s.S_NAME, s.S_ADDRESS, s.S_PHONE, s.S_COMMENT
        FROM nation n JOIN supplier s ON n.N_NATIONKEY = s.S_NATIONKEY
        WHERE n.N_REGIONKEY = (
            SELECT r.R_REGIONKEY
            FROM region r
            WHERE r.R_NAME = 'EUROPE'
        )
    """)
    suppliers = cursor.fetchall()

# Convert suppliers to DataFrame
suppliers_df = pd.DataFrame(suppliers, columns=['N_NAME', 'S_SUPPKEY', 'S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
parts = list(mongodb['part'].find({'P_TYPE': 'BRASS', 'P_SIZE': 15}, {'P_PARTKEY': 1, 'P_MFGR': 1}))

# Convert parts to DataFrame
parts_df = pd.DataFrame(parts, columns=['P_PARTKEY', 'P_MFGR'])

# Simplified Redis connection
class DirectRedis:
    def __init__(self, host, port, db):
        import redis
        self.client = redis.Redis(host=host, port=port, db=db)
        
    def get(self, table_name):
        return self.client.get(table_name)

redis_conn = DirectRedis('redis', 6379, 0)
region = pd.read_pickle(redis_conn.get('region'))
partsupp = pd.read_pickle(redis_conn.get('partsupp'))

# Query Processing
result = pd.merge(partsupp, suppliers_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
result = pd.merge(result, parts_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')
result = result[result['PS_SUPPLYCOST'] == result.groupby('P_PARTKEY')['PS_SUPPLYCOST'].transform('min')]
result = result.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Write the final result to csv
result.to_csv('query_output.csv', index=False)
