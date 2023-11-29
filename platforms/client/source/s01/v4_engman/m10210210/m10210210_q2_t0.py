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
mysql_cursor = mysql_conn.cursor()

# Retrieve partsupp data from MySQL
partsupp_query = "SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST FROM partsupp"
mysql_cursor.execute(partsupp_query)
partsupp_data = mysql_cursor.fetchall()
partsupp_df = pd.DataFrame(partsupp_data, columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_SUPPLYCOST'])

# Retrieve region data and filter by EUROPE from MySQL
region_query = "SELECT R_REGIONKEY FROM region WHERE R_NAME = 'EUROPE'"
mysql_cursor.execute(region_query)
region_key = mysql_cursor.fetchone()[0]

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Retrieve nation data and filter by EUROPE region key from MongoDB
nation_cursor = mongo_db.nation.find({'N_REGIONKEY': region_key})
nation_df = pd.DataFrame(list(nation_cursor))

# Retrieve supplier data from MongoDB
supplier_cursor = mongo_db.supplier.find({})
supplier_df = pd.DataFrame(list(supplier_cursor))

# Redis connection
redis = DirectRedis(host='redis', port=6379, db=0)

# Retrieve part data from Redis and filter by P_TYPE 'BRASS' and P_SIZE '15'
part_data = redis.get('part')
part_df = pd.DataFrame(part_data)
part_df = part_df[(part_df['P_TYPE'] == 'BRASS') & (part_df['P_SIZE'] == 15)]

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Join operations to simulate SQL joins across different DBMSs
# 1. Join partsupp and supplier - PS_SUPPKEY = S_SUPPKEY
parts_and_supplier_df = pd.merge(partsupp_df, supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
# 2. Join previous result with nation - S_NATIONKEY = N_NATIONKEY
parts_and_supplier_nation_df = pd.merge(parts_and_supplier_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
# 3. Join previous result with part - PS_PARTKEY = P_PARTKEY
full_df = pd.merge(parts_and_supplier_nation_df, part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Filtering for the minimum cost and sorting
full_df['PS_SUPPLYCOST'] = full_df['PS_SUPPLYCOST'].astype(float)
min_cost_df = full_df.loc[full_df.groupby('P_PARTKEY')['PS_SUPPLYCOST'].idxmin()]
result_df = min_cost_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Select and reorder columns
final_df = result_df[['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']]

# Output to CSV file
final_df.to_csv('query_output.csv', index=False)
