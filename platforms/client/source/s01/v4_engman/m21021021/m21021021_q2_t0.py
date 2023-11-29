import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Connect to MongoDB 
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for parts
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(
    "SELECT P_PARTKEY, P_MFGR FROM part WHERE P_TYPE='BRASS' AND P_SIZE=15"
)
parts_data = mysql_cursor.fetchall()

# Create a DataFrame for parts data
parts_df = pd.DataFrame(parts_data, columns=['P_PARTKEY', 'P_MFGR'])

# Query MongoDB for regions and partsupp
region_docs = mongo_db.region.find({'R_NAME': 'EUROPE'})
europe_region_key = [doc['R_REGIONKEY'] for doc in region_docs]

nation_docs = redis_conn.get('nation')
nation_df = pd.read_json(nation_docs)

# Filter nations for EUROPE region
europe_nations = nation_df[nation_df['N_REGIONKEY'].isin(europe_region_key)]

# Query MongoDB for partsupp data
partsupp_docs = mongo_db.partsupp.find({'PS_PARTKEY': {'$in': parts_df['P_PARTKEY'].tolist()}})
partsupp_df = pd.DataFrame(list(partsupp_docs))

# Get the minimum cost for each part
min_cost_df = partsupp_df.groupby('PS_PARTKEY').agg({'PS_SUPPLYCOST': 'min'}).reset_index()

# Get supplier keys for the minimum cost suppliers
min_cost_suppliers = partsupp_df[
    partsupp_df.set_index(['PS_PARTKEY', 'PS_SUPPLYCOST']).index.isin(min_cost_df.set_index(['PS_PARTKEY', 'PS_SUPPLYCOST']).index)
]['PS_SUPPKEY'].tolist()

# Get suppliers data
supplier_docs = redis_conn.get('supplier')
supplier_df = pd.read_json(supplier_docs)

# Filter suppliers for those in europe_nations and the minimum cost suppliers
europe_suppliers = supplier_df[
    (supplier_df['S_NATIONKEY'].isin(europe_nations['N_NATIONKEY'])) &
    (supplier_df['S_SUPPKEY'].isin(min_cost_suppliers))
]

# Merge to get the required data
merged_df = (
    europe_suppliers
    .merge(europe_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY', suffixes=('_SUPP', '_NAT'))
    .merge(parts_df, left_on='S_SUPPKEY', right_on='P_PARTKEY')
    .merge(min_cost_df, on='P_PARTKEY')
    .sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])
)

# Filter and rename columns as required by the query
final_df = merged_df[['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']]
final_df.to_csv('query_output.csv', index=False)
