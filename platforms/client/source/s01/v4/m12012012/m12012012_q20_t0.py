import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
nation_collection = mongo_db['nation']
supplier_collection = mongo_db['supplier']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load parts that match the condition from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT P_PARTKEY
        FROM part
        WHERE P_NAME LIKE 'forest%'
    """)
    partkeys = [row[0] for row in cursor.fetchall()]

# Load partsupp from Redis
partsupp_df = pd.read_json(redis_client.get('partsupp'), orient='records')

# Filter partsupp with the partkeys
partsupp_df = partsupp_df[partsupp_df['PS_PARTKEY'].isin(partkeys)]

# Load lineitem from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter lineitem based on the dates and join with partsupp
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1994-01-01') &
    (lineitem_df['L_SHIPDATE'] < '1995-01-01') &
    (lineitem_df['L_PARTKEY'].isin(partkeys))
]

# Group by partsupp keys and sum up quantity
grouped_lineitem = filtered_lineitem_df.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum().reset_index()
grouped_lineitem['total_quantity'] = grouped_lineitem['L_QUANTITY'] * 0.5

# Merge partsupp with lineitem quantities
merged_df = pd.merge(partsupp_df, grouped_lineitem, how='left', left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])
merged_df = merged_df[merged_df['PS_AVAILQTY'] > merged_df['total_quantity']]

# Get the suppkeys
suppkeys = merged_df['PS_SUPPKEY'].unique()

# Get suppliers from MongoDB
supplier_docs = list(supplier_collection.find({'S_SUPPKEY': {'$in': list(suppkeys)}}))

# Get nation key for 'CANADA'
nation_doc = nation_collection.find_one({'N_NAME': 'CANADA'})
if nation_doc:
    nation_key = nation_doc['N_NATIONKEY']

    # Filter suppliers by nation key
    qualifying_suppliers = [doc for doc in supplier_docs if doc['S_NATIONKEY'] == nation_key]
    
    # Create dataframe
    final_output = pd.DataFrame(qualifying_suppliers, columns=['S_NAME', 'S_ADDRESS'])
    
    # Order by S_NAME and save to CSV
    final_output.sort_values(by='S_NAME').to_csv('query_output.csv', index=False)
