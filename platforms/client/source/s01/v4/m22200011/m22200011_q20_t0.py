import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Establish connections to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Establish a connection to the MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Establish connection to the Redis database
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Fetch data from Redis
nation_df = pd.DataFrame(eval(redis_client.get('nation')))
part_df = pd.DataFrame(eval(redis_client.get('part')))

# Fetch data from MySQL
supplier_df = pd.read_sql("SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY FROM supplier", mysql_conn)
partsupp_df = pd.read_sql("SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY FROM partsupp", mysql_conn)

# Fetch data from MongoDB
lineitem_collection = mongo_db['lineitem']
lineitem_query = {
    'L_SHIPDATE': {
        '$gte': '1994-01-01',
        '$lt': '1995-01-01'
    }
}
lineitem_projection = {
    '_id': False,
    'L_PARTKEY': True,
    'L_SUPPKEY': True,
    'L_QUANTITY': True
}
lineitem_df = pd.DataFrame(list(lineitem_collection.find(lineitem_query, lineitem_projection)))

# Processing
# Select parts with names starting with 'forest'
part_forest_df = part_df[part_df['P_NAME'].str.startswith('forest')]

# Inner join to find partsupp entries matching the selected parts
partsupp_forest_df = partsupp_df[partsupp_df['PS_PARTKEY'].isin(part_forest_df['P_PARTKEY'])]

# Calculate the sum of L_QUANTITY from lineitem per PS_PARTKEY and PS_SUPPKEY
lineitem_grouped = lineitem_df.groupby(['L_PARTKEY', 'L_SUPPKEY']).agg({'L_QUANTITY': 'sum'}).reset_index()
lineitem_grouped['half_sum_quantity'] = lineitem_grouped['L_QUANTITY'] * 0.5

# Merge partsupp_forest with lineitem_grouped on PS_PARTKEY and PS_SUPPKEY, compare quantities
ps_li_merged = partsupp_forest_df.merge(lineitem_grouped, how='left', left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])
ps_li_filtered = ps_li_merged[ps_li_merged['PS_AVAILQTY'] > ps_li_merged['half_sum_quantity']]

# Filter suppliers based on PS_SUPPKEY and S_NATIONKEY = N_NATIONKEY of CANADA
suppliers_filtered = supplier_df[supplier_df['S_SUPPKEY'].isin(ps_li_filtered['PS_SUPPKEY'])]
nation_canada = nation_df[nation_df['N_NAME'] == 'CANADA']
suppliers_nation = suppliers_filtered.merge(nation_canada, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Select the relevant columns and order by name
result_df = suppliers_nation[['S_NAME', 'S_ADDRESS']].sort_values(by='S_NAME')

# Write out the result to query_output.csv
result_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
mongo_client.close()
