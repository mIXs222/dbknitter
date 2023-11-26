import pandas as pd
import pymysql
import pymongo
from direct_redis import DirectRedis

# Establish connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
# Establish connection to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
# Establish connection to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch parts from MySQL
part_query = "SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'"
parts_df = pd.read_sql(part_query, mysql_conn)

# Fetch partsupp from MongoDB
partsupp_df = pd.DataFrame(list(mongodb_db.partsupp.find(
    {'PS_PARTKEY': {'$in': parts_df['P_PARTKEY'].tolist()}}
)))

# Fetch lineitem from MongoDB
lineitem_df = pd.DataFrame(list(mongodb_db.lineitem.find(
    {
        'L_PARTKEY': {'$in': parts_df['P_PARTKEY'].tolist()},
        'L_SUPPKEY': {'$in': partsupp_df['PS_SUPPKEY'].tolist()},
        'L_SHIPDATE': {'$gte': '1994-01-01', '$lt': '1995-01-01'}
    },
    {'L_PARTKEY': 1, 'L_SUPPKEY': 1, 'L_QUANTITY': 1}
)))

# Perform the subquery calculation for PS_AVAILQTY
lineitem_grouped = lineitem_df.groupby(['L_PARTKEY', 'L_SUPPKEY'])
half_sum_qty = lineitem_grouped['L_QUANTITY'].sum() * 0.5
half_sum_qty_dict = half_sum_qty.to_dict()
partsupp_df = partsupp_df[partsupp_df.apply(
    lambda x: x['PS_AVAILQTY'] > half_sum_qty_dict.get((x['PS_PARTKEY'], x['PS_SUPPKEY']), 0), axis=1
)]

# Fetch and filter suppliers from Redis.
supplier_df = pd.read_json(redis_client.get('supplier'))
supplier_df = supplier_df[supplier_df['S_SUPPKEY'].isin(partsupp_df['PS_SUPPKEY'])]

# Fetch and filter nation from Redis.
nation_df = pd.read_json(redis_client.get('nation'))
nation_df = nation_df[nation_df['N_NAME'] == 'CANADA']

# Perform the final join and filtering
final_df = supplier_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
final_df.sort_values(by='S_NAME', inplace=True)

# Select the required columns and write to CSV
output_df = final_df[['S_NAME', 'S_ADDRESS']]
output_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongodb_client.close()
