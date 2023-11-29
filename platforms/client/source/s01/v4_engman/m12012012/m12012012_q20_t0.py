# Potential_Part_Promotion.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
from io import StringIO

# Establish connection to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Establish connection to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Establish connection to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for part details with the specified pattern
part_query = "SELECT P_PARTKEY, P_NAME FROM part WHERE P_NAME LIKE 'forest%'"
mysql_parts = pd.read_sql(part_query, mysql_conn)
mysql_conn.close()  # Close MySQL connection

# Query MongoDB for suppliers in Canada
suppliers_in_canada = list(mongo_db['nation'].find({'N_NAME': 'CANADA'}, {'N_NATIONKEY': 1}))
canada_key_list = [n['N_NATIONKEY'] for n in suppliers_in_canada]
suppliers = mongo_db['supplier'].find({'S_NATIONKEY': {'$in': canada_key_list}})
supplier_df = pd.DataFrame(suppliers)

# Get partsupp and lineitem from Redis
partsupp_df = pd.read_parquet(StringIO(redis_conn.get('partsupp')), engine='pyarrow')
lineitem_df = pd.read_parquet(StringIO(redis_conn.get('lineitem')), engine='pyarrow')

# Filter lineitem table for the date range and join with parts and suppliers
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem = lineitem_df[(lineitem_df['L_SHIPDATE'] >= datetime(1994, 1, 1)) &
                                (lineitem_df['L_SHIPDATE'] < datetime(1995, 1, 1))]

# Join the dataframes to find suppliers with more than 50% of the parts like the forest part
merged_part_lineitem = pd.merge(filtered_lineitem, mysql_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_partsupp_lineitem = pd.merge(partsupp_df, merged_part_lineitem, on=['PS_PARTKEY', 'PS_SUPPKEY'])
supplier_part_qty = merged_partsupp_lineitem.groupby(['S_SUPPKEY']).agg(
    total_qty=('PS_AVAILQTY', 'sum'),
    shipped_qty=('L_QUANTITY', 'sum')
).reset_index()

# Identify suppliers with excess forest part
supplier_part_qty['excess'] = supplier_part_qty['total_qty'] > (1.5 * supplier_part_qty['shipped_qty'])
suppliers_with_excess = supplier_part_qty[supplier_part_qty['excess']]

# Join with supplier dataframe to get supplier names
suppliers_with_excess = pd.merge(suppliers_with_excess, supplier_df, left_on='S_SUPPKEY', right_on='S_SUPPKEY')

# Write result to query_output.csv
suppliers_with_excess.to_csv('query_output.csv', index=False)
