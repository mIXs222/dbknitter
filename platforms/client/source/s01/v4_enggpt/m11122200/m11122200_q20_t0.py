import pymysql
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# MongoDB Connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Redis Connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# MySQL Query
mysql_query = """
SELECT l.L_PARTKEY, l.L_SUPPKEY, l.L_QUANTITY
FROM lineitem l
WHERE l.L_SHIPDATE >= '1994-01-01' AND l.L_SHIPDATE <= '1995-01-01';
"""

# Execute MySQL Query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    lineitem_data = cursor.fetchall()

lineitem_df = pd.DataFrame(lineitem_data, columns=['L_PARTKEY', 'L_SUPPKEY', 'L_QUANTITY'])

# MongoDB Queries
nation_docs = mongodb_db['nation'].find({'N_NAME': 'CANADA'})

# Create a DataFrame for the nation table
nation_df = pd.DataFrame(nation_docs)

# MongoDB Query for 'part' collection
part_docs = mongodb_db['part'].find({'P_NAME': {'$regex': '^forest'}})

# Create a DataFrame for the part table
part_df = pd.DataFrame(part_docs)

# Get supplier and partsupp Data from Redis
supplier_df = pd.read_json(redis_conn.get('supplier').decode('utf-8'))
partsupp_df = pd.read_json(redis_conn.get('partsupp').decode('utf-8'))

# Filter partsupp based on part keys from MongoDB
filtered_partsupp_df = partsupp_df[partsupp_df['PS_PARTKEY'].isin(part_df['P_PARTKEY'])]

# Calculate 50% threshold quantity for partsupp by part and supplier
threshold_quantities = lineitem_df.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum() * 0.5
threshold_quantities = threshold_quantities.reset_index()
threshold_quantities.columns = ['PS_PARTKEY', 'PS_SUPPKEY', 'THRESHOLD_QUANTITY']

# Merge partsupp information and threshold quantities
threshold_partsupp_df = pd.merge(filtered_partsupp_df, threshold_quantities,
                                 on=['PS_PARTKEY', 'PS_SUPPKEY'])

# Filter supplier keys based on available quantities above threshold
eligible_suppliers_df = threshold_partsupp_df[threshold_partsupp_df['PS_AVAILQTY'] > threshold_partsupp_df['THRESHOLD_QUANTITY']]

# Merge with supplier dataframe to get supplier details
eligible_supplier_details_df = pd.merge(supplier_df, eligible_suppliers_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Filter based on nation and select the required fields
final_df = pd.merge(eligible_supplier_details_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
final_df = final_df[['S_NAME', 'S_ADDRESS']].sort_values('S_NAME')

# Write the results to CSV
final_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongodb_client.close()
redis_conn.close()
