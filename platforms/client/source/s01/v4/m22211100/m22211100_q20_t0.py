# query.py
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
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query Redis for Parts
parts_df = pd.DataFrame(eval(redis_client.get('part')))
forest_parts = parts_df[parts_df['P_NAME'].str.startswith('forest')]

# Query MongoDB for PartsSupp
partsupp_suppliers = mongo_db['partsupp'].find(
    {'PS_PARTKEY': {'$in': list(forest_parts['P_PARTKEY'])}}
)

# Filter PartsSupp for supplier keys and filter by PS_AVAILQTY
supp_keys_availability = {}
for ps in partsupp_suppliers:
    if ps['PS_AVAILQTY'] > 0:
        supp_keys_availability[ps['PS_SUPPKEY']] = ps['PS_AVAILQTY']

# Query MySQL for LineItem
query_lineitem = """
SELECT
    L_PARTKEY, L_SUPPKEY, SUM(L_QUANTITY)
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01'
GROUP BY L_PARTKEY, L_SUPPKEY
"""
mysql_cursor.execute(query_lineitem)
lineitem_availability = {}
for L_PARTKEY, L_SUPPKEY, sum_quantity in mysql_cursor.fetchall():
    if L_SUPPKEY in supp_keys_availability:
        if supp_keys_availability[L_SUPPKEY] > 0.5 * sum_quantity:
            lineitem_availability[L_SUPPKEY] = True

# Filter suppliers
suppliers = list(lineitem_availability.keys())

# Query Redis for Nations
nations_df = pd.DataFrame(eval(redis_client.get('nation')))
canada_nation = nations_df[nations_df['N_NAME'] == 'CANADA']

# Query MongoDB for Supplier
supplier_cursor = mongo_db['supplier'].find(
    {'S_SUPPKEY': {'$in': suppliers}, 'S_NATIONKEY': {'$in': list(canada_nation['N_NATIONKEY'])}}
)

# Create a list of suitable suppliers
supplier_list = []
for supplier in supplier_cursor:
    supplier_list.append((supplier['S_NAME'], supplier['S_ADDRESS']))

# Convert to DataFrame and sort
supplier_df = pd.DataFrame(supplier_list, columns=['S_NAME', 'S_ADDRESS'])
supplier_df_sorted = supplier_df.sort_values('S_NAME')

# Write to CSV
supplier_df_sorted.to_csv('query_output.csv', index=False)

# Clean up
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
