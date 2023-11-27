import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_parts = mongo_db['part']

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for nation and supplier
mysql_cursor.execute("""
SELECT 
    s.S_SUPPKEY, s.S_NAME, sn.N_NAME
FROM 
    supplier s
JOIN 
    nation sn
ON 
    s.S_NATIONKEY = sn.N_NATIONKEY
WHERE 
    sn.N_NAME = 'CANADA';
""")
suppliers_in_canada = mysql_cursor.fetchall()
df_suppliers = pd.DataFrame(suppliers_in_canada, columns=['S_SUPPKEY', 'S_NAME', 'N_NAME'])

# Query MongoDB for part
part_docs = mongo_parts.find({"P_NAME": {"$regex": "forest"}}, {'_id': False})
df_parts = pd.DataFrame(list(part_docs))

# Query Redis for partsupp and lineitem
df_partsupp = pd.read_json(redis_conn.get('partsupp'), orient='records')
df_lineitem = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Filter lineitems between the specified dates
df_lineitem_filtered = df_lineitem[
    (pd.to_datetime(df_lineitem['L_SHIPDATE']) >= pd.Timestamp('1994-01-01')) &
    (pd.to_datetime(df_lineitem['L_SHIPDATE']) < pd.Timestamp('1995-01-01'))
]

# Combine the lineitem and partsupp data with the part data
df_lineitem_parts = pd.merge(
    df_lineitem_filtered,
    df_partsupp,
    how='inner',
    left_on=['L_PARTKEY', 'L_SUPPKEY'],
    right_on=['PS_PARTKEY', 'PS_SUPPKEY']
)
df_lineitem_parts = pd.merge(
    df_lineitem_parts,
    df_parts,
    how='inner',
    left_on='L_PARTKEY',
    right_on='P_PARTKEY'
)

# Compute total quantity per supplier for relevant parts
df_total_qty = df_lineitem_parts.groupby('L_SUPPKEY').agg({'L_QUANTITY': 'sum'}).reset_index()
df_total_qty.columns = ['S_SUPPKEY', 'TOTAL_QTY']

# Identify suppliers with excess (more than 50% of parts)
df_excess_suppliers = df_total_qty[df_total_qty['TOTAL_QTY'] > df_total_qty['TOTAL_QTY'].sum() * 0.5]

# Join to get supplier names
df_final = pd.merge(
    df_excess_suppliers,
    df_suppliers,
    how='inner',
    on='S_SUPPKEY'
)

# Write output to CSV file
df_final.to_csv('query_output.csv', index=False)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
redis_conn.close()
