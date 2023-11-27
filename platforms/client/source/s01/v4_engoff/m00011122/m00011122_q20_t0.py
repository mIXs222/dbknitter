# python_code.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn.cursor() as cursor:
    # Execute the query - select only the relevant columns to minimize data transfer
    cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'CANADA'")
    canada_nation_key = cursor.fetchone()[0]

    cursor.execute("SELECT S_SUPPKEY FROM supplier WHERE S_NATIONKEY = %s", (canada_nation_key,))
    supplier_keys = cursor.fetchall()

    cursor.execute("SELECT P_PARTKEY, P_NAME FROM part WHERE P_NAME LIKE 'forest%'")
    part_details = cursor.fetchall()

mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client.tpch

# Get the suppliers from MySQL who are in Canada
supplier_keys = [key[0] for key in supplier_keys]

# Get the part keys with 'forest%' name from MySQL and make a dict for quick lookup
part_keys = {key: name for key, name in part_details}

# Get the parts supplied by these suppliers from MongoDB
partsupp_docs = mongodb.partsupp.find({'PS_SUPPKEY': {'$in': supplier_keys}},
                                     {'PS_PARTKEY': 1, 'PS_SUPPKEY': 1, 'PS_AVAILQTY': 1})

# Make a dict of parts supplied by the suppliers with quantities
parts_supplied = {}
for doc in partsupp_docs:
    if doc['PS_PARTKEY'] in part_keys:
        parts_supplied[(doc['PS_PARTKEY'], doc['PS_SUPPKEY'])] = doc['PS_AVAILQTY']

redis_conn = DirectRedis(host='redis', port=6379, db=0)
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Filter the lineitems for the given date range and with the part keys we are interested in
date_mask = (lineitem_df['L_SHIPDATE'] >= '1994-01-01') & (lineitem_df['L_SHIPDATE'] < '1995-01-01')
relevant_lineitems = lineitem_df[date_mask & lineitem_df['L_PARTKEY'].isin(part_keys.keys())]

# Aggregate the quantities shipped by part key and supplier key
shipped_parts_grouped = relevant_lineitems.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum().reset_index()

# Determine the suppliers with excess parts
excess_suppliers = []
for key, quantity in parts_supplied.items():
    part_shipped = shipped_parts_grouped[(shipped_parts_grouped['L_PARTKEY'] == key[0]) & 
                                         (shipped_parts_grouped['L_SUPPKEY'] == key[1])]
    if part_shipped.empty or quantity > 1.5 * part_shipped['L_QUANTITY'].iloc[0]:
        excess_suppliers.append({
            'PS_PARTKEY': key[0],
            'PS_SUPPKEY': key[1],
            'PS_AVAILQTY': quantity,
            'P_NAME': part_keys[key[0]]
        })

# Create dataframe from excess suppliers and export to CSV
excess_suppliers_df = pd.DataFrame(excess_suppliers)
excess_suppliers_df.to_csv('query_output.csv', index=False)
