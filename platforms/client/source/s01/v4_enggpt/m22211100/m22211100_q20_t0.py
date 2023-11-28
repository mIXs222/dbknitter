# File: query_analysis.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv

# MySQL Connection
mysql_conn = pymysql.connect(host="mysql", user="root", password="my-secret-pw", db="tpch")
with mysql_conn:
    with mysql_conn.cursor() as cursor:
        cursor.execute("""
            SELECT L_PARTKEY, L_SUPPKEY
            FROM lineitem
            WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
        """)
        lineitem_records = cursor.fetchall()
        cursor.execute("""
            SELECT L_PARTKEY, L_SUPPKEY, SUM(L_QUANTITY) / 2 AS THRESHOLD_QUANTITY
            FROM lineitem
            WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
            GROUP BY L_PARTKEY, L_SUPPKEY
        """)
        threshold_quantities = cursor.fetchall()
threshold_df = pd.DataFrame(threshold_quantities, columns=['L_PARTKEY', 'L_SUPPKEY', 'THRESHOLD_QUANTITY'])

# MongoDB Connection
mongo_conn = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_conn["tpch"]

supplier_cursor = mongo_db["supplier"].find({})
supplier_df = pd.DataFrame(list(supplier_cursor))

partsupp_cursor = mongo_db["partsupp"].find({"PS_AVAILQTY": {"$gt": 0}})
partsupp_df = pd.DataFrame(list(partsupp_cursor))

# Redis Connection
redis_conn = DirectRedis(host="redis", port=6379, db=0)
nation_data = redis_conn.get('nation')
part_data = redis_conn.get('part')
nation_df = pd.read_json(nation_data)
part_df = pd.read_json(part_data)

# Combine and analyze data
# Filter out nation == 'CANADA'
canada_nations = nation_df[nation_df['N_NAME'] == 'CANADA']
canada_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(canada_nations['N_NATIONKEY'])]

# Parts that start with 'forest'
part_suppliers = partsupp_df[partsupp_df['PS_PARTKEY'].isin(part_df[part_df['P_NAME'].str.startswith('forest')]['P_PARTKEY'])]

# Join to filter based on threshold
threshold_part_suppliers = threshold_df.merge(part_suppliers, how='inner', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])

final_suppliers = canada_suppliers[canada_suppliers['S_SUPPKEY'].isin(threshold_part_suppliers['PS_SUPPKEY'])][['S_NAME', 'S_ADDRESS']]

# Sort by supplier name
final_suppliers_sorted = final_suppliers.sort_values(by='S_NAME')

# Write output to CSV
final_suppliers_sorted.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close connections
mysql_conn.close()
mongo_conn.close()
redis_conn.close()
