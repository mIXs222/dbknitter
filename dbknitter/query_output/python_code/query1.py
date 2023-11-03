import pandas as pd
from pymongo import MongoClient
import mysql.connector
import csv

def fetch_query(cursor):
    header = [i[0] for i in cursor.description]
    return [dict(zip(header, row)) for row in cursor]

# mongodb connection
mongo_client = MongoClient('mongodb://mongodb:27017')
mongo_db = mongo_client['tpch']
part_col = mongo_db['part']
partsupp_col = mongo_db['partsupp']

# mysql connection
mysql_conn = mysql.connector.connect(
    host='mysql',
    user='root',
    passwd='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# MongoDB queries
part_docs = part_col.find({
    "P_BRAND": {'$ne': 'Brand#45'},
    "P_TYPE": {'$not': {'$regex': '^MEDIUM POLISHED'}},
    "P_SIZE": {'$in': [49, 14, 23, 45, 19, 3, 36, 9]}
}, {"P_PARTKEY": 1, "P_BRAND": 1, "P_TYPE": 1, "P_SIZE": 1})
partsupp_docs = partsupp_col.find({}, {
    "PS_PARTKEY": 1,
    "PS_SUPPKEY": 1
})

# MySQL query
mysql_cursor.execute("""
SELECT S_SUPPKEY FROM SUPPLIER
WHERE S_COMMENT LIKE '%Customer%Complaints%'
""")
suppkeys_with_complaints = [i['S_SUPPKEY'] for i in fetch_query(mysql_cursor)]

# match data
result = []
for part in part_docs:
    for partsupp in partsupp_docs:
        if part["P_PARTKEY"] == partsupp["PS_PARTKEY"] and \
        partsupp["PS_SUPPKEY"] not in suppkeys_with_complaints:
            result.append({
                "P_BRAND": part["P_BRAND"],
                "P_TYPE": part["P_TYPE"],
                "P_SIZE": part["P_SIZE"],
                "SUPPLIER_CNT": len(set([i["PS_SUPPKEY"] for i in partsupp_docs]))
            })

# export to csv
df_result = pd.DataFrame(result)
df_result.drop_duplicates(inplace=True)
df_result.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)
df_result.to_csv('query_output.csv', index=False)
