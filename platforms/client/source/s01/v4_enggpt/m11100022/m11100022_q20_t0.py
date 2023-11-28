# Dependencies: pymysql, pymongo, pandas, direct_redis
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT S_SUPPKEY, S_NAME, S_ADDRESS
        FROM supplier
        WHERE S_SUPPKEY IN (
            SELECT PS_SUPPKEY
            FROM partsupp
            WHERE PS_PARTKEY IN (
                SELECT P_PARTKEY
                FROM part
                WHERE P_NAME LIKE 'forest%'
            )
        )
    """)
    suppliers = cursor.fetchall()
    df_supplier = pd.DataFrame(suppliers, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS'])
mysql_conn.close()

# Mongodb connection and query
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client['tpch']
canada_nation = mongodb.nation.find_one({'N_NAME': 'CANADA'}, {'N_NATIONKEY': 1})
supplier_keys = list(df_supplier['S_SUPPKEY'])
df_supplier = df_supplier[df_supplier['S_SUPPKEY'].isin(supplier_keys)]
df_supplier = df_supplier.sort_values('S_NAME')

# Redis connection and data retrieval
redis_client = DirectRedis(host='redis', port=6379, db=0)
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Subquery to get the sum of line item quantities per part-supplier combination
threshold_quantities = lineitem_df.loc[
    (lineitem_df['L_SHIPDATE'] >= '1994-01-01') &
    (lineitem_df['L_SHIPDATE'] <= '1995-01-01')
].groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum() / 2

# Filtering the supplier DataFrame with other conditions
threshold_dict = threshold_quantities.to_dict()
df_supplier['PS_AVAILQTY_THRESHOLD'] = df_supplier.apply(lambda row: threshold_dict.get((row['PS_PARTKEY'], row['S_SUPPKEY']), 0), axis=1)
df_supplier = df_supplier[df_supplier['PS_AVAILQTY_THRESHOLD'] >= 50]

# Save the results to a CSV file
df_supplier[['S_NAME', 'S_ADDRESS']].to_csv('query_output.csv', index=False)
