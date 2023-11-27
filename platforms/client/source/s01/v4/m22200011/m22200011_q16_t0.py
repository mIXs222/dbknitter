import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# Query for `partsupp` from MySQL
partsupp_query = """
SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT
FROM partsupp
"""
partsupp_df = pd.read_sql(partsupp_query, mysql_conn)

# Query for `supplier` from MySQL to get supplier keys to exclude
supplier_query = """
SELECT S_SUPPKEY
FROM supplier
WHERE S_COMMENT LIKE '%Customer%Complaints%'
"""
supplier_df = pd.read_sql(supplier_query, mysql_conn)
excluded_supp_keys = supplier_df['S_SUPPKEY'].tolist()

# Close MySQL connection
mysql_conn.close()

# Connect to Redis and get `part` as a DataFrame
redis_conn = DirectRedis(host='redis', port=6379, db=0)
part_encoded = redis_conn.get('part')
part_df = pd.read_msgpack(part_encoded)

# Filter DataFrames before merge
part_df_filtered = part_df[
    (part_df['P_BRAND'] != 'Brand#45') &
    (~part_df['P_TYPE'].str.contains('^MEDIUM POLISHED')) &
    (part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]
partsupp_df_filtered = partsupp_df[
    ~partsupp_df['PS_SUPPKEY'].isin(excluded_supp_keys)
]

# Merge filtered DataFrames on part key
merged_df = pd.merge(part_df_filtered, partsupp_df_filtered, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Group by and count distinct suppliers
result_df = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(SUPPLIER_CNT=pd.NamedAgg(column='PS_SUPPKEY', aggfunc=pd.Series.nunique))

# Reset index in place to correctly save as csv
result_df.reset_index(inplace=True)

# Sort the result
result_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Write the output to CSV
result_df.to_csv('query_output.csv', index=False)
