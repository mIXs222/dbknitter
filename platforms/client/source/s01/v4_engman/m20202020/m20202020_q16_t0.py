import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(host="mysql",
                                   user="root",
                                   password="my-secret-pw",
                                   database="tpch")
try:
    # Get 'supplier' table where S_COMMENT does not indicate a complaint
    with mysql_connection.cursor() as cursor:
        cursor.execute("""
        SELECT S_SUPPKEY FROM supplier
        WHERE S_COMMENT NOT LIKE '%Customer%Complaints%'
        """)
        suppliers_without_complaints = cursor.fetchall()

finally:
    mysql_connection.close()

# Convert suppliers list to a DataFrame
supplier_df = pd.DataFrame(suppliers_without_complaints, columns=["S_SUPPKEY"])

# Connect to Redis
redis_connection = DirectRedis(host="redis", port=6379)

# Get 'part' and 'partsupp' tables and convert to DataFrames
part_df = pd.DataFrame(redis_connection.get('part'))
partsupp_df = pd.DataFrame(redis_connection.get('partsupp'))

# Filter 'part' DataFrame for the required specifications
parts_filtered_df = part_df[
    (part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9])) &
    (part_df['P_BRAND'] != 'Brand#45') &
    (part_df['P_TYPE'].str.contains('MEDIUM POLISHED', regex=False) == False)
]

# Merge 'partsupp' with filtered parts
partsupp_filtered_df = partsupp_df.merge(parts_filtered_df, how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Join 'partsupp' with 'supplier' on S_SUPPKEY and PS_SUPPKEY
result_df = partsupp_filtered_df.merge(supplier_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Group by P_BRAND, P_TYPE, P_SIZE and count distinct suppliers
final_df = result_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']) \
    .agg({'S_SUPPKEY': 'nunique'}) \
    .reset_index() \
    .rename(columns={'S_SUPPKEY': 'SupplierCount'}) \
    .sort_values(by=['SupplierCount', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write final results to CSV
final_df.to_csv('query_output.csv', index=False)
