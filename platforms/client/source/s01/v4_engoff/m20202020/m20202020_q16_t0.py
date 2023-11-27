# python_code.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connection information for MySQL and Redis
mysql_connection_info = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "db": "tpch",
}

# Connect to MySQL
connection = pymysql.connect(**mysql_connection_info)
try:
    # Fetch suppliers from MySQL
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM supplier WHERE S_COMMENT NOT LIKE '%%Customer%%Complaints%%'")
        suppliers = pd.DataFrame(cursor.fetchall(), columns=["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"])

finally:
    connection.close()

# Connect to Redis
redis_connection_info = {
    "host": "redis",
    "port": 6379,
    "db": 0,
}
redis_client = DirectRedis(**redis_connection_info)

# Fetch parts and partsupp from Redis
parts_df = pd.read_json(redis_client.get('part'), orient='records')
partsupp_df = pd.read_json(redis_client.get('partsupp'), orient='records')

# Filter parts by specified attributes and prepare the size set
sizes = {49, 14, 23, 45, 19, 3, 36, 9}
filtered_parts = parts_df[
    (parts_df['P_SIZE'].isin(sizes)) &
    (parts_df['P_TYPE'] != 'MEDIUM POLISHED') &
    (parts_df['P_BRAND'] != 'Brand#45')
]

# Merge parts with partsupp, and then merge with suppliers
merged_df = pd.merge(filtered_parts, partsupp_df, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')
merged_df = pd.merge(merged_df, suppliers, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Group by brand, type, and size, then count suppliers
grouped = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'], as_index=False).agg({'S_SUPPKEY': pd.Series.nunique})
grouped = grouped.rename(columns={'S_SUPPKEY': 'SUPPLIER_COUNT'})

# Sort the results
sorted_grouped = grouped.sort_values(by=['SUPPLIER_COUNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write out to 'query_output.csv'
sorted_grouped.to_csv('query_output.csv', index=False)
