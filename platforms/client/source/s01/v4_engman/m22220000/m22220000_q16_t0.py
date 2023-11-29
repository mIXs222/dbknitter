import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Query partsupp from MySQL
ps_query = '''SELECT PS_PARTKEY, PS_SUPPKEY FROM partsupp WHERE PS_PARTKEY NOT IN (
                SELECT PS_PARTKEY FROM partsupp 
                JOIN supplier ON PS_SUPPKEY = S_SUPPKEY
                WHERE S_COMMENT LIKE '%%complaints%%'
              )'''
ps_df = pd.read_sql(ps_query, mysql_connection)

# Close MySQL connection
mysql_connection.close()

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379)

# Get part and supplier tables from Redis
part_df = pd.read_json(redis_connection.get('part'), orient='index')
supplier_df = pd.read_json(redis_connection.get('supplier'), orient='index')

# Filter part table according to conditions
filtered_part_df = part_df[
    (~part_df["P_BRAND"].eq("Brand#45")) &
    (~part_df["P_TYPE"].str.contains("MEDIUM POLISHED")) &
    (part_df["P_SIZE"].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

# Filter partsupp table to parts that meet customer's requirements
compatible_partsupp_df = ps_df[ps_df["PS_PARTKEY"].isin(filtered_part_df["P_PARTKEY"])]

# Filter suppliers that have not had complaints
supplier_no_complaints_df = supplier_df[~supplier_df["S_COMMENT"].str.contains("complaints")]

# Find the suppliers who can provide the parts
result_df = compatible_partsupp_df[
    compatible_partsupp_df["PS_SUPPKEY"].isin(supplier_no_complaints_df["S_SUPPKEY"])
]

# Aggregate the result by part attributes
aggregated_result = (
    filtered_part_df.merge(result_df, left_on="P_PARTKEY", right_on="PS_PARTKEY")
    .groupby(["P_BRAND", "P_TYPE", "P_SIZE"])
    .agg({"PS_SUPPKEY": "nunique"})
    .rename(columns={"PS_SUPPKEY": "supplier_count"})
    .sort_values(by=["supplier_count", "P_BRAND", "P_TYPE", "P_SIZE"], ascending=[False, True, True, True])
    .reset_index()
)

# Save the result to CSV
aggregated_result.to_csv('query_output.csv', index=False)
