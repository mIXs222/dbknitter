import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# Connect to Redis
redis_conn_info = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}
redis_client = DirectRedis(**redis_conn_info)

# Execute query on MySQL to fetch supplier data not like '%Customer%Complaints%'
with pymysql.connect(**mysql_conn_info) as mysql_connection:
    with mysql_connection.cursor() as cursor:
        cursor.execute("""SELECT S_SUPPKEY FROM supplier WHERE S_COMMENT NOT LIKE '%Customer%Complaints%'""")
        suppliers = cursor.fetchall()
    supplier_df = pd.DataFrame(suppliers, columns=['S_SUPPKEY'])

# Fetch 'part' and 'partsupp' tables from Redis as DataFrames
part_df = pd.read_msgpack(redis_client.get('part'))
partsupp_df = pd.read_msgpack(redis_client.get('partsupp'))

# Filter out the 'part' table based on the conditions
filtered_part_df = part_df[
    (part_df['P_BRAND'] != 'Brand#45') & 
    (~part_df['P_TYPE'].str.startswith('MEDIUM POLISHED')) &
    (part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

# Join Redis tables and filter out the suppliers based on supplier_df
join_df = partsupp_df.merge(filtered_part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')
filtered_join_df = join_df[~join_df['PS_SUPPKEY'].isin(supplier_df['S_SUPPKEY'])]

# Perform the aggregation
result_df = (
    filtered_join_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])
    .agg(SUPPLIER_CNT=pd.NamedAgg(column='PS_SUPPKEY', aggfunc='nunique'))
    .reset_index()
    .sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])
)

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)
