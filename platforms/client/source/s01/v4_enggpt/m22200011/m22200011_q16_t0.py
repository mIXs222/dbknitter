import pymysql
import direct_redis
import pandas as pd

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql', user='root', passwd='my-secret-pw', db='tpch'
)

# Execute MySQL query for supplier and partsupp
with mysql_conn.cursor() as cursor:
    mysql_query = '''
    SELECT ps.PS_PARTKEY, ps.PS_SUPPKEY
    FROM partsupp ps
    JOIN supplier s ON ps.PS_SUPPKEY = s.S_SUPPKEY
    WHERE s.S_COMMENT NOT LIKE '%Customer Complaints%'
    '''
    cursor.execute(mysql_query)
    partsupp_supplier_data = cursor.fetchall()

mysql_conn.close()

# Convert MySQL data to DataFrame
ps_supplier_df = pd.DataFrame(partsupp_supplier_data, columns=['PS_PARTKEY', 'PS_SUPPKEY'])

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get parts data from Redis
parts_data = r.get('part')
part_df = pd.read_msgpack(parts_data)

# Filter the part DataFrame
filtered_part_df = part_df[
    (part_df['P_BRAND'] != 'Brand#45') &
    (~part_df['P_TYPE'].str.startswith('MEDIUM POLISHED')) &
    (part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

# Merge the DataFrame
merged_df = ps_supplier_df.merge(filtered_part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Grouping and counting unique suppliers
grouped_df = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(SUPPLIER_CNT=pd.NamedAgg(column='PS_SUPPKEY', aggfunc='nunique'))

# Sorting the results
sorted_df = grouped_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True]).reset_index()

# Write the output to a CSV file
sorted_df.to_csv('query_output.csv', index=False)
