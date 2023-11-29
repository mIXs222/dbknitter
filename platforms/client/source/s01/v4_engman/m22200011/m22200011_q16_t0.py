import pymysql
import redis
import pandas as pd

# Establish a connection to the mysql database
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Execute the query for supplier and partsupp tables in mysql
with mysql_connection.cursor() as cursor:
    mysql_query = """
    SELECT S_SUPPKEY
    FROM supplier
    WHERE S_COMMENT NOT LIKE '%Customer%Complaints%'
    """
    cursor.execute(mysql_query)
    suppliers_with_no_complaints = cursor.fetchall()

# List of suppliers who have no complaints
suppliers_list = [row[0] for row in suppliers_with_no_complaints]

# Establish a connection to the redis database
r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
part_keys = r.keys('part:*')

# Extract parts from redis and convert to a pandas DataFrame
parts_data = []
for key in part_keys:
    part = r.hgetall(key)
    if (int(part['P_SIZE']) in [49, 14, 23, 45, 19, 3, 36, 9] and
            'MEDIUM POLISHED' not in part['P_TYPE'] and
            part['P_BRAND'] != 'Brand#45'):
        parts_data.append(part)

# Create DataFrame
parts_df = pd.DataFrame(parts_data)

# Convert P_PARTKEY to int for later processing
parts_df['P_PARTKEY'] = parts_df['P_PARTKEY'].astype(int)

# Execute the query for partsupp table in mysql
with mysql_connection.cursor() as cursor:
    mysql_query = """
    SELECT PS_PARTKEY, PS_SUPPKEY
    FROM partsupp
    """
    cursor.execute(mysql_query)
    partsupp_data = cursor.fetchall()

# Convert to DataFrame
partsupp_df = pd.DataFrame(partsupp_data, columns=['PS_PARTKEY', 'PS_SUPPKEY'])

# Close the mysql connection
mysql_connection.close()

# Filter partsupp DataFrame for suppliers with no complaints
partsupp_df = partsupp_df[partsupp_df['PS_SUPPKEY'].isin(suppliers_list)]

# Merge parts and partsupp DataFrames
merged_df = parts_df.merge(partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Group and count
output_df = (merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])
             .size()
             .reset_index(name='supplier_count')
             .sort_values(by=['supplier_count', 'P_BRAND', 'P_TYPE', 'P_SIZE'],
                          ascending=[False, True, True, True]))

# Save the result to query_output.csv
output_df.to_csv('query_output.csv', index=False)
