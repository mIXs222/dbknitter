import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Execute query to get supplier and nation data from mysql db
with mysql_conn.cursor() as cursor:
    cursor.execute("""SELECT S_SUPPKEY, N_NATIONKEY FROM nation JOIN supplier ON N_NATIONKEY = S_NATIONKEY WHERE N_NAME = 'GERMANY'""")
    mysql_data = cursor.fetchall()

# Convert mysql data to DataFrame
mysql_df = pd.DataFrame(mysql_data, columns=['S_SUPPKEY', 'N_NATIONKEY'])

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get partsupp data from redis db
partsupp_data = redis_conn.get('partsupp')
partsupp_df = pd.read_json(partsupp_data)

# Merge to simulate JOIN between mysql and redis data
merged_df = partsupp_df.merge(mysql_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Computing the aggregate value
merged_df['VALUE'] = merged_df['PS_SUPPLYCOST'] * merged_df['PS_AVAILQTY']
grouped_df = merged_df.groupby('PS_PARTKEY').agg(TOTAL_VALUE=pd.NamedAgg(column='VALUE', aggfunc='sum'))
total_value = grouped_df['TOTAL_VALUE'].sum() * 0.0001000000
filtered_df = grouped_df[grouped_df['TOTAL_VALUE'] > total_value].sort_values(by='TOTAL_VALUE', ascending=False).reset_index()

# Write the output to a CSV file
filtered_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
redis_conn.close()
