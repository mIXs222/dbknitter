import pandas as pd
import pymysql
import direct_redis

# Function to connect to MySQL
def get_mysql_connection(db_name, user, password, host):
    return pymysql.connect(host=host, user=user, passwd=password, db=db_name)

# Function to fetch the MySQL table data
def fetch_mysql_data(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        field_names = [i[0] for i in cursor.description]
        return pd.DataFrame(result, columns=field_names)

# Connect to MySQL and fetch nation table
conn_mysql = get_mysql_connection('tpch', 'root', 'my-secret-pw', 'mysql')
nation_df = fetch_mysql_data(conn_mysql, 'SELECT * FROM nation')
conn_mysql.close()

# Connect to Redis and fetch supplier and partsupp tables
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
supplier_df = pd.read_json(r.get('supplier'))
partsupp_df = pd.read_json(r.get('partsupp'))

# Filter nation to 'GERMANY'
nation_df = nation_df[nation_df.N_NAME == 'GERMANY']

# Merge DataFrames
merged_df = (partsupp_df.merge(supplier_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
             .merge(nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY'))

# Calculate the aggregate value
merged_df['VALUE'] = merged_df['PS_SUPPLYCOST'] * merged_df['PS_AVAILQTY']
agg_value = merged_df.groupby('PS_PARTKEY')['VALUE'].sum().reset_index()
total_value = agg_value['VALUE'].sum() * 0.0001000000

# Filter based on HAVING condition
final_df = agg_value[agg_value['VALUE'] > total_value]

# Sort and write to CSV
final_df.sort_values('VALUE', ascending=False, inplace=True)
final_df.to_csv('query_output.csv', index=False)
