import pandas as pd
import pymysql
from sqlalchemy import create_engine
import direct_redis

# Connect to MySQL
mysql_conn_info = {
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
    'database': 'tpch'
}
mysql_connection = pymysql.connect(**mysql_conn_info)

# Create a SQLAlchemy engine
mysql_engine = create_engine(f"mysql+pymysql://{mysql_conn_info['user']}:{mysql_conn_info['password']}@{mysql_conn_info['host']}/{mysql_conn_info['database']}")

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetching MySQL data for 'nation', 'region', 'part', 'supplier'
nation_df = pd.read_sql("SELECT * FROM nation", mysql_engine)
region_df = pd.read_sql("SELECT * FROM region", mysql_engine)
part_df = pd.read_sql("SELECT * FROM part WHERE P_TYPE = 'BRASS' AND P_SIZE = 15", mysql_engine)
supplier_df = pd.read_sql("SELECT * FROM supplier", mysql_engine)

# Combine MySQL data according to the query requirement
mysql_combined_df = pd.merge(supplier_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
mysql_combined_df = pd.merge(mysql_combined_df, region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
mysql_combined_df = mysql_combined_df[mysql_combined_df['R_NAME'] == 'EUROPE']
mysql_combined_df = pd.merge(mysql_combined_df, part_df, how='cross')

# Fetching Redis data for 'partsupp'
partsupp_df = pd.read_json(redis_connection.get('partsupp'), orient='records')

# Join and filter the partsupp data by keys and find minimum cost
partsupp_df_min_cost = partsupp_df.groupby(['PS_PARTKEY'])['PS_SUPPLYCOST'].min().reset_index()
partsupp_min_cost_df = pd.merge(partsupp_df, partsupp_df_min_cost, on=['PS_PARTKEY', 'PS_SUPPLYCOST'])

# Now, combine the data from MySQL and Redis
final_output = pd.merge(mysql_combined_df, partsupp_min_cost_df, left_on=['P_PARTKEY', 'S_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])

# Sort the final data as per the query requirement
final_output_sorted = final_output.sort_values(
    by=['PS_SUPPLYCOST', 'S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'],
    ascending=[True, False, True, True, True]
)

# Select required columns
final_output_columns = [
    'S_ACCTBAL', 'S_NAME', 'N_NAME', 
    'P_PARTKEY', 'P_MFGR', 
    'S_ADDRESS', 'S_PHONE', 'S_COMMENT'
]

final_output_final = final_output_sorted[final_output_columns]

# Write the output to a CSV file
final_output_final.to_csv('query_output.csv', index=False)
