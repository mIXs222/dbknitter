# query.py
import pandas as pd
import pymysql
from direct_redis import DirectRedis

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Fetch data from MySQL
def fetch_mysql_data():
    with mysql_conn.cursor() as cursor:
        query = """
        SELECT P_PARTKEY, P_MFGR, P_TYPE, P_SIZE
        FROM part
        WHERE P_SIZE=15 AND P_TYPE LIKE '%BRASS'
        """
        cursor.execute(query)
        parts = pd.DataFrame(cursor.fetchall(), columns=['P_PARTKEY', 'P_MFGR', 'P_TYPE', 'P_SIZE'])

        query = """
        SELECT N_NATIONKEY, N_NAME, N_REGIONKEY
        FROM nation
        """
        cursor.execute(query)
        nations = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY'])

        query = """
        SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST
        FROM partsupp
        """
        cursor.execute(query)
        partsupps = pd.DataFrame(cursor.fetchall(), columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_SUPPLYCOST'])

    return parts, nations, partsupps

# Fetch data from Redis
def fetch_redis_data():
    redis_client = DirectRedis(host='redis', port=6379, db=0)

    # Fetch and decode data, convert to DataFrame
    region_data = pd.read_json(redis_client.get('region').decode('utf-8'))
    supplier_data = pd.read_json(redis_client.get('supplier').decode('utf-8'))

    return region_data, supplier_data

# Get data from all sources
parts_df, nations_df, partsupps_df = fetch_mysql_data()
region_df, supplier_df = fetch_redis_data()

# Merge data from different sources
merged_df = (
    parts_df.merge(partsupps_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
    .merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
    .merge(nations_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
)

# Filter to only include rows where R_NAME is 'EUROPE'
merged_df = merged_df[merged_df['R_NAME'] == 'EUROPE']

# Get the minimum PS_SUPPLYCOST per P_PARTKEY for filtered data
min_cost_df = merged_df.groupby('P_PARTKEY')['PS_SUPPLYCOST'].min().reset_index()
min_cost_df.rename(columns={'PS_SUPPLYCOST': 'MIN_PS_SUPPLYCOST'}, inplace=True)

# Merge to get rows with minimum PS_SUPPLYCOST
final_df = merged_df.merge(min_cost_df, how='inner', left_on=['P_PARTKEY', 'PS_SUPPLYCOST'],
                           right_on=['P_PARTKEY', 'MIN_PS_SUPPLYCOST'])

# Select and reorder columns
result_df = final_df[
    ['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']
]

# Sort data
result_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)

# Close MySQL connection
mysql_conn.close()
