# python_code.py
import pymysql
import direct_redis
import pandas as pd

# Connect to MySQL database
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor
)

try:
    # Query for MySQL
    mysql_query = """
    SELECT
        nation.N_NATIONKEY,
        nation.N_NAME,
        region.R_NAME,
        part.P_PARTKEY,
        part.P_MFGR,
        part.P_SIZE,
        part.P_TYPE
    FROM
        part,
        nation,
        region
    WHERE
        part.P_SIZE = 15 AND
        part.P_TYPE LIKE '%BRASS' AND
        nation.N_REGIONKEY = region.R_REGIONKEY AND
        region.R_NAME = 'EUROPE'
    """

    # Execute MySQL query
    with mysql_connection.cursor() as cursor:
        cursor.execute(mysql_query)
        mysql_data = cursor.fetchall()

    # Convert MySQL data to DataFrame
    mysql_df = pd.DataFrame(mysql_data, columns=['N_NATIONKEY', 'N_NAME', 'R_NAME',
                                                 'P_PARTKEY', 'P_MFGR', 'P_SIZE', 'P_TYPE'])
finally:
    mysql_connection.close()

# Connect to Redis database
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve tables from Redis and convert to DataFrame
supplier_df = pd.read_json(redis_connection.get('supplier'), orient='records')
partsupp_df = pd.read_json(redis_connection.get('partsupp'), orient='records')

# Merge the dataframes from MySQL and Redis
merged_df = supplier_df.merge(partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
merged_df = merged_df.merge(mysql_df, left_on=['S_NATIONKEY', 'PS_PARTKEY'],
                            right_on=['N_NATIONKEY', 'P_PARTKEY'])

# Get the minimum PS_SUPPLYCOST per PARTKEY for region 'EUROPE'
min_supplycost_df = merged_df.groupby('P_PARTKEY')['PS_SUPPLYCOST'].min().reset_index()
min_supplycost_df.columns = ['P_PARTKEY', 'MIN_SUPPLYCOST']

# Merge to filter rows with minimum PS_SUPPLYCOST
merged_df = merged_df.merge(min_supplycost_df, on='P_PARTKEY', how='inner')
merged_df = merged_df[merged_df['PS_SUPPLYCOST'] == merged_df['MIN_SUPPLYCOST']]

# Select only required columns and sort
result_df = merged_df[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY',
                       'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]
result_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True],
                      inplace=True)

# Writing to CSV file
result_df.to_csv('query_output.csv', index=False)
