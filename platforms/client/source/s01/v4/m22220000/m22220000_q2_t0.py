import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_db = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Read Redis tables
df_nation = pd.read_json(redis_client.get('nation'))
df_region = pd.read_json(redis_client.get('region'))
df_part = pd.read_json(redis_client.get('part'))
df_supplier = pd.read_json(redis_client.get('supplier'))

# Filter the necessary Redis tables
df_nation.region = df_nation.N_REGIONKEY
df_filtered_region = df_region[df_region.R_NAME == 'EUROPE']
df_filtered_part = df_part[(df_part.P_SIZE == 15) & (df_part.P_TYPE.str.endswith('BRASS'))]

# Perform necessary join operations in Redis tables
df_nation_region = df_nation.merge(df_filtered_region, left_on='region', right_on='R_REGIONKEY')
df_supplier_nation = df_supplier.merge(df_nation_region, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Execute the subquery on MySQL to find the minimum PS_SUPPLYCOST for Europe
with mysql_db.cursor() as cursor:
    subquery_sql = """
        SELECT MIN(PS_SUPPLYCOST) AS min_cost
        FROM partsupp
        WHERE PS_PARTKEY IN (SELECT P_PARTKEY FROM part WHERE P_SIZE = 15 AND P_TYPE LIKE '%BRASS')
    """
    cursor.execute(subquery_sql)
    result = cursor.fetchone()
    min_supplycost = result[0]

# Main query on MySQL for partsupp
with mysql_db.cursor() as cursor:
    query_sql = f"""
        SELECT PS_PARTKEY, PS_SUPPLYCOST
        FROM partsupp
        WHERE PS_SUPPLYCOST = {min_supplycost}
    """
    cursor.execute(query_sql)
    partsupp_records = cursor.fetchall()
    df_partsupp = pd.DataFrame(partsupp_records, columns=['P_PARTKEY', 'PS_SUPPLYCOST'])

# Combine all the parts together
df = (
    df_filtered_part.merge(df_partsupp, on='P_PARTKEY')
    .merge(df_supplier_nation, left_on='P_PARTKEY', right_on='P_PARTKEY')
    [['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]
    .sort_values(['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])
)

# Write the results to a CSV file
df.to_csv('query_output.csv', index=False)
