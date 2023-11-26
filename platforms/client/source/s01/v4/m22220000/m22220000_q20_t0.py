import pandas as pd
import pymysql
import direct_redis

# Connect to mysql
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Get part keys with P_NAME like 'forest%'
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'")
    part_keys = cursor.fetchall()

part_keys = [item[0] for item in part_keys]

# Filter partsupp by part keys and availability
with mysql_conn.cursor() as cursor:
    ps_query = """
    SELECT PS_SUPPKEY
    FROM partsupp
    WHERE PS_PARTKEY IN (%s) AND PS_AVAILQTY > (
        SELECT 0.5 * SUM(L_QUANTITY)
        FROM lineitem
        WHERE L_PARTKEY = partsupp.PS_PARTKEY AND L_SUPPKEY = partsupp.PS_SUPPKEY
        AND L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01'
    )
    """
    in_pks = ', '.join(['%s'] * len(part_keys))
    cursor.execute(ps_query % in_pks, part_keys)
    supp_keys = cursor.fetchall()

mysql_conn.close()
supp_keys = [item[0] for item in supp_keys]

# Connect to redis
r_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve suppliers from Redis
supplier_df = pd.read_json(r_conn.get('supplier'))
nation_df = pd.read_json(r_conn.get('nation'))

# Filter suppliers by supp keys and nation
supplier_df = supplier_df[supplier_df['S_SUPPKEY'].isin(supp_keys)]
nation_df = nation_df[nation_df['N_NAME'] == 'CANADA']

# Merge supplier and nation
merged_df = supplier_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter the final dataframe and sort by name
final_df = merged_df[['S_NAME', 'S_ADDRESS']]
final_df = final_df.sort_values('S_NAME')

# Save the results to CSV
final_df.to_csv('query_output.csv', index=False)
