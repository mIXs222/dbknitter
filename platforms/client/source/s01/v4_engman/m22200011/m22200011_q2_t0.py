import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Construct and execute the query in MySQL
mysql_query = """
SELECT 
    p.PS_PARTKEY as partkey, 
    p.PS_SUPPLYCOST as supplycost, 
    s.S_NAME as sname, 
    s.S_ACCTBAL as sacctbal, 
    s.S_ADDRESS as saddress, 
    s.S_PHONE as sphone, 
    s.S_COMMENT as scomment,
    s.S_NATIONKEY as snationkey
FROM partsupp p
INNER JOIN supplier s ON p.PS_SUPPKEY = s.S_SUPPKEY
"""

mysql_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Read data from Redis
nation_df = pd.read_json(redis_conn.get('nation'), orient='records')
region_df = pd.read_json(redis_conn.get('region'), orient='records')
part_df = pd.read_json(redis_conn.get('part'), orient='records')

# Filter nation and region for "EUROPE"
europe_nations = region_df[region_df["R_NAME"] == "EUROPE"]["R_REGIONKEY"].unique()
europe_nation_df = nation_df[nation_df["N_REGIONKEY"].isin(europe_nations)]

# Filter parts for "BRASS" and size 15
brass_parts = part_df[(part_df["P_TYPE"] == "BRASS") & (part_df["P_SIZE"] == 15)]

# Merge to get the relevant suppliers for BRASS parts size 15 in the EUROPE region
relevant_suppliers = pd.merge(
    brass_parts,
    europe_nation_df,
    left_on="P_PARTKEY",
    right_on="N_NATIONKEY",
    how="inner"
)

# Merge to get final data containing all relevant information
final_df = pd.merge(
    relevant_suppliers,
    mysql_df,
    left_on=["P_PARTKEY", "N_NATIONKEY"],
    right_on=["partkey", "snationkey"],
    how="inner"
)

# Filter for minimum supply cost per part and sort as requested
final_df = (final_df.loc[final_df.groupby('partkey')['supplycost'].idxmin()]
            .sort_values(by=['sacctbal', 'N_NAME', 'sname', 'partkey'], ascending=[False, True, True, True]))

# Select and order columns as requested
output_df = final_df[['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']]
output_df.columns = ['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']

# Write the result to a CSV file
output_df.to_csv('query_output.csv', index=False)
