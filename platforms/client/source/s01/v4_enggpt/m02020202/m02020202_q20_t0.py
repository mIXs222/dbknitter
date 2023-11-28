import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MySQL
with mysql_conn.cursor() as cursor:
    # Fetch nation data only for 'CANADA'
    cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'CANADA'")
    nation_keys = cursor.fetchall()
    nation_keys = [key[0] for key in nation_keys]
    
    # Fetch partsupp data where P_NAME starts with 'forest'
    cursor.execute("""
        SELECT PS_PARTKEY, PS_SUPPKEY 
        FROM partsupp 
        WHERE PS_PARTKEY IN (
            SELECT P_PARTKEY 
            FROM part 
            WHERE P_NAME LIKE 'forest%'
        )
    """)
    part_supps = cursor.fetchall()

# Convert partsupp data to DataFrame
partsupp_df = pd.DataFrame(part_supps, columns=['PS_PARTKEY', 'PS_SUPPKEY'])

# Fetch data from Redis
supplier_df = pd.DataFrame(redis_conn.get('supplier'))
supplier_df.columns = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']
lineitem_df = pd.DataFrame(redis_conn.get('lineitem'))
lineitem_df.columns = ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT']

# Only select suppliers from the nation 'CANADA' and with matching S_SUPPKEY from the list of part_supps
suppliers_in_canada = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_keys)]
suppliers_in_canada = suppliers_in_canada[suppliers_in_canada['S_SUPPKEY'].isin(partsupp_df['PS_SUPPKEY'])]

# Lineitem data condition
lineitem_cond = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1994-01-01') &
    (lineitem_df['L_SHIPDATE'] <= '1995-01-01')
]
thresholds = lineitem_cond.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum() * 0.5
thresholds = thresholds.reset_index()

# Combine and filter data
final_df = suppliers_in_canada.merge(thresholds, how='inner', left_on='S_SUPPKEY', right_on='L_SUPPKEY')
final_df = final_df[['S_NAME', 'S_ADDRESS']]

# Sort and write to CSV
final_df.sort_values(by='S_NAME', ascending=True).to_csv('query_output.csv', index=False)

# Closing connections
mysql_conn.close()
