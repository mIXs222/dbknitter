import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Get nations and suppliers from MySQL
with mysql_conn:
    with mysql_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM nation WHERE N_NAME = 'SAUDI ARABIA'")
        nations = cursor.fetchall()
        
        saudi_arabia_nationkey = nations[0][0] if nations else None

        if saudi_arabia_nationkey is not None:
            cursor.execute("""
                SELECT S_SUPPKEY, S_NAME
                FROM supplier
                WHERE S_NATIONKEY = %s
            """, (saudi_arabia_nationkey,))
            suppliers = cursor.fetchall()

# Dictionary to hold suppliers from SAUDI ARABIA
supplier_dict = {supp[0]: supp[1] for supp in suppliers} if suppliers else {}

mysql_conn.close()

# Establish connection to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get orders and lineitems from Redis
orders_df = pd.read_json(redis_conn.get('orders'), orient='records')
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Query Logic
kept_orders_waiting_df = (
    lineitem_df.merge(
        orders_df,
        left_on='L_ORDERKEY',
        right_on='O_ORDERKEY'
    )
    .query("`L_COMMITDATE` < `L_RECEIPTDATE` and `O_ORDERSTATUS` == 'F'")
    .groupby('L_ORDERKEY')
    .filter(lambda x: (x['L_SUPPKEY'].isin(supplier_dict.keys()) & ~(x['L_SUPPKEY'].duplicated(keep=False))).any())
    .query("`L_SUPPKEY` in @supplier_dict")
    .assign(S_NAME=lambda df: df['L_SUPPKEY'].map(supplier_dict))
    .loc[:, ['S_SUPPKEY', 'S_NAME']]
    .drop_duplicates()
)

# Write to CSV
kept_orders_waiting_df.to_csv('query_output.csv', index=False)
