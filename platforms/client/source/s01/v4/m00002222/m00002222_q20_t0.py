# query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

try:
    # Query parts where P_NAME like 'forest%'
    with mysql_conn.cursor() as cursor:
        cursor.execute("SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'")
        part_keys = [row[0] for row in cursor.fetchall()]

    # Connect to Redis
    redis_conn = DirectRedis(host='redis', port=6379, db=0)

    # Query part suppliers from partsupp in Redis
    partsupp_df = pd.read_json(redis_conn.get('partsupp'))
    qualified_partsupps = partsupp_df[partsupp_df['PS_PARTKEY'].isin(part_keys)]

    # Query line items from lineitem in Redis
    lineitem_df = pd.read_json(redis_conn.get('lineitem'))
    lineitem_grouped = lineitem_df.groupby(['L_PARTKEY', 'L_SUPPKEY'])

    # Filter part suppliers based on availability and line items
    def filter_partsupps(row):
        if (row['PS_PARTKEY'], row['PS_SUPPKEY']) in lineitem_grouped.groups:
            group_df = lineitem_grouped.get_group((row['PS_PARTKEY'], row['PS_SUPPKEY']))
            total_quantity = group_df[
                (group_df['L_SHIPDATE'] >= '1994-01-01') & (group_df['L_SHIPDATE'] < '1995-01-01')
            ]['L_QUANTITY'].sum()
            return total_quantity * 0.5 < row['PS_AVAILQTY']
        return False

    qualified_partsupps = qualified_partsupps[qualified_partsupps.apply(filter_partsupps, axis=1)]

    # Get the suppliers' keys
    supplier_keys = qualified_partsupps['PS_SUPPKEY'].unique().tolist()

    # Query suppliers and nations from MySQL
    suppliers_query = f"""
    SELECT
        S_NAME, S_ADDRESS
    FROM
        supplier JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY
    WHERE
        S_SUPPKEY IN ({','.join(map(str, supplier_keys))})
        AND N_NAME = 'CANADA'
    ORDER BY
        S_NAME
    """
    with mysql_conn.cursor() as cursor:
        cursor.execute(suppliers_query)
        suppliers = cursor.fetchall()

finally:
    mysql_conn.close()

# Convert the result to DataFrame and then save to CSV
df = pd.DataFrame(suppliers, columns=['S_NAME', 'S_ADDRESS'])
df.to_csv('query_output.csv', index=False)
