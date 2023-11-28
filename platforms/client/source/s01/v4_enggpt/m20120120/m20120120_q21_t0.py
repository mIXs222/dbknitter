import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Query for mysql to get line items that fulfill the conditions
lineitem_query = """
SELECT
    L1.L_SUPPKEY
FROM
    lineitem L1
WHERE
    L1.L_RETURNFLAG = 'R' AND
    L1.L_RECEIPTDATE > L1.L_COMMITDATE
GROUP BY
    L1.L_SUPPKEY
"""

# Execute query on mysql
lineitem_df = pd.read_sql(lineitem_query, con=mysql_conn)
mysql_conn.close()

# Establish connection to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)
  
# Fetch nation, supplier and orders data from Redis using DirectRedis
nation_df = pd.read_json(redis_conn.get('nation'))
supplier_df = pd.read_json(redis_conn.get('supplier'))
orders_df = pd.read_json(redis_conn.get('orders'))

# Merge supplier with the nation to filter only 'SAUDI ARABIA' and lineitem_df suppliers
suppliers_in_saudi = supplier_df.merge(nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA'],
                                       left_on='S_NATIONKEY',
                                       right_on='N_NATIONKEY',
                                       how='inner')

# Merge to get valid suppliers based on the lineitem_df
valid_suppliers = suppliers_in_saudi.merge(lineitem_df,
                                           left_on='S_SUPPKEY',
                                           right_on='L_SUPPKEY',
                                           how='inner')

# Merge with orders to include only orders with 'F' status
valid_orders = valid_suppliers.merge(orders_df[orders_df['O_ORDERSTATUS'] == 'F'],
                                     left_on='L_ORDERKEY',
                                     right_on='O_ORDERKEY',
                                     how='inner')

# Perform statistics calculation (NUMWAIT)
waiting_stats = valid_orders.groupby('S_NAME') \
                            .agg(NUMWAIT=pd.NamedAgg(column='L_ORDERKEY', aggfunc='count')) \
                            .reset_index()

# Order the results as specified
results = waiting_stats.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Saving the result to a CSV file
results.to_csv('query_output.csv', index=False)
