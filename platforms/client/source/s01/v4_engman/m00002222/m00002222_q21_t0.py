import pymysql
import pandas as pd

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Fetching nation and supplier tables from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME = 'SAUDI ARABIA';")
    saudi_nation = cursor.fetchone()
    nation_key = saudi_nation[0] if saudi_nation else None

    if nation_key is not None:
        cursor.execute("SELECT S_SUPPKEY, S_NAME FROM supplier WHERE S_NATIONKEY = %s;", (nation_key,))
        suppliers = cursor.fetchall()
    else:
        suppliers = []

mysql_conn.close()

# Create DirectRedis instance and read Pandas DataFrames
from direct_redis import DirectRedis

redis_conn = DirectRedis(host='redis', port=6379, db=0)
orders_df = pd.read_json(redis_conn.get('orders'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Filtering orders with 'F' status
filtered_orders_df = orders_df[orders_df['O_ORDERSTATUS'] == 'F']

# Finding multi-supplier orders
multi_supplier_orders = lineitem_df[lineitem_df.duplicated(['L_ORDERKEY'], keep=False)]
multi_supplier_orders = multi_supplier_orders.groupby('L_ORDERKEY').filter(lambda g: g['L_SUPPKEY'].nunique() > 1)

# Filtering lineitems which failed to meet delivery date
failed_lineitems_df = multi_supplier_orders[multi_supplier_orders['L_RETURNFLAG'] == 'F']

# Counting the number of waits per supplier
numwait_df = failed_lineitems_df.groupby('L_SUPPKEY').size().reset_index(name='NUMWAIT')

# Filtering suppliers for nation 'SAUDI ARABIA' (nation_key) and merging with numwait
saudi_suppliers_df = pd.DataFrame(suppliers, columns=['S_SUPPKEY', 'S_NAME'])
result_df = saudi_suppliers_df.merge(numwait_df, on='S_SUPPKEY')

# Sorting the result as per the user's requirement
sorted_result_df = result_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write to .csv
sorted_result_df.to_csv('query_output.csv', columns=['NUMWAIT', 'S_NAME'], index=False)
