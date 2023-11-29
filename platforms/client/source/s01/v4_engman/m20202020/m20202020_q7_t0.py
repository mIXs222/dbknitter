import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connection to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connection to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MySQL
supplier_query = "SELECT S_SUPPKEY, S_NATIONKEY FROM supplier"
customer_query = "SELECT C_CUSTKEY, C_NATIONKEY FROM customer"
lineitem_query = """
SELECT
  L_SUPPKEY,
  L_ORDERKEY,
  L_EXTENDEDPRICE,
  L_DISCOUNT,
  YEAR(L_SHIPDATE) as L_YEAR
FROM
  lineitem
WHERE
  L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
"""

with mysql_connection.cursor() as cursor:
    cursor.execute(supplier_query)
    suppliers = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NATIONKEY'])
    cursor.execute(customer_query)
    customers = pd.DataFrame(cursor.fetchall(), columns=['C_CUSTKEY', 'C_NATIONKEY'])
    cursor.execute(lineitem_query)
    lineitems = pd.DataFrame(cursor.fetchall(), columns=['L_SUPPKEY', 'L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_YEAR'])

# Retrieve data from Redis and convert to DataFrame
nation = pd.DataFrame(redis_connection.get('nation'), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])
orders = pd.DataFrame(redis_connection.get('orders'), columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

# Filter nation names for 'INDIA' and 'JAPAN'
nation_filter = nation[N_NAME'].isin(['INDIA', 'JAPAN'])

# Merge tables
combined = (
    lineitems
    .merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(suppliers, on='S_SUPPKEY')
)

# Filter nations
filtered_nations = (
    combined
    .merge(nation[nation_filter], left_on='C_NATIONKEY', right_on='N_NATIONKEY')
    .merge(nation[nation_filter], left_on='S_NATIONKEY', right_on='N_NATIONKEY', suffixes=('_CUST', '_SUPP'))
)

# Filter where supplier and customer nations are different and either 'INDIA' or 'JAPAN'
final_result = (
    filtered_nations[
        (filtered_nations['N_NAME_CUST'] != filtered_nations['N_NAME_SUPP']) &
        (filtered_nations['N_NAME_CUST'].isin(['INDIA', 'JAPAN'])) &
        (filtered_nations['N_NAME_SUPP'].isin(['INDIA', 'JAPAN']))
    ]
)

# Calculate 'goss discounted revenue'
final_result['REVENUE'] = final_result['L_EXTENDEDPRICE'] * (1 - final_result['L_DISCOUNT'])

# Group by necessary fields and sum revenue
output = (
    final_result.groupby(['N_NAME_CUST', 'L_YEAR', 'N_NAME_SUPP'])
    .agg(REVENUE=('REVENUE', 'sum'))
    .reset_index()
)

# Rename columns as per requirement
output.rename(columns={
    'N_NAME_CUST': 'CUST_NATION',
    'N_NAME_SUPP': 'SUPP_NATION'
}, inplace=True)

# Sort as per requirement
output = output.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Write to CSV
output.to_csv('query_output.csv', index=False)

# Close connections
mysql_connection.close()
redis_connection.close()
