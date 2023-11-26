import pymysql
import pandas as pd
import direct_redis

# Establish a connection to MySQL database
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Creating a function to read data from Redis
def read_from_redis(table_name):
    redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    df_bytes = redis_conn.get(table_name)
    if df_bytes:
        df = pd.read_msgpack(df_bytes)
        return df
    else:
        return pd.DataFrame()

# Read the needed tables from MySQL
with mysql_connection.cursor() as cursor:
    # Read supplier table
    cursor.execute("SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY FROM supplier")
    supplier_data = cursor.fetchall()
    df_supplier = pd.DataFrame(list(supplier_data), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY'])
    # Read lineitem table
    cursor.execute("SELECT L_PARTKEY, L_SUPPKEY, L_QUANTITY, L_SHIPDATE FROM lineitem WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01'")
    lineitem_data = cursor.fetchall()
    df_lineitem = pd.DataFrame(list(lineitem_data), columns=['L_PARTKEY', 'L_SUPPKEY', 'L_QUANTITY', 'L_SHIPDATE'])

# Close MySQL connection
mysql_connection.close()

# Read the needed tables from Redis
df_nation = read_from_redis('nation')
df_part = read_from_redis('part')
df_partsupp = read_from_redis('partsupp')

# Processing the data
# Filtering parts with names starting with 'forest'
part_forest = df_part[df_part['P_NAME'].str.startswith('forest')]

# Joining part and partsupp tables on partkey
part_partsupp = pd.merge(part_forest, df_partsupp, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Calculating half of the sum of quantities from lineitem for each partkey and suppkey
lineitem_agg = df_lineitem.groupby(['L_PARTKEY', 'L_SUPPKEY']).agg({'L_QUANTITY': 'sum'}).reset_index()
lineitem_agg['HALF_QUANTITY'] = 0.5 * lineitem_agg['L_QUANTITY']

# Filtering partsupp with available quantity greater than half the sum of quantities of lineitems
qualified_partsupp = part_partsupp[part_partsupp.apply(lambda x: x['PS_AVAILQTY'] > lineitem_agg[(lineitem_agg['L_PARTKEY'] == x['PS_PARTKEY']) & (lineitem_agg['L_SUPPKEY'] == x['PS_SUPPKEY'])]['HALF_QUANTITY'].values[0], axis=1)]

# Filtering supplier for those available in qualified_partsupp
qualified_suppliers = df_supplier[df_supplier['S_SUPPKEY'].isin(qualified_partsupp['PS_SUPPKEY'])]

# Joining qualified suppliers with nation
final_df = qualified_suppliers.merge(df_nation[df_nation['N_NAME'] == 'CANADA'], left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='inner')

# Selecting relevant columns and sorting by supplier name
result = final_df[['S_NAME', 'S_ADDRESS']].sort_values(by='S_NAME')

# Writing the result to a CSV file
result.to_csv('query_output.csv', index=False)
