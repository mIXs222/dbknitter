# query.py
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

# Fetch lineitem table from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM lineitem")
    lineitem_data = cursor.fetchall()
    lineitem_df = pd.DataFrame(list(lineitem_data), columns=[
        'L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY',
        'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS',
        'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT',
        'L_SHIPMODE', 'L_COMMENT'
    ])

# Close MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch part table from Redis in DataFrame format
part_data = eval(redis_conn.get('part'))  # Assume the data is stored as a string representation of a list
part_df = pd.DataFrame(part_data, columns=[
    'P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER',
    'P_RETAILPRICE', 'P_COMMENT'
])

# Perform the query equivalent by using pandas operations
# Apply filters specified in the original SQL query to the part_df
filtered_part_df = part_df[
    (part_df['P_BRAND'] == 'Brand#23') &
    (part_df['P_CONTAINER'] == 'MED BAG')
]

# Compute the average quantity for each part in lineitem_df
avg_quantity_per_part = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()

# 0.2 * AVG(L_QUANTITY) for each P_PARTKEY
avg_quantity_per_part['L_QUANTITY'] = avg_quantity_per_part['L_QUANTITY'] * 0.2

# Merge the filtered part dataframe with the average quantity dataframe to enforce the P_PARTKEY relation
part_with_avg_qty = filtered_part_df.merge(avg_quantity_per_part, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Finally, filter the lineitem_df based on L_QUANTITY being less than the corresponding 0.2 * AVG(L_QUANTITY)
filtered_lineitem_df = lineitem_df[lineitem_df.apply(
    lambda row: row['L_PARTKEY'] in part_with_avg_qty['P_PARTKEY'].values and
                row['L_QUANTITY'] < part_with_avg_qty.loc[
                    part_with_avg_qty['P_PARTKEY'] == row['L_PARTKEY'], 'L_QUANTITY'
                ].values[0],
    axis=1
)]

# Calculate AVG_YEARLY
result_df = pd.DataFrame({'AVG_YEARLY': [(filtered_lineitem_df['L_EXTENDEDPRICE'].sum() / 7.0)]})

# Write result to the file
result_df.to_csv('query_output.csv', index=False)
