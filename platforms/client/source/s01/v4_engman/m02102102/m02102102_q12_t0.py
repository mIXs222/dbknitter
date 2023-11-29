import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

with mysql_conn.cursor() as mysql_cursor:
    # Query to select orders from MySQL database with high priority
    mysql_query = """
    SELECT O_ORDERKEY, O_ORDERPRIORITY 
    FROM orders 
    WHERE O_ORDERPRIORITY = 'URGENT' OR O_ORDERPRIORITY = 'HIGH'
    """
    mysql_cursor.execute(mysql_query)
    high_priority_orders = mysql_cursor.fetchall()

# Preparing a data frame for high priority orders
high_priority_df = pd.DataFrame(high_priority_orders, columns=['O_ORDERKEY', 'O_ORDERPRIORITY'])

# Connecting to the Redis database
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieving lineitem data from Redis and loading into a DataFrame
lineitem_df = pd.DataFrame.from_records(redis_conn.get('lineitem'))
lineitem_df.columns = ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY',
                       'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS',
                       'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT']

# Converting string dates to pandas datetime
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

# Filtering the lineitem dataset based on the provided conditions
filtered_lineitem_df = lineitem_df[(lineitem_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
                                   (lineitem_df['L_RECEIPTDATE'] >= '1994-01-01') &
                                   (lineitem_df['L_RECEIPTDATE'] <= '1995-01-01') &
                                   (lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']) &
                                   (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE'])]

# Joining both datasets on orderkey
merged_df = pd.merge(filtered_lineitem_df, high_priority_df, how='left', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Adding a column to identify high and low priority lineitems
merged_df['PRIORITY_TYPE'] = merged_df['O_ORDERPRIORITY'].apply(lambda x: 'HIGH' if x in ['URGENT', 'HIGH'] else 'LOW')

# Grouping the results by L_SHIPMODE and PRIORITY_TYPE, then counting
result = merged_df.groupby(['L_SHIPMODE', 'PRIORITY_TYPE']).size().reset_index(name='COUNT')

# Saving the results to a CSV file
result.to_csv('query_output.csv', index=False)

# Close the database connections
mysql_conn.close()
