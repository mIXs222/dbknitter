import pandas as pd
import pymysql
from collections import namedtuple
import direct_redis
import csv

def dump_to_csv(filename, data):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(data[0]._fields)
        for row in data:
            writer.writerow(row)

# Establishing connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    passwd='my-secret-pw',
    db='tpch'
)

# Querying MySQL for lineitem data
mysql_query = """
    select
        L_SUPPKEY as SUPPLIER_NO,
        sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE
    from
        lineitem
    where
        L_SHIPDATE >= '1996-01-01'
        AND L_SHIPDATE < '1996-04-01'
    group by
        L_SUPPKEY
"""

with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    Revenue = namedtuple('Revenue', ['SUPPLIER_NO', 'TOTAL_REVENUE'])
    revenue0 = [Revenue(*row) for row in cursor.fetchall()]

# Finding the max total_revenue in revenue0
max_total_revenue = max(r.TOTAL_REVENUE for r in revenue0)

# Filter suppliers with the max total_revenue
filtered_revenue = [r for r in revenue0 if r.TOTAL_REVENUE == max_total_revenue]

# Connecting Redis database
dr = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Assuming supplier data is stored as hashes in Redis
supplier_data = pd.read_json(dr.get('supplier'), orient='records')

# Processing data and merging
result = supplier_data[supplier_data['S_SUPPKEY'].isin([r.SUPPLIER_NO for r in filtered_revenue])]
result['TOTAL_REVENUE'] = max_total_revenue
result = result[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]

# Closing MySQL connection
mysql_conn.close()

# Outputting to CSV
dump_to_csv('query_output.csv', result.itertuples(index=False))
