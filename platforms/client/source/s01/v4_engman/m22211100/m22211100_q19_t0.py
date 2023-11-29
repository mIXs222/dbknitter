import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Define the query for MySQL
mysql_query = '''
SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM lineitem
WHERE
    L_SHIPMODE IN ('AIR', 'AIR REG')
    AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    AND (
        (L_QUANTITY >= 1 AND L_QUANTITY <= 11) OR
        (L_QUANTITY >= 10 AND L_QUANTITY <= 20) OR
        (L_QUANTITY >= 20 AND L_QUANTITY <= 30)
    )
'''

mysql_cursor.execute(mysql_query)
revenue_mysql = mysql_cursor.fetchone()[0]
mysql_cursor.close()
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch part table from Redis and convert to DataFrame
part_df = pd.read_json(redis_conn.get('part'))

# Filter parts based on the conditions
filtered_part_df = part_df[
    ((part_df['P_BRAND'] == 'Brand#12') &
     (part_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
     (part_df['P_SIZE'] >= 1) & (part_df['P_SIZE'] <= 5)) |

    ((part_df['P_BRAND'] == 'Brand#23') &
     (part_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
     (part_df['P_SIZE'] >= 1) & (part_df['P_SIZE'] <= 10)) |

    ((part_df['P_BRAND'] == 'Brand#34') &
     (part_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
     (part_df['P_SIZE'] >= 1) & (part_df['P_SIZE'] <= 15))
]

revenue_redis = filtered_part_df['P_RETAILPRICE'].sum()

# Combine results from both databases
total_revenue = revenue_mysql + revenue_redis

# Write output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['REVENUE'])
    csvwriter.writerow([total_revenue])
