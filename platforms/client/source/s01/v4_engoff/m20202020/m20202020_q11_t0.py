# Python code (important_stock_identification.py)
import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')

# Execute query in MySQL to get German suppliers
supplier_query = """
SELECT S_SUPPKEY
FROM supplier
WHERE S_NATIONKEY = (
    SELECT N_NATIONKEY
    FROM nation
    WHERE N_NAME = 'GERMANY'
)
"""
german_suppliers = pd.read_sql(supplier_query, mysql_conn)

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get nation and partsupp as DataFrames from Redis
nation_data = redis_conn.get('nation')
partsupp_data = redis_conn.get('partsupp')

# Filter nation to get German nationkey
german_nationkey = nation_data[nation_data['N_NAME'] == 'GERMANY']['N_NATIONKEY'].iloc[0]

# Filter partsupp by German suppliers
partsupp_german = partsupp_data[partsupp_data['PS_SUPPKEY'].isin(german_suppliers['S_SUPPKEY'])]

# Calculate part value and select significant parts
partsupp_german['PART_VALUE'] = partsupp_german['PS_AVAILQTY'] * partsupp_german['PS_SUPPLYCOST']
total_value = partsupp_german['PART_VALUE'].sum()
significant_parts = partsupp_german[partsupp_german['PART_VALUE'] > total_value * 0.0001]

# Select relevant fields and sort by PART_VALUE descending
important_stock = significant_parts[['PS_PARTKEY', 'PART_VALUE']].sort_values(by='PART_VALUE', ascending=False)

# Write the results to a CSV file
important_stock.to_csv('query_output.csv', index=False)

# Close database connections
mysql_conn.close()
