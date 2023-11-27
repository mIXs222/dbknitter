import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection details
mysql_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# Establish MySQL connection
connection = pymysql.connect(**mysql_config)
query = '''
SELECT s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_PHONE, s.S_ACCTBAL, s.S_COMMENT
FROM supplier s
INNER JOIN partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY 
INNER JOIN lineitem l ON ps.PS_PARTKEY = l.L_PARTKEY AND ps.PS_SUPPKEY = l.L_SUPPKEY
WHERE l.L_SHIPDATE >= '1994-01-01' AND l.L_SHIPDATE < '1995-01-01'
GROUP BY s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_PHONE, s.S_ACCTBAL, s.S_COMMENT
HAVING SUM(l.L_QUANTITY) > 0.5 * SUM(ps.PS_AVAILQTY);
'''

# Execute the query and load into a DataFrame
mysql_data = pd.read_sql(query, connection)

# Close mysql connection
connection.close()

# Redis connection details
redis_config = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Establish Redis connection
r = DirectRedis(**redis_config)
nation = pd.read_json(r.get('nation'))
part = pd.read_json(r.get('part'))

# Filter parts with naming convention containing 'forest'
forest_parts = part[part['P_NAME'].str.contains("forest", case=False)]

# Get CANADA's nation key
canada_nkey = nation[nation['N_NAME'] == 'CANADA']['N_NATIONKEY'].squeeze()

# Use CANADA's nation key to filter suppliers
suppliers_in_canada = mysql_data[mysql_data.apply(lambda row: row['S_NATIONKEY'] == canada_nkey, axis=1)]

# Join filtered parts and suppliers
result = pd.merge(suppliers_in_canada, forest_parts, left_on='S_SUPPKEY', right_on='P_PARTKEY')

# Write output to CSV
result.to_csv('query_output.csv', index=False)
