import pandas as pd
import redis

# Connect to redis
r = redis.Redis(host='redis', port=6379, db=0)

# Read the tables into pandas dataframes
nation = pd.read_msgpack(r.get('nation'))
supplier = pd.read_msgpack(r.get('supplier'))
partsupp = pd.read_msgpack(r.get('partsupp'))
part = pd.read_msgpack(r.get('part'))
lineitem = pd.read_msgpack(r.get('lineitem'))

# Begin to process the query
part = part[part['P_NAME'].str.startswith('forest')]
parts_with_suppliers = partsupp[partsupp['PS_PARTKEY'].isin(part['P_PARTKEY'])]

# Find the quantity from the lineitem table
quantity = lineitem[(lineitem['L_PARTKEY'].isin(parts_with_suppliers['PS_PARTKEY'])) & 
                    (lineitem['L_SUPPKEY'].isin(parts_with_suppliers['PS_SUPPKEY'])) & 
                    (lineitem['L_SHIPDATE'] >= '1994-01-01') & 
                    (lineitem['L_SHIPDATE'] < '1995-01-01')]['L_QUANTITY'].sum()
quantity *= 0.5

# Filter out the parts and suppliers with available quantity more than the half of the total quantity
parts_with_suppliers = parts_with_suppliers[parts_with_suppliers['PS_AVAILQTY'] > quantity]

# Filter on the suppliers in the nation Canada
supplier = supplier[(supplier['S_SUPPKEY'].isin(parts_with_suppliers['PS_SUPPKEY'])) & 
                    (supplier['S_NATIONKEY'].isin(nation[nation['N_NAME'] == 'CANADA']['N_NATIONKEY']))]

# Get the result and write to a CSV file
result = supplier[['S_NAME', 'S_ADDRESS']].sort_values(by='S_NAME')
result.to_csv('query_output.csv', index=False)
