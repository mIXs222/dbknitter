import pandas as pd
from direct_redis import DirectRedis

# Connect to the Redis database
redis_host = 'redis'
port = 6379
database_name = '0'
r = DirectRedis(host=redis_host, port=port, db=database_name)

# Convert bytes to dataframe if data present, else empty dataframe
def bytes_to_df(data, columns=None):
    if data:
        return pd.read_json(data.decode('utf-8'))
    else:
        return pd.DataFrame(columns=columns)

# Get the data from Redis
nation = bytes_to_df(r.get('nation'), ["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"])
region = bytes_to_df(r.get('region'), ["R_REGIONKEY", "R_NAME", "R_COMMENT"])
part = bytes_to_df(r.get('part'), ["P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND", "P_TYPE", "P_SIZE", "P_CONTAINER", "P_RETAILPRICE", "P_COMMENT"])
supplier = bytes_to_df(r.get('supplier'), ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"])
partsupp = bytes_to_df(r.get('partsupp'), ["PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY", "PS_SUPPLYCOST", "PS_COMMENT"])

# Join tables
nation_region = nation.merge(region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
supplier_nation = supplier.merge(nation_region, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
part_partsupp = part.merge(partsupp, left_on='P_PARTKEY', right_on='PS_PARTKEY')
supplier_part = supplier_nation.merge(part_partsupp, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Query for EUROPE region, BRASS parts of size 15
eu_brass_parts = supplier_part[
    (supplier_part['R_NAME'] == 'EUROPE') &
    (supplier_part['P_TYPE'] == 'BRASS') &
    (supplier_part['P_SIZE'] == 15)
]

# Calculate minimum cost suppliers
cost_df = eu_brass_parts.loc[eu_brass_parts.groupby('P_PARTKEY')['PS_SUPPLYCOST'].idxmin()]

# Sort based on required columns
sorted_cost_df = cost_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Select and rename the columns
output_df = sorted_cost_df[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']].copy()
output_df.columns = ['Supplier Account Balance', 'Supplier Name', 'Nation Name', 'Part Number', 'Manufacturer', 'Address', 'Phone', 'Comment']

# Write the result to a CSV file
output_df.to_csv('query_output.csv', index=False)
