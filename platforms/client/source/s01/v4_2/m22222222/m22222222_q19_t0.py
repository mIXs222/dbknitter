import redis
import pandas as pd

r = redis.StrictRedis(host='redis', port=6379, db=0)

part = pd.read_json(r.get("part"))
lineitem = pd.read_json(r.get("lineitem"))

merged_df = pd.merge(lineitem, part, left_on='L_PARTKEY', right_on='P_PARTKEY')

merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

filtered_df_1 = merged_df.loc[
    (merged_df['P_BRAND'] == 'Brand#12') &
    (merged_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
    (merged_df['L_QUANTITY'].between(1, 10)) &
    (merged_df['P_SIZE'].between(1, 5)) &
    (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
]

filtered_df_2 = merged_df.loc[
    (merged_df['P_BRAND'] == 'Brand#23') &
    (merged_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
    (merged_df['L_QUANTITY'].between(10, 20)) &
    (merged_df['P_SIZE'].between(1, 10)) &
    (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
]

filtered_df_3 = merged_df.loc[
    (merged_df['P_BRAND'] == 'Brand#34') &
    (merged_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
    (merged_df['L_QUANTITY'].between(20, 30)) &
    (merged_df['P_SIZE'].between(1, 15)) &
    (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
]

filtered_df = pd.concat([filtered_df_1, filtered_df_2, filtered_df_3])
filtered_df.to_csv('query_output.csv', columns=['REVENUE'], index=False)
