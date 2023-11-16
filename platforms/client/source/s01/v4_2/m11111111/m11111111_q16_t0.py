import pymongo
import pandas as pd
from pandas.io.json import json_normalize

# Establish connection
client = pymongo.MongoClient("mongodb://mongodb:27017")
db = client["tpch"]

# Load data from mongodb collections
part_data = list(db["part"].find({}, {"_id":0}))
supplier_data = list(db["supplier"].find({}, {"_id":0}))
partsupp_data = list(db["partsupp"].find({}, {"_id":0}))

# Convert data to pandas DataFrame
df_part = pd.json_normalize(part_data)
df_supplier = pd.json_normalize(supplier_data)
df_partsupp = pd.json_normalize(partsupp_data)

# Apply the filters and joins
filtered_part = df_part[
    (df_part['P_BRAND'] != 'Brand#45') & 
    (~df_part['P_TYPE'].str.contains('MEDIUM POLISHED')) & 
    (df_part['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]
excluded_suppliers = df_supplier[df_supplier['S_COMMENT'].str.contains('Customer Complaints')]['S_SUPPKEY']
filtered_partsupp = df_partsupp[~df_partsupp['PS_SUPPKEY'].isin(excluded_suppliers)]

# Merge tables and group by
df_merged = pd.merge(filtered_part, filtered_partsupp, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')
result = df_merged.groupby(['P_BRAND','P_TYPE','P_SIZE']).agg(SUPPLIER_CNT=pd.NamedAgg(column='PS_SUPPKEY', aggfunc='nunique')).reset_index()

# Sort and write to csv
result_sorted = result.sort_values(by=['SUPPLIER_CNT','P_BRAND','P_TYPE','P_SIZE'], ascending=[False, True, True, True])
result_sorted.to_csv('query_output.csv', index=False, line_terminator='\r\n')
