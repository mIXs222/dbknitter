from pymongo import MongoClient
import pandas as pd

# connect to mongodb
client = MongoClient("mongodb://mongodb:27017")
db = client['tpch']

# load data from mongodb to pandas dataframes
partsupp_col = db['partsupp']
part_col = db['part']
supplier_col = db['supplier']

partsupp_df = pd.DataFrame(list(partsupp_col.find()))
part_df = pd.DataFrame(list(part_col.find()))
supplier_df = pd.DataFrame(list(supplier_col.find()))

# merge dataframe
df = pd.merge(partsupp_df, part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# filter data
df = df[
    (df.P_BRAND != 'Brand#45') & 
    (~df.P_TYPE.str.startswith('MEDIUM POLISHED')) & 
    (df.P_SIZE.isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

# filter supplier
supplier_filtered = supplier_df[~supplier_df.S_COMMENT.str.contains('Customer Complaints')]['S_SUPPKEY']
df = df[df.PS_SUPPKEY.isin(supplier_filtered)]

# group by and count
results = df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['PS_SUPPKEY'].nunique().reset_index()
results.columns = ['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT']

# sort values
results = results.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# write to csv
results.to_csv('query_output.csv', index=False)
