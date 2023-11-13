from pymongo import MongoClient
import pandas as pd

def query_mongodb():
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    # Select database and collection
    db = client['tpch']

    # Select part and partsupp collections
    part = db['part']
    partsupp = db['partsupp']

    # Create dataframes
    part_df = pd.DataFrame(list(part.find()))
    partsupp_df = pd.DataFrame(list(partsupp.find()))

    # Merge dataframes
    merge_df = pd.merge(partsupp_df, part_df, how='inner', left_on="PS_PARTKEY", right_on="P_PARTKEY")

    # Filter rows
    condition_df = merge_df[(merge_df['P_BRAND'] != 'Brand#45') & 
                            (~merge_df['P_TYPE'].str.contains('MEDIUM POLISHED')) & 
                            (merge_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))]

    # Group by columns and apply count function
    final_df = condition_df.groupby(['P_BRAND','P_TYPE','P_SIZE'])['PS_SUPPKEY'].nunique().reset_index(name='SUPPLIER_CNT')

    # Filter supplier collection
    supplier = db['supplier']
    supplier_df = pd.DataFrame(list(supplier.find()))
    supplier_df = supplier_df[supplier_df['S_COMMENT'].str.contains('Customer Complaints')]

    # Get SUPPKEY which is not in supplier dataframe
    final_df = final_df[~final_df['PS_SUPPKEY'].isin(supplier_df['S_SUPPKEY'])]

    # Sort df
    final_df = final_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

    # Save to csv
    final_df.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    query_mongodb()
