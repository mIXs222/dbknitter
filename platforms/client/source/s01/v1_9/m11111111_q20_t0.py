from pymongo import MongoClient
import pandas as pd
import csv

# Creating a connection to the MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["tpch"]

# Define a function to convert a cursor to DataFrame
def cursor_to_df(cursor):
    df = pd.DataFrame(list(cursor))
    return df

# Get the Part Keys with Part name like 'forest%'
part_keys_cursor = db.part.find({"P_NAME": {"$regex": "^forest"}}, {"P_PARTKEY": 1})
part_keys_df = cursor_to_df(part_keys_cursor)
part_keys = part_keys_df['P_PARTKEY'].tolist()

# Get the Partsupp keys of part keys fetched above
partsupp_cursor = db.partsupp.find({"PS_PARTKEY": {"$in": part_keys}}, {"PS_SUPPKEY": 1})
partsupp_df = cursor_to_df(partsupp_cursor)
supp_keys = partsupp_df['PS_SUPPKEY'].tolist()

# Get the Supplier details of suppliers from the supp keys
supplier_cursor = db.supplier.find({"S_SUPPKEY": {"$in": supp_keys}})
supplier_df = cursor_to_df(supplier_cursor)

# Get the nation details where nation name is 'CANADA'
nation_cursor = db.nation.find({"N_NAME": "CANADA"})
nation_df = cursor_to_df(nation_cursor)

# Filter out the suppliers falling under Canadian nation key
filter_condition = supplier_df['S_NATIONKEY'].isin(nation_df['N_NATIONKEY'])
final_output_df = supplier_df[filter_condition][['S_NAME', 'S_ADDRESS']]

# We are performing a sort operation similar to ORDER BY in SQL
final_output_df.sort_values(by=["S_NAME"], inplace=True)

# Now, convert the final output dataframe to a csv file
final_output_df.to_csv('query_output.csv', index=False)
