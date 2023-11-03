from pymongo import MongoClient
from sqlalchemy import create_engine
import pandas as pd

# Initialise MongoDB client & MySQL client (SQL-Alchemy)
client = MongoClient('mongodb://localhost:27017/')
engine = create_engine('mysql://root:my-secret-pw@mysql/tpch')

# Initial part of the query
query_mysql = """
SELECT
    S_NAME,
    S_ADDRESS,
    S_SUPPKEY,
    N_NATIONKEY
FROM
    SUPPLIER,
    NATION
WHERE
    S_NATIONKEY = N_NATIONKEY
    AND N_NAME = 'CANADA'
"""

supplier_nation_df = pd.read_sql_query(query_mysql, engine)

# Now query MongoDB
partsupp = client['tpch']['partsupp']
lineitem = client['tpch']['lineitem']
part = client['tpch']['part']

pipeline_part = [{ "$match": { "P_NAME": { "$regex": 'forest' } } }]

part_df = pd.DataFrame(list(part.aggregate(pipeline_part)))

partsupp_df = pd.DataFrame(list(partsupp.find({ "PS_PARTKEY": { "$in": part_df["P_PARTKEY"].tolist() } })))

complete_df = pd.merge(supplier_nation_df, partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

pipeline_lineitem = [{ "$match": { "L_PARTKEY": "PS_PARTKEY" } }, { "$match": { "L_SUPPKEY": "PS_SUPPKEY" } }]

lineitem_df = pd.DataFrame(list(lineitem.aggregate(pipeline_lineitem)))

complete_df["PS_AVAILQTY"] = complete_df["PS_AVAILQTY"] > lineitem_df["L_QUANTITY"].sum()
complete_df = complete_df.sort_values(by=['S_NAME'])

# Now, write to CSV
complete_df.to_csv('query_output.csv', index=False)
