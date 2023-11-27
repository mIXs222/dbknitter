import pymysql
import pymongo
import pandas as pd
from sqlalchemy import create_engine

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Pull data from MySQL
orders_query = """SELECT * FROM orders WHERE O_ORDERSTATUS = 'F'"""
lineitem_query = """SELECT * FROM lineitem WHERE L_RECEIPTDATE > L_COMMITDATE"""

orders_df = pd.read_sql(orders_query, mysql_conn)
lineitem_df = pd.read_sql(lineitem_query, mysql_conn)

mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Pull data from MongoDB
nation_coll = mongo_db['nation']
supplier_coll = mongo_db['supplier']

nation_df = pd.DataFrame(list(nation_coll.find({"N_NAME": "SAUDI ARABIA"})))
supplier_df = pd.DataFrame(list(supplier_coll.find()))

mongo_client.close()

# Merge datasets
merged_df = pd.merge(
    lineitem_df,
    orders_df,
    how='inner',
    left_on='L_ORDERKEY',
    right_on='O_ORDERKEY'
)
merged_df = pd.merge(
    merged_df,
    supplier_df,
    how='inner',
    left_on='L_SUPPKEY',
    right_on='S_SUPPKEY'
)
merged_df = pd.merge(
    merged_df,
    nation_df,
    how='inner',
    left_on='S_NATIONKEY',
    right_on='N_NATIONKEY'
)

# Filter lineitems to those that meet the subquery conditions
def subquery_filter(df):
    grouped = df.groupby('L_ORDERKEY')
    for name, group in grouped:
        if any(group['L_SUPPKEY'] != group['L_SUPPKEY'].iloc[0]) and not any(
            (group['L_SUPPKEY'] != group['L_SUPPKEY'].iloc[0]) & (group['L_RECEIPTDATE'] > group['L_COMMITDATE'])
        ):
            yield group.iloc[0]

filtered_lineitems = pd.DataFrame(subquery_filter(merged_df))

# Merge result of subquery with the original merged frame
final_df = pd.merge(
    filtered_lineitems,
    merged_df,
    how='inner',
    on=['L_ORDERKEY', 'L_SUPPKEY']
)

# Group by supplier name and count
result = final_df.groupby('S_NAME').size().reset_index(name='NUMWAIT')

# Sort results
result.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True], inplace=True)

# Write to CSV
result.to_csv('query_output.csv', index=False)
