import pymongo
from pymongo import MongoClient
import pymysql
import pandas as pd

# Connect to MySQL db
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)

# Connect to MongoDB
mongo_client = MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Fetch Data from MySQL and MongoDB and Merge into pandas DataFrame

def get_mysql_data(sql):
    mysql_cursor.execute(sql)
    return pd.DataFrame(mysql_cursor.fetchall())

def get_mongo_data(collection_name):
    collection = mongo_db[collection_name]
    return pd.DataFrame(list(collection.find()))


lineitem_df = get_mongodb_data('lineitem')
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

mask = (lineitem_df['L_SHIPDATE'] <= '1998-09-02')

df_filtered = lineitem_df.loc[mask]

group_by_columns = [
    'L_RETURNFLAG',
    'L_LINESTATUS'
]

aggregations = {
    'L_QUANTITY': ['sum', 'mean'],
    'L_EXTENDEDPRICE': ['sum', 'mean'],
    'L_DISCOUNT': ['mean', 'count'],
    '_id': ['count']
}

df_grouped  = df_filtered.groupby(group_by_columns).agg(aggregations)
df_grouped.columns = ["_".join(x) for x in df_grouped.columns.ravel()]
df_grouped.reset_index(inplace=True)

df_grouped['SUM_DISC_PRICE'] = df_grouped['L_EXTENDEDPRICE_sum'] * (1 - df_grouped['L_DISCOUNT_mean'])
df_grouped['SUM_CHARGE'] = df_grouped['SUM_DISC_PRICE'] * (1 + df_grouped['L_DISCOUNT_mean'])

df_grouped = df_grouped.sort_values(by=group_by_columns)

df_grouped.to_csv('query_output.csv', index=False)
