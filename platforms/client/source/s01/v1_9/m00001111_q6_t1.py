import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine

# function to get data from mysql
def get_from_mysql(table_name):
    db_connection_str = 'mysql+pymysql://root:my-secret-pw@mysql/tpch'
    db_connection = create_engine(db_connection_str)

    df = pd.read_sql(f'SELECT * FROM {table_name}', con=db_connection)
    return df

# function to get data from mongodb
def get_from_mongodb(table_name):
    client = MongoClient("mongodb://localhost:27017/")
    db = client['tpch']

    data = db[table_name].find()
    df = pd.DataFrame(list(data))
    return df

# get data from mysql and mongodb
df_lineitem = get_from_mongodb('lineitem')

# filter data and calculate the revenue
df_lineitem["L_SHIPDATE"] = pd.to_datetime(df_lineitem["L_SHIPDATE"])
mask = (df_lineitem['L_SHIPDATE'] >= '1994-01-01') & (df_lineitem['L_SHIPDATE'] < '1995-01-01') & \
       (df_lineitem['L_DISCOUNT'] >= (.06 - 0.01)) & (df_lineitem['L_DISCOUNT'] <= (.06 + 0.01)) & \
       (df_lineitem['L_QUANTITY'] < 24)

df_filtered = df_lineitem.loc[mask]
df_filtered['REVENUE'] = df_filtered['L_EXTENDEDPRICE'] * df_filtered['L_DISCOUNT']  
df_grouped = df_filtered.groupby(level=0, axis=0).sum()

# write the output to a csv file
df_grouped.to_csv("query_output.csv", columns=["REVENUE"])
