import pymysql
import pymongo
import pandas as pd

mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

mongoclient = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongoclient["tpch"]

with mysql_conn.cursor() as cursor:
    sql_query = """
    SELECT
        N_NAME,
        S_SUPPKEY,
        S_NATIONKEY
    FROM
        nation,
        supplier
    WHERE
        S_NATIONKEY = N_NATIONKEY
    """
    cursor.execute(sql_query)
    result_mysql = cursor.fetchall()
    df_mysql = pd.DataFrame(result_mysql, columns = ['N_NAME', 'S_SUPPKEY', 'S_NATIONKEY'])

customers = mongodb['customer'].find()
orders = mongodb['orders'].find()
lineitems = mongodb['lineitem'].find()

df_customers = pd.DataFrame(list(customers))
df_orders = pd.DataFrame(list(orders))
df_lineitems = pd.DataFrame(list(lineitems))

df = pd.merge(df_customers, df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df = pd.merge(df, df_lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df = pd.merge(df, df_mysql, left_on='N_NATIONKEY', right_on='S SUPPKEY')

df['O_ORDERDATE'] = pd.to_datetime(df['O_ORDERDATE'])
mask = (df['O_ORDERDATE'] >= '1990-01-01') & (df['O_ORDERDATE'] < '1995-01-01')
df = df.loc[mask]

df['REVENUE'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])

result_df = df.groupby(['N_NAME'])['REVENUE'].sum()
result_df = result_df.reset_index().sort_values('REVENUE', ascending=False)

result_df.to_csv('query_output.csv', index=False)
