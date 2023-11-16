import pandas as pd
import os
import csv
import pymysql

# connect to mysql
db_connection = pymysql.connect(host="mysql", user="root", password="my-secret-pw", db="tpch")
cursor = db_connection.cursor()

def to_csv(data, filename):
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(data)

def extract_data(sql_query):
    cursor.execute(sql_query)
    data = cursor.fetchall()
    return data

sql_queries = [
    "SELECT * FROM nation",
    "SELECT * FROM region",
    "SELECT * FROM orders",
    "SELECT * FROM customer",
    "SELECT * FROM supplier",
    "SELECT * FROM part",
    "SELECT * FROM lineitem"
]

for sql_query in sql_queries:
    data = extract_data(sql_query)
    filename = f"{os.getcwd()}/{sql_query.split('FROM ')[1]}.csv"
    to_csv(data, filename)

# combine data and execute the original query
# parts after 'as ALL_NATIONS' is not included and should be processed after this statement in pandas
sql_query_combined_data = """
SELECT
    strftime('%Y', O_ORDERDATE) AS O_YEAR,
    L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME,
    N2.N_NAME AS NATION
    FROM
        part,
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2,
        region
    WHERE
        P_PARTKEY = L_PARTKEY
        AND S_SUPPKEY = L_SUPPKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND O_CUSTKEY = C_CUSTKEY
        AND C_NATIONKEY = N1.N_NATIONKEY
        AND N1.N_REGIONKEY = R_REGIONKEY
        AND R_NAME = 'ASIA'
        AND S_NATIONKEY = N2.N_NATIONKEY
        AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
        AND P_TYPE = 'SMALL PLATED COPPER'
"""
combined_data = extract_data(sql_query_combined_data)

df_combined_data = pd.DataFrame(combined_data, columns=['O_YEAR', 'VOLUME', 'NATION'])

grouped_data = (df_combined_data
                .groupby(['O_YEAR'])
                .agg(
                    INDIAN_MKT_SHARE = pd.NamedAgg(column='VOLUME', aggfunc = lambda x: sum(df_combined_data['NATION']=='INDIA') / len(x))
                )
                .reset_index())

grouped_data.to_csv('/path/to/save/query_output.csv')
db_connection.close()
