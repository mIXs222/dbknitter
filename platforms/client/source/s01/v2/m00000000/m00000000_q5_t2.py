import pandas as pd
from sqlalchemy import create_engine

# Creating connections
mysql_engine = create_engine('mysql://root:my-secret-pw@mysql/tpch')

# Fetching Data
def get_data(query, engine):
    return pd.read_sql(query, engine)

# Fetching tables from MySQL
nation = get_data('SELECT * FROM nation', mysql_engine)
region = get_data('SELECT * FROM region', mysql_engine)
supplier = get_data('SELECT * FROM supplier', mysql_engine)
customer = get_data('SELECT * FROM customer', mysql_engine)
orders = get_data('SELECT * FROM orders', mysql_engine)
lineitem = get_data('SELECT * FROM lineitem', mysql_engine)

# Merge DataFrames (Add your merging conditions)
merged_df = pd.merge(customer, orders, on='C_CUSTKEY') \
    .merge(lineitem, on='O_ORDERKEY') \
    .merge(supplier, on='S_SUPPKEY') \
    .merge(nation, on='N_NATIONKEY') \
    .merge(region, on='R_REGIONKEY')

# Execute original SQL query on the merged DataFrame
query = """
    SELECT
        N_NAME,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
    FROM
        merged_df
    WHERE
        R_NAME = 'ASIA'
        AND O_ORDERDATE >= '1990-01-01'
        AND O_ORDERDATE < '1995-01-01'
    GROUP BY
        N_NAME
    ORDER BY
        REVENUE DESC
"""

result_df = pd.read_sql(query, merged_df)
result_df.to_csv('query_output.csv', index=False)
