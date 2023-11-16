import pandas as pd
from redis import Redis
from pandasql import sqldf

# Set up connection
conn = Redis(host='redis', port=6379, db=0)
direct_redis = conn

#Get data from redis
nation = pd.DataFrame(direct_redis.get('nation'))
part = pd.DataFrame(direct_redis.get('part'))
supplier = pd.DataFrame(direct_redis.get('supplier'))
partsupp = pd.DataFrame(direct_redis.get('partsupp'))
orders = pd.DataFrame(direct_redis.get('orders'))
lineitem = pd.DataFrame(direct_redis.get('lineitem'))

#Change the column names to lowercase
nation.columns = map(str.lower, nation.columns)
part.columns = map(str.lower, part.columns)
supplier.columns = map(str.lower, supplier.columns)
partsupp.columns = map(str.lower, partsupp.columns)
orders.columns = map(str.lower, orders.columns)
lineitem.columns = map(str.lower, lineitem.columns)

# Joining the dataframes
combined = pd.concat([nation, part, supplier, partsupp, orders, lineitem])

# Query the required output
query = """
SELECT
    NATION,
    strftime('%Y', O_ORDERDATE) AS O_YEAR,
    SUM(AMOUNT) AS SUM_PROFIT
FROM
    (
    SELECT
        N_NAME AS NATION,
        O_ORDERDATE AS O_YEAR,
        L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY AS AMOUNT
    FROM
        combined
    WHERE
        S_SUPPKEY = L_SUPPKEY
        AND PS_SUPPKEY = L_SUPPKEY
        AND PS_PARTKEY = L_PARTKEY
        AND P_PARTKEY = L_PARTKEY
        AND O_ORDERKEY = L_ORDERKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND P_NAME LIKE '%dim%'
    ) AS PROFIT
GROUP BY
    NATION,
    O_YEAR
ORDER BY
    NATION,
    O_YEAR DESC
"""

result = sqldf(query)

# Write Result to CSV
result.to_csv('query_output.csv', index=False)
