import pandas as pd
import redis 
from pandasql import sqldf

#Function to execute SQL over Pandas DataFrame
pysqldf = lambda q: sqldf(q, globals())

#Connect to redis
r = redis.Redis(host='redis', port=6379, db=0)

#Read the data from redis
customers = pd.read_json(r.get('customer'), orient='records')
orders = pd.read_json(r.get('orders'), orient='records')
lineitem = pd.read_json(r.get('lineitem'), orient='records')
nation = pd.read_json(r.get('nation'), orient='records')

#Execute the query
query = """
    SELECT
        C_CUSTKEY,
        C_NAME,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
        C_ACCTBAL,
        N_NAME,
        C_ADDRESS,
        C_PHONE,
        C_COMMENT
    FROM
        customers,
        orders,
        lineitem,
        nation
    WHERE
        C_CUSTKEY = O_CUSTKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND O_ORDERDATE >= '1993-10-01'
        AND O_ORDERDATE < '1994-01-01'
        AND L_RETURNFLAG = 'R'
        AND C_NATIONKEY = N_NATIONKEY
    GROUP BY
        C_CUSTKEY,
        C_NAME,
        C_ACCTBAL,
        C_PHONE,
        N_NAME,
        C_ADDRESS,
        C_COMMENT
    ORDER BY
        REVENUE, C_CUSTKEY, C_NAME, C_ACCTBAL DESC"""
result = pysqldf(query)

#Save the result 
result.to_csv('query_output.csv', index=False)
