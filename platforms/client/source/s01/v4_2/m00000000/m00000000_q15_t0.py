import pymysql
import pandas as pd
from sqlalchemy import create_engine

# Establishing connection with the mysql database
con = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')

# Creating a sql alchemy engine to write to csv
engine = create_engine('mysql+pymysql://root:my-secret-pw@mysql/tpch')

# Executing the query on the relevant tables in the mysql database
with con:
    cur = con.cursor()
    query = """
    with revenue0 as
    (select
    L_SUPPKEY as SUPPLIER_NO,
    sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE
    from
    lineitem
    where
    L_SHIPDATE >= '1996-01-01'
    AND L_SHIPDATE < DATE_ADD('1996-01-01', INTERVAL 3 MONTH)
    group by
    L_SUPPKEY)
    select
    S_SUPPKEY,
    S_NAME,
    S_ADDRESS,
    S_PHONE,
    TOTAL_REVENUE
    from
    supplier,
    revenue0
    where
    S_SUPPKEY = SUPPLIER_NO
    and TOTAL_REVENUE = (
    select
    max(TOTAL_REVENUE)
    from
    revenue0
    )
    order by
    S_SUPPKEY
    """
    cur.execute(query)
    res = cur.fetchall()
    
df = pd.DataFrame(list(res))

# Writing the output to a csv file
df.to_csv('query_output.csv', index=False)
