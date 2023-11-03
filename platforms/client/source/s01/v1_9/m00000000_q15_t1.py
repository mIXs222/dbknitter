import mysql.connector
import pandas as pd

# connect to mysql
con = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')

# define the query
sql = """
with revenue0 as
(select
  L_SUPPKEY as SUPPLIER_NO,
  sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE
  from
  lineitem
  where
  L_SHIPDATE >= '1996-01-01'
  AND L_SHIPDATE < date('1996-01-01', '+3 month')
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

# execute the query and convert to a pandas dataframe
result = pd.read_sql_query(sql, con)

# write to csv
result.to_csv('query_output.csv', index=False)

# close the connection
con.close()
