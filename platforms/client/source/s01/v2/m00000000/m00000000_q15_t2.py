import mysql.connector
import pandas as pd

# create a connection to the database
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# execute the query
mycursor = mydb.cursor()

mycursor.execute("""
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
""")

# fetch the results
results = mycursor.fetchall()

# write the output to csv file
df = pd.DataFrame(results)
df.to_csv('query_output.csv', index=False)
