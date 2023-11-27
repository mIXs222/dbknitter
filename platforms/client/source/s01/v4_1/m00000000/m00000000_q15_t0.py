import pymysql
import pandas as pd
import csv

# Connect to your MySQL Database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

query="""
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

# execute the query and fetch the result
with connection.cursor() as cursor:
    cursor.execute(query)
    data = cursor.fetchall()

# write the result to the csv file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)

# close the connection
connection.close()
