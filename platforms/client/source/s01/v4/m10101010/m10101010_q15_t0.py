import pymysql
import csv

# Connect to MySQL database
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
cur = conn.cursor()

# Write the SQL query
query = """
with revenue0 as
(select
  L_SUPPKEY as SUPPLIER_NO,
  sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE
  from
  lineitem
  where
  L_SHIPDATE >= '1996-01-01'
  AND L_SHIPDATE < '1996-04-01'
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

# Execute the query
cur.execute(query)

# Fetch all the results
results = cur.fetchall()

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])
    for row in results:
        writer.writerow(row)

# Close the cursor and connection
cur.close()
conn.close()
