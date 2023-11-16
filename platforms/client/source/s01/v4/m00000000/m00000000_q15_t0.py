# query.py
import pymysql
import csv

# Define connection parameters
connection_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4'
}

# SQL query
sql_query = """
with revenue0 as
(select
  L_SUPPKEY as SUPPLIER_NO,
  sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE
  from
  lineitem
  where
  L_SHIPDATE >= '1996-01-01'
  AND L_SHIPDATE < date('1996-01-01' + interval 3 month)
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
S_SUPPKEY;
"""

# Connect to the MySQL database
connection = pymysql.connect(**connection_params)

try:
    with connection.cursor() as cursor:
        cursor.execute(sql_query)

        # Fetch results
        results = cursor.fetchall()
        
        # Write to csv file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            
            # write header
            csvwriter.writerow([i[0] for i in cursor.description])
            
            # write data
            for row in results:
                csvwriter.writerow(row)

finally:
    # Close the connection
    connection.close()
