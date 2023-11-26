import pymysql
import csv

# Connection parameters
conn_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# SQL query
sql_query = """
with revenue0 as (
  select
    L_SUPPKEY as SUPPLIER_NO,
    sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE
    from
    lineitem
    where
    L_SHIPDATE >= '1996-01-01'
    and L_SHIPDATE < '1996-04-01'  -- Modified for compatibility with MySQL
    group by
    L_SUPPKEY
)
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

# Execute the query and write to CSV
try:
    # Establishing the connection
    conn = pymysql.connect(**conn_params)
    
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    
    # Executing the SQL command
    cursor.execute(sql_query)
    
    # Fetch all rows
    results = cursor.fetchall()
    
    # Writing query results to a CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])  # header
        csvwriter.writerows(results)

    print("Query executed successfully. Results are saved in 'query_output.csv'.")
except pymysql.MySQLError as e:
    print("Error while connecting to MySQL", e)
finally:
    # Closing the connection
    if conn:
        cursor.close()
        conn.close()
