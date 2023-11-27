# import required libraries
import pymysql
import csv

# connection parameters
db_params = {
    'db': 'tpch',
    'user': 'root',
    'passwd': 'my-secret-pw',
    'host': 'mysql'
}

# SQL query
query = """
SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS revenue_increase
FROM lineitem
WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
  AND L_DISCOUNT BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
  AND L_QUANTITY < 24;
"""

try:
    # establish connection to database
    conn = pymysql.connect(**db_params)
    cursor = conn.cursor()

    # execute query
    cursor.execute(query)

    # fetch the result
    revenue_increase = cursor.fetchone()

    # write the result to csv file
    with open('query_output.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['revenue_increase'])
        writer.writerow([revenue_increase[0]])

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # close the cursor and connection
    cursor.close()
    conn.close()
