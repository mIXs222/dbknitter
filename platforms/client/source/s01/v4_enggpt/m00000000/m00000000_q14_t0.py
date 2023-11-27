import pymysql
import csv
from decimal import Decimal

# Database connection
conn_info = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "db": "tpch",
    "charset": "utf8mb4"
}

# Function to execute query in MySQL and return results
def query_mysql(query, connection_info):
    try:
        connection = pymysql.connect(**connection_info)
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        return results
    finally:
        connection.close()

# Define the SQL query
query = """
SELECT 
    SUM(CASE WHEN p.P_TYPE LIKE 'PROMO%%' THEN l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) ELSE 0 END) AS promo_revenue,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS total_revenue
FROM 
    lineitem l 
INNER JOIN 
    part p ON l.L_PARTKEY = p.P_PARTKEY
WHERE 
    l.L_SHIPDATE BETWEEN '1995-09-01' AND '1995-09-30';
"""

# Execute the query
results = query_mysql(query, conn_info)

# Calculate the percentage of promotional revenue
promo_revenue = results[0][0] if results and results[0] and results[0][0] else Decimal(0)
total_revenue = results[0][1] if results and results[0] and results[0][1] else Decimal(0)
promo_percentage = (promo_revenue / total_revenue * 100) if total_revenue else Decimal(0)

# Write output to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Promo Revenue', 'Total Revenue', 'Promo Revenue Percentage'])
    writer.writerow([str(promo_revenue), str(total_revenue), str(promo_percentage)])
