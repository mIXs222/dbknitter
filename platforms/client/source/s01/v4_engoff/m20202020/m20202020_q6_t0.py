import pymysql
import csv

# MySQL connection function
def connect_to_mysql():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        database='tpch',
        charset='utf8mb4'
    )

# SQL query
sql = '''
SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) as `revenue_increase`
FROM lineitem
WHERE
    L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01' AND
    L_DISCOUNT BETWEEN 0.06 - 0.01 AND 0.06 + 0.01 AND
    L_QUANTITY < 24;
'''

# Main function to execute the query and save results to CSV
def main():
    connection = connect_to_mysql()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()
            revenue_increase = result['revenue_increase'] if result['revenue_increase'] else 0

        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['revenue_increase'])
            writer.writerow([revenue_increase])

    finally:
        connection.close()


if __name__ == "__main__":
    main()
