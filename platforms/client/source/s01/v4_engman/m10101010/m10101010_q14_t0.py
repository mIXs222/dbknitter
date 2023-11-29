# query.py

import pymysql
import pymongo
import csv
from datetime import datetime

# Define MongoDB connection and query
def get_promotional_parts():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    promotional_parts = db.part.find({}, {'P_PARTKEY': 1})
    promotional_part_keys = [pp['P_PARTKEY'] for pp in promotional_parts]
    return promotional_part_keys

# Define MySQL connection and query
def get_revenue_for_promotional_parts(promotional_part_keys):
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch')
    try:
        with connection.cursor() as cursor:
            sql = """
            SELECT
                SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
            FROM
                lineitem
            WHERE
                L_SHIPDATE >= '1995-09-01' AND
                L_SHIPDATE < '1995-10-01' AND
                L_PARTKEY IN (%s)
            """
            in_p = ', '.join(list(map(lambda x: '%s', promotional_part_keys)))
            sql = sql % in_p
            cursor.execute(sql, promotional_part_keys)
            result = cursor.fetchone()
            return result if result else (0,)
    finally:
        connection.close()

# Main program flow
def main():
    promotional_part_keys = get_promotional_parts()
    total_revenue = get_revenue_for_promotional_parts(promotional_part_keys)

    # Calculate revenue as a percentage
    total_revenue_amount = total_revenue[0] if total_revenue[0] else 0
    revenue_percentage = (total_revenue_amount / total_revenue_amount) * 100 if total_revenue_amount else 0

    # Write results to a CSV file
    with open('query_output.csv', mode='w') as file:
        writer = csv.writer(file)
        writer.writerow(['revenue_percentage'])
        writer.writerow([revenue_percentage])

# Execute the script
if __name__ == "__main__":
    main()
