import pymysql
import csv
from datetime import datetime

# Function to query the MySQL database
def query_mysql():
    # Connection details
    mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    cursor = mysql_connection.cursor()
    
    # Query
    query = """
    SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as total_revenue
    FROM lineitem
    WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE <= '1994-12-31'
    AND L_DISCOUNT BETWEEN 0.05 AND 0.07
    AND L_QUANTITY < 24;
    """
    
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    mysql_connection.close()
    
    return result[0] if result else 0

# Main execution
if __name__ == "__main__":
    total_revenue = query_mysql()
    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['total_revenue']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerow({'total_revenue': total_revenue})
