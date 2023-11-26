import pymysql
import csv

# Function to connect to the MySQL database and execute the query
def fetch_data():
    # Connect to the MySQL Database
    conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    cursor = conn.cursor()
    
    # SQL Query
    query = """
    SELECT C_CUSTKEY, COUNT(O_ORDERKEY) AS num_orders
    FROM customer
    LEFT JOIN orders ON customer.C_CUSTKEY = orders.O_CUSTKEY AND NOT (orders.O_ORDERSTATUS = 'pending' OR orders.O_ORDERSTATUS = 'deposits' OR orders.O_COMMENT LIKE '%pending%' OR orders.O_COMMENT LIKE '%deposits%')
    GROUP BY C_CUSTKEY;
    """
    
    # Execute the query
    cursor.execute(query)
    
    # Fetch all the results
    results = cursor.fetchall()
    
    # Write results to CSV
    with open('query_output.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['C_CUSTKEY', 'num_orders'])
        for row in results:
            writer.writerow(row)

    # Close cursor and connection
    cursor.close()
    conn.close()

# Main function to fetch and output data
if __name__ == '__main__':
    fetch_data()
