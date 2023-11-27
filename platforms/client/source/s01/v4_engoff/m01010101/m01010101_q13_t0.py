import pymysql
import pymongo
import csv

# Function to execute MySQL query and retrieve results
def fetch_mysql_data():
    # Connect to the MySQL database
    mysql_conn = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch',
        charset='utf8mb4'
    )

    # Prepare MySQL query
    mysql_query = """
    SELECT O_CUSTKEY, COUNT(*) AS order_count
    FROM orders
    WHERE O_ORDERSTATUS NOT IN ('pending', 'deposits')
    AND O_COMMENT NOT REGEXP '.*pending.*|.*deposits.*'
    GROUP BY O_CUSTKEY
    """

    # Execute query and fetch results
    with mysql_conn.cursor() as cursor:
        cursor.execute(mysql_query)
        mysql_results = {row[0]: row[1] for row in cursor.fetchall()}

    # Close the MySQL connection
    mysql_conn.close()
    return mysql_results

# Function to execute MongoDB query and retrieve results
def fetch_mongo_data():
    # Connect to the MongoDB database
    mongo_client = pymongo.MongoClient('mongodb', 27017)
    mdb = mongo_client['tpch']
    customer_collection = mdb['customer']

    # MongoDB does not require an explicit query to fetch all customers
    mongo_results = {}
    for doc in customer_collection.find({}):
        mongo_results[doc['C_CUSTKEY']] = 0    # Initialize customer order count to 0

    return mongo_results

# Merge MySQL and MongoDB results
def merge_results(mysql_data, mongo_data):
    # Add MySQL data to MongoDB results
    for custkey, order_count in mysql_data.items():
        mongo_data[custkey] = order_count

    # Convert result to a list of tuples sorted by the number of orders
    results_list = sorted(mongo_data.items(), key=lambda x: x[1])

    # Calculate distribution of customers by number of orders
    distribution = {}
    for _, count in results_list:
        distribution[count] = distribution.get(count, 0) + 1

    return distribution

# Write output to a CSV file
def write_to_csv(distribution):
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Number of Orders', 'Number of Customers'])
        for num_orders, num_customers in distribution.items():
            writer.writerow([num_orders, num_customers])

# Main function to execute the query
def main():
    mysql_data = fetch_mysql_data()
    mongo_data = fetch_mongo_data()
    distribution = merge_results(mysql_data, mongo_data)
    write_to_csv(distribution)

if __name__ == '__main__':
    main()
