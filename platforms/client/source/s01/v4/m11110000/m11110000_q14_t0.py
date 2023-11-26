# code.py
import pymysql
import pymongo
import csv

# Define function to get MySQL connection
def get_mysql_connection():
    return pymysql.connect(
        user='root',
        password='my-secret-pw',
        host='mysql',
        database='tpch'
    )

# Define function to get MongoDB connection
def get_mongo_client():
    return pymongo.MongoClient('mongodb', 27017)

# Connect to MySQL
mysql_conn = get_mysql_connection()

# Connect to MongoDB
mongo_client = get_mongo_client()
mongodb = mongo_client['tpch']
part_collection = mongodb['part']

# Find the part keys from the MongoDB with the specified condition
promo_parts = part_collection.find({"P_TYPE": {"$regex": "^PROMO"}})
# Creating a dictionary for part keys with P_TYPE like 'PROMO%'
part_keys = {item['P_PARTKEY']:item['P_TYPE'].startswith('PROMO') for item in promo_parts}

# Process the MySQL data
try:
    with mysql_conn.cursor() as cursor:
        # Select data from lineitem table given the condition
        cursor.execute("""
            SELECT
                L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE
            FROM
                lineitem
            WHERE
                L_SHIPDATE >= '1995-09-01'
                AND L_SHIPDATE < '1995-10-01'
        """)
        result_set = cursor.fetchall()
        
        # Initialize the SUM values for the calculation
        promo_sum = 0
        total_sum = 0
        
        # Calculate the sum values only for the relevant items
        for (l_partkey, l_extendedprice, l_discount, _) in result_set:
            if l_partkey in part_keys:
                adjusted_price = l_extendedprice * (1 - l_discount)
                total_sum += adjusted_price
                if part_keys[l_partkey]:
                    promo_sum += adjusted_price
        
        # Calculate the promo revenue percentage
        promo_revenue = 100.0 * promo_sum / total_sum if total_sum else None

        # Write the query result to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            result_writer = csv.writer(csvfile)
            result_writer.writerow(['PROMO_REVENUE'])
            result_writer.writerow([promo_revenue])

finally:
    # Close MySQL connection
    mysql_conn.close()
