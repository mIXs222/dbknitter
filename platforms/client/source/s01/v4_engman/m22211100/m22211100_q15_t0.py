import pymysql
import pymongo
from datetime import datetime
import csv

# Function to query MySQL
def query_mysql():
    # Connect to the MySQL Database
    mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
    cursor = mysql_conn.cursor()

    # Query to select data from lineitem table
    cursor.execute("""
    SELECT L_SUPPKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
    FROM lineitem
    WHERE L_SHIPDATE >= '1996-01-01' AND L_SHIPDATE < '1996-04-01'
    GROUP BY L_SUPPKEY;
    """)

    # Fetch the results
    results = {row[0]: row[1] for row in cursor.fetchall()}
    mysql_conn.close()
    return results

# Function to query MongoDB
def query_mongodb():
    # Connect to MongoDB
    client = pymongo.MongoClient(host='mongodb', port=27017)
    db = client['tpch']
    
    # Query to select data from supplier table
    suppliers = db['supplier'].find({})

    # Fetch the results
    supplier_info = {doc['S_SUPPKEY']: {
        "S_NAME": doc['S_NAME'],
        "S_ADDRESS": doc['S_ADDRESS'],
        "S_PHONE": doc['S_PHONE']
    } for doc in suppliers}
    return supplier_info

# Write results to a CSV file
def write_to_csv(supplier_data, lineitem_data):
    # Find the maximum revenue
    max_revenue = max(lineitem_data.values())

    # Filter suppliers with maximum revenue
    top_suppliers = {s_id: s_data for s_id, s_data in supplier_data.items() if lineitem_data.get(s_id, 0) == max_revenue}

    # Sort suppliers by supplier key (s_id)
    sorted_suppliers = sorted(top_suppliers.items())

    # Write to CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for supp_key, supp_info in sorted_suppliers:
            writer.writerow({
                'S_SUPPKEY': supp_key,
                'S_NAME': supp_info['S_NAME'],
                'S_ADDRESS': supp_info['S_ADDRESS'],
                'S_PHONE': supp_info['S_PHONE'],
                'TOTAL_REVENUE': lineitem_data[supp_key]
            })

# Main execution
def main():
    supplier_data = query_mongodb()
    lineitem_data = query_mysql()
    write_to_csv(supplier_data, lineitem_data)

if __name__ == "__main__":
    main()
