import pymysql
import pymongo
import csv

# Function to connect to MySQL and get the lineitem data
def get_mysql_data():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch')
    cursor = connection.cursor()
    query = """
    SELECT
        L_PARTKEY, L_EXTENDEDPRICE, L_QUANTITY
    FROM
        lineitem
    """
    cursor.execute(query)
    lineitem_data = cursor.fetchall()
    cursor.close()
    connection.close()
    return lineitem_data

# Function to connect to MongoDB and get the part data
def get_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    part_data = list(db.part.find(
        {
            'P_BRAND': 'Brand#23',
            'P_CONTAINER': 'MED BAG'
        },
        {
            '_id': 0,
            'P_PARTKEY': 1
        }
    ))
    client.close()
    return part_data

# Function to calculate AVG_YEARLY
def calculate_avg_yearly(lineitem_data, part_data):
    # Convert part_data to a dictionary for faster lookup
    part_keys = {part['P_PARTKEY'] for part in part_data}
    
    # Calculate average quantity for each part
    part_avg_qty = {}
    for part_key in part_keys:
        part_qty = [qty for (pkey, price, qty) in lineitem_data if pkey == part_key]
        if part_qty:
            part_avg_qty[part_key] = sum(part_qty) / len(part_qty)
    
    # Calculate SUM(L_EXTENDEDPRICE) for qualified line items
    total_extended_price = 0.0
    for (pkey, price, qty) in lineitem_data:
        if pkey in part_keys and qty < 0.2 * part_avg_qty[pkey]:
            total_extended_price += price

    # Calculate AVG_YEARLY
    avg_yearly = total_extended_price / 7.0
    return avg_yearly

# Main code execution
def main():
    # Retrieve data from databases
    lineitem_data = get_mysql_data()
    part_data = get_mongodb_data()

    # Perform the calculation for AVG_YEARLY
    avg_yearly = calculate_avg_yearly(lineitem_data, part_data)

    # Output result to a CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['AVG_YEARLY']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({'AVG_YEARLY': avg_yearly})

if __name__ == "__main__":
    main()
