import csv
import pymysql
from pymongo import MongoClient

# MySQL connection and query
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)
with mysql_conn.cursor() as cursor:
    # Fetch parts that match the criteria from the MongoDB database and prepare filter for MySQL query
    mongo_client = MongoClient('mongodb', 27017)
    mongodb = mongo_client['tpch']
    part_collection = mongodb['part']
    
    # Find P_PARTKEYs which satisfy the conditions in the MongoDB database
    partkeys = part_collection.find(
        {'P_BRAND': 'Brand#23', 'P_CONTAINER': 'MED BAG'},
        {'P_PARTKEY': 1}
    )
    partkey_list = [part['P_PARTKEY'] for part in partkeys]
    
    # Continue with MySQL only if we have valid partkeys
    if partkey_list:
        # The inner SQL query
        subquery = """
        SELECT
            0.2 * AVG(L_QUANTITY)
        FROM
            lineitem
        WHERE
            L_PARTKEY = %s
        """
        
        # Aggregate query for line items using the previously obtained partkeys
        aggregate_query = """
        SELECT
            SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
        FROM
            lineitem
        WHERE
            L_PARTKEY IN (%s)
        """
        
        # Placeholder string with appropriate number of placeholders for SQL IN clause
        placeholders = ', '.join(['%s'] * len(partkey_list))
        aggregate_query_formatted = aggregate_query % placeholders
        
        # Execute inner query for each part and calculate the average yearly extended price
        avg_yearly_sum = 0
        for partkey in partkey_list:
            cursor.execute(subquery, (partkey,))
            quantity_threshold, = cursor.fetchone()
            cursor.execute("""SELECT L_QUANTITY, L_EXTENDEDPRICE 
                              FROM lineitem 
                              WHERE L_PARTKEY = %s""", (partkey,))
            for l_quantity, l_extendedprice in cursor:
                if l_quantity < quantity_threshold:
                    avg_yearly_sum += l_extendedprice
        
        # Final output divided by 7 to get the average
        avg_yearly = avg_yearly_sum / 7.0
        
        # Write result to file
        with open('query_output.csv', 'w', newline='') as file:
            csvwriter = csv.writer(file)
            csvwriter.writerow(['AVG_YEARLY'])
            csvwriter.writerow([avg_yearly])

    mysql_conn.close()
