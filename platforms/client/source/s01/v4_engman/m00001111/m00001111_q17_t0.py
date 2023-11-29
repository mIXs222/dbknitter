import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_db = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

def main():
    try:
        # MySQL: get part keys for BRAND#23 and MED BAG parts
        with mysql_db.cursor() as cursor:
            cursor.execute("""
                SELECT P_PARTKEY 
                FROM part 
                WHERE P_BRAND = 'BRAND#23' AND P_CONTAINER = 'MED BAG';
            """)
            part_keys = [row[0] for row in cursor.fetchall()]
        
        if part_keys:
            # Mongo: calculate average quantity of lineitems with the parts obtained above
            pipeline = [
                {"$match": {
                    "L_PARTKEY": {"$in": part_keys}
                }},
                {"$group": {
                    "_id": None,
                    "average_quantity": {"$avg": "$L_QUANTITY"}
                }}
            ]
            avg_result = list(mongodb_db.lineitem.aggregate(pipeline))
            average_quantity = avg_result[0]['average_quantity'] if avg_result else 0
            
            # Mongo: find lineitems with small quantities and calculate gross loss
            pipeline_loss = [
                {"$match": {
                    "L_PARTKEY": {"$in": part_keys},
                    "L_QUANTITY": {"$lt": average_quantity * 0.2}
                }},
                {"$group": {
                    "_id": None,
                    "total_loss": {"$sum": "$L_EXTENDEDPRICE"}
                }}
            ]
            loss_result = list(mongodb_db.lineitem.aggregate(pipeline_loss))
            total_loss = loss_result[0]['total_loss'] if loss_result else 0
            
            # Calculate the average yearly gross loss
            average_yearly_loss = total_loss / 7
            
            # Write the result to a CSV file
            with open('query_output.csv', 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['average_yearly_loss'])
                writer.writerow([average_yearly_loss])
    
    finally:
        mysql_db.close()
        mongodb_client.close()

if __name__ == "__main__":
    main()
