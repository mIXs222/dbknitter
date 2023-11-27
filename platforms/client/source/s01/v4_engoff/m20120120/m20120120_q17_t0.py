import pymysql
import pymongo
import csv

def get_mysql_connection(db_name, user, password, host):
    connection = pymysql.connect(host=host,
                                 user=user,
                                 password=password,
                                 database=db_name,
                                 cursorclass=pymysql.cursors.Cursor)
    return connection

def get_lineitem_avg_quantity(mysql_connection):
    avg_quantity_query = """
    SELECT AVG(L_QUANTITY) as avg_quantity
    FROM lineitem
    WHERE L_PARTKEY IN (
        SELECT P_PARTKEY
        FROM part
        WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'
    )
    """
    with mysql_connection.cursor() as cursor:
        cursor.execute(avg_quantity_query)
        result = cursor.fetchone()
        return result[0] if result else None

def get_part_keys(mongo_client, dbname):
    db = mongo_client[dbname]
    part_collection = db["part"]
    part_keys = part_collection.find(
        {"P_BRAND": "Brand#23", "P_CONTAINER": "MED BAG"},
        {"P_PARTKEY": 1}
    )
    return [part["P_PARTKEY"] for part in part_keys]

def get_revenue_loss(mysql_connection, avg_quantity_cutoff):
    revenue_loss_query = """
    SELECT SUM(L_EXTENDEDPRICE) as revenue_loss
    FROM lineitem
    WHERE L_QUANTITY < %s AND L_PARTKEY IN (
        SELECT P_PARTKEY
        FROM part
        WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'
    )
    """
    with mysql_connection.cursor() as cursor:
        cursor.execute(revenue_loss_query, (avg_quantity_cutoff,))
        result = cursor.fetchone()
        return result[0] if result else 0

def calculate_average_yearly_loss(revenue_loss, years=7):
    return revenue_loss / years if revenue_loss is not None else 0

# Connect to MySQL
mysql_conn = get_mysql_connection(
    db_name='tpch',
    user='root',
    password='my-secret-pw',
    host='mysql'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
part_keys = get_part_keys(mongo_client, 'tpch')

# Get the average quantity from MySQL
avg_quantity = get_lineitem_avg_quantity(mysql_conn)

# Calculate the cutoff for small quantities (less than 20% of the average)
quantity_cutoff = avg_quantity * 0.20

# Get the total lost revenue for small quantities
revenue_loss = get_revenue_loss(mysql_conn, quantity_cutoff)

# Calculate the average yearly loss
average_yearly_loss = calculate_average_yearly_loss(revenue_loss)

# Close the MySQL connection
mysql_conn.close()

# Write results to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["average_yearly_loss"])
    writer.writerow([average_yearly_loss])
