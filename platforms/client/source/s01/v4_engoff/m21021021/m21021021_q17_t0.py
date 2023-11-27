import pymysql
import pymongo
import pandas as pd

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Fetch parts from MySQL
part_sql = "SELECT P_PARTKEY, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER FROM part WHERE P_BRAND = 'Brand#23' AND P_TYPE LIKE '%MED BAG%'"
mysql_cursor.execute(part_sql)
part_results = mysql_cursor.fetchall()

# Convert fetch results to dataframe
part_df = pd.DataFrame(part_results, columns=['P_PARTKEY', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Find lineitems from MongoDB for the part keys obtained from MySQL
pipeline = [
    {
        '$match': {
            'L_PARTKEY': {'$in': list(part_df['P_PARTKEY'])}
        }
    },
    {
        '$group': {
            '_id': None,
            'averageQuantity': {'$avg': '$L_QUANTITY'}
        }
    },
    {
        '$project': {
            'averageQuantity': 1,
            '_id': 0
        }
    }
]
average_result = list(lineitem_collection.aggregate(pipeline))

if average_result:
    # Calculate 20% of the average quantity
    threshold_quantity = 0.2 * average_result[0]['averageQuantity']

    # Fetch lineitems with quantity less than the threshold
    pipeline_small_orders = [
        {
            '$match': {
                'L_PARTKEY': {'$in': list(part_df['P_PARTKEY'])},
                'L_QUANTITY': {'$lt': threshold_quantity}
            }
        },
        {
            '$group': {
                '_id': None,
                'averageYearlyLostRevenue': {'$avg': {'$multiply': ['$L_QUANTITY', '$L_EXTENDEDPRICE']}}
            }
        },
        {
            '$project': {
                'averageYearlyLostRevenue': 1,
                '_id': 0
            }
        }
    ]
    small_orders_result = list(lineitem_collection.aggregate(pipeline_small_orders))

    if small_orders_result:
        # Get the average yearly lost revenue
        average_yearly_lost_revenue = small_orders_result[0]['averageYearlyLostRevenue']

        # Write the result to CSV
        result_df = pd.DataFrame([{'average_yearly_lost_revenue': average_yearly_lost_revenue}])
        result_df.to_csv('query_output.csv', index=False)
else:
    # Write an empty CSV file when no average is calculated
    pd.DataFrame(columns=['average_yearly_lost_revenue']).to_csv('query_output.csv', index=False)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
