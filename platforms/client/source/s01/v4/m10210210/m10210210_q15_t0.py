import pymysql
import pymongo
import csv
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Calculate the required date range
start_date = datetime.strptime('1996-01-01', "%Y-%m-%d")
end_date = start_date + relativedelta(months=3)

# Query MySQL to calculate revenue
with mysql_conn.cursor() as cursor:
    revenue_query = """
    select
        L_SUPPKEY as SUPPLIER_NO,
        sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE
    from
        lineitem
    where
        L_SHIPDATE >= %s
    AND L_SHIPDATE < %s
    group by
        L_SUPPKEY
    """
    cursor.execute(revenue_query, (start_date, end_date))
    revenue_data = cursor.fetchall()

# Find the maximum revenue among the suppliers
max_revenue = max(revenue_data, key=lambda x: x[1])[1]

# Gather the suppliers with the maximum revenue
max_revenue_suppliers = [supplier[0] for supplier in revenue_data if supplier[1] == max_revenue]

# Query MongoDB to get supplier details
supplier_details = list(mongodb.supplier.find({'S_SUPPKEY': {'$in': max_revenue_suppliers}}))

# Create a combined result
combined_result = []
for supplier in supplier_details:
    supplier_dict = {
        'S_SUPPKEY': supplier['S_SUPPKEY'],
        'S_NAME': supplier['S_NAME'],
        'S_ADDRESS': supplier['S_ADDRESS'],
        'S_PHONE': supplier['S_PHONE'],
        'TOTAL_REVENUE': next(
            (revenue for revenue in revenue_data if revenue[0] == supplier['S_SUPPKEY']), (None, None)
        )[1]
    }
    combined_result.append(supplier_dict)

# Sort result by S_SUPPKEY
combined_result.sort(key=lambda x: x['S_SUPPKEY'])

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(combined_result)

# Close connections
mysql_conn.close()
mongo_client.close()
