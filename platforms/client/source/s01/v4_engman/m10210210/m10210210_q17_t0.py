import pymysql
import pandas as pd

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# Execute the SQL query on the MySQL database
with mysql_conn.cursor() as cursor:
    cursor.execute("""
    SELECT L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT
    FROM lineitem
    WHERE L_PARTKEY IN (
        SELECT P_PARTKEY
        FROM part
        WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'
    )
    """)
    lineitem_data = cursor.fetchall()

# Close the MySQL connection
mysql_conn.close()

# Convert the MySQL data to a pandas DataFrame
lineitem_df = pd.DataFrame(lineitem_data, columns=['L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])

# Calculate the average quantity
average_quantity = lineitem_df['L_QUANTITY'].mean()

# Filter out rows where quantity is less than 20% of the average quantity
filtered_df = lineitem_df[lineitem_df['L_QUANTITY'] < 0.2 * average_quantity]

# Calculate the undiscounted loss in revenue per row
filtered_df['UNDISCOUNTED_LOSS'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Calculate the total loss in revenue
total_loss = filtered_df['UNDISCOUNTED_LOSS'].sum()

# Assuming there are 7 years of data, calculate the average yearly loss
average_yearly_loss = total_loss / 7

# Save the output to a file
output_df = pd.DataFrame({'Average_Yearly_Loss': [average_yearly_loss]})
output_df.to_csv('query_output.csv', index=False)
