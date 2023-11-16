import pandas as pd
from sqlalchemy import create_engine

# Connect to mysql database
mysql_engine = create_engine("mysql+pymysql://root:my-secret-pw@mysql/tpch")

# Define the required SQL queries to get data from mysql
query_supplier = "SELECT * FROM supplier WHERE S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'CANADA')"
query_partsupp = "SELECT * FROM partsupp"
query_nation = "SELECT * FROM nation WHERE N_NAME = 'CANADA'"

# Execute the SQL queries using pandas
suppliers_df = pd.read_sql_query(query_supplier, mysql_engine)
partsupp_df = pd.read_sql_query(query_partsupp, mysql_engine)
nation_df = pd.read_sql_query(query_nation, mysql_engine)

# Get parts with names that start with 'forest' from the part table
query_part = "SELECT * FROM part WHERE P_NAME LIKE 'forest%'"
part_df = pd.read_sql_query(query_part, mysql_engine)

# Get lineitems shipped between '1994-01-01' and '1995-01-01'
query_lineitem = "SELECT * FROM lineitem WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01'"
lineitem_df = pd.read_sql_query(query_lineitem, mysql_engine)

# Merge suppliers and partsupp on suppkey
join1 = pd.merge(suppliers_df, partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Merge join1 and part on partkey
join2 = pd.merge(join1, part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Merge join2 and lineitem on partkey and suppkey
join3 = pd.merge(join2, lineitem_df, left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])

# Calculate the sum of quantity, group by partkey and suppkey
quantity_sum = lineitem_df.groupby(['L_PARTKEY', 'L_SUPPKEY']).L_QUANTITY.sum().reset_index()

# Filter dataframe for suppliers whose available quantity > 0.5*sum_quantity
quantity_sum['0.5*QTY'] = 0.5 * quantity_sum['L_QUANTITY']
final_df = join3[join3['PS_AVAILQTY'] > quantity_sum['0.5*QTY']]

# Select the required columns, order by S_NAME
output_df = final_df[['S_NAME', 'S_ADDRESS']].sort_values(by=['S_NAME'])

# Save the final output to a CSV file
output_df.to_csv('query_output.csv', index=False)
