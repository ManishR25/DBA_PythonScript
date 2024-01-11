import pandas as pd
import pyodbc
import shutil
from pathlib import Path

# Replace these values with your SQL Server connection details
server = 'kcs-ovh-mssql01.kcsitglobal.com\SQL2014'
database = 'ClubO7_QC'
username = 'eCubeeHSM'
password = 'eCubeeHSM'

# Create a connection string
connection_string = f'DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Connect to the SQL Server database
conn = pyodbc.connect(connection_string)

# Replace 'your_table' with the actual table name
table_name = '[dbo].[CRMMembershipTaxDetail]'

# SQL query to fetch data from the table
sql_query = f'SELECT * FROM {table_name}'

# Use pandas to read the SQL query result into a DataFrame
df = pd.read_sql(sql_query, conn)

# Close the database connection
conn.close()

# Replace 'output.csv' with the desired output file name
output_file = 'output.csv'

# Save the DataFrame to a CSV file
df.to_csv(output_file, index=False)

# Compress the CSV file into a zip file
zip_file = 'output.zip'
with shutil.ZipFile(zip_file, 'w') as zipf:
    zipf.write(output_file, arcname=Path(output_file).name)

print(f'CSV file "{output_file}" compressed to "{zip_file}" successfully.')
