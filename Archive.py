import pyodbc
from datetime import datetime, timedelta

def create_archive_table(cursor, source_table_name, archive_table_name, archive_condition):
    create_table_query = f"""
    SELECT *
    INTO {archive_table_name}
    FROM {source_table_name}
    WHERE {archive_condition};
    """
    cursor.execute(create_table_query)
    print(f"Data meeting the condition from '{source_table_name}' to '{archive_table_name}' successfully.")

def archive_data(cursor, source_table_name, archive_table_name, archive_condition):
    # Check if data meets the archive condition before archiving
    check_query = f"SELECT COUNT(*) FROM {source_table_name} WHERE {archive_condition};"
    cursor.execute(check_query)
    count = cursor.fetchone()[0]

    if count > 0:
        # Archive data only if there is data meeting the condition and not already in the backup table
        archive_query = f"""
        INSERT INTO {archive_table_name}
        SELECT *
        FROM {source_table_name} AS src
        WHERE {archive_condition}
        AND NOT EXISTS (
            SELECT 1
            FROM {archive_table_name} AS dest
            WHERE {archive_condition}
            AND src.id = dest.id -- Assuming 'id' is the primary key
        );
        """
        cursor.execute(archive_query)
        print(f"Data archived from '{source_table_name}' to '{archive_table_name}' successfully.")

        # Uncomment the following section to delete the archived data from the source table
        delete_query = f"DELETE FROM {source_table_name} WHERE {archive_condition};"
        cursor.execute(delete_query)
        print(f"Archived data deleted from '{source_table_name}'.")
    else:
        print(f"No data meets the condition in '{source_table_name}'. Nothing archived.")

def get_table_structure(cursor, source_table_name):
    cursor.execute(f"SELECT TOP 1 * FROM {source_table_name}")
    columns = [column[0] for column in cursor.description]

    # Fetching column information with data types
    columns_info = cursor.columns(table=source_table_name)

    # Creating a list of "column_name data_type" pairs
    column_data_types = [f'{column[3]} {column[5]}' for column in columns_info]

    return ', '.join(column_data_types)

def main():
    # Set your server, database, and table details
    server = 'KCSLAP5263\SQLEXPRESS2019'
    database = 'Test'
    source_table = 'Original'
    archive_table = 'bkpOriginal'

    # Prompt the user for the desired year
    year_to_archive = int(input("Enter the year to archive data (e.g., 2019): "))

    # Define the condition to identify data to be archived based on the user input
    archive_condition = f"YEAR(date_time) = {year_to_archive}"  # Assuming 'date_time' is the actual date column in your table

    # Create a connection string with Windows authentication
    conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"

    # Connect to the database
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cursor:
            # Step 1: Get the structure of the source table
            source_table_structure = get_table_structure(cursor, source_table)

            # Step 2: Create an archive table based on the source table structure
            create_archive_table(cursor, source_table, archive_table, archive_condition)

            # Step 3: Archive data based on the condition and delete from the source table
            archive_data(cursor, source_table, archive_table, archive_condition)

if __name__ == "__main__":
    main()
