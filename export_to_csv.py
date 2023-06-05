"""
MIT License

Smart Schema Builder

Copyright (c) 2023 Kamil Ercan Turkarslan

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import os
import csv
import getpass
import click
import logging

import common.library as common_library
import db_utility.database as database_utility

common_library.configure_logging('export_to_csv.log', logging.INFO)


def export_table_to_csv(cursor, folder, schema, table_name):
    """
    Export a database table to a CSV file.

    Args:
        cursor: Database cursor object to execute SQL queries.
        folder (str): Path to the folder where the CSV file will be saved.
        schema (str): Name of the database schema containing the table.
        table_name (str): Name of the table to export.

    Returns:
        None
    """

    # Construct the path to the CSV file
    csv_path = os.path.join(folder, f"{table_name}.csv")

    # Open the CSV file in write mode
    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Fetch all rows from the table
        cursor.execute(f"SELECT * FROM {schema}.{table_name}")
        rows = cursor.fetchall()

        # Write the header row
        writer.writerow([desc[0] for desc in cursor.description])

        # Write the data rows
        for row in rows:
            writer.writerow(row)

    # Log the successful export
    logging.info(f"Table '{table_name}' exported to '{csv_path}'")


@click.command()
@click.option('--db-type', type=common_library.CaseInsensitiveChoice(database_utility.SUPPORTED_DATABASES),
              prompt='Enter the database type')
@click.option('--host', prompt='Enter the host', help='Database host')
@click.option('--port', default=None, type=int, help='Enter the database port number')
@click.option('--database', prompt='Enter the database name', help='Database name')
@click.option('--username', prompt='Enter the username', help='Database username')
@click.option('--schema', default='public', help='Schema name')
@click.option('--folder', prompt='Enter the folder path', help='Folder path to save CSV files')
def export_tables_to_csv(db_type, host, port, database, username, schema, folder):
    """
    Export all tables from a database to CSV files.
    Supports PostgreSQL, MySQL, and SQL Server databases.
    """

    # Connect to the database
    password = common_library.get_str_from_env("DB_PASSWORD")
    # If the password is not set as an environment variable, prompt the user for it
    if not password:
        password = getpass.getpass('Enter the password: ')

    conn = None
    cursor = None

    try:
        conn = database_utility.connect_to_database(db_type, host, port, database, username, password)

        cursor = conn.cursor()
        # Get a list of all tables
        cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema='{schema}'")
        tables = cursor.fetchall()

        folder = os.path.abspath(folder)
        # Create the folder if it doesn't exist
        os.makedirs(folder, exist_ok=True)

        # Export each table to a CSV file
        for table in tables:
            export_table_to_csv(cursor, folder, schema, table[0])

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return

    finally:
        # Close the database connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    logging.info("Table export completed successfully.")


if __name__ == '__main__':
    export_tables_to_csv()
