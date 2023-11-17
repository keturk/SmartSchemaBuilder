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
import click
import logging
import json

import common.library as common_library
from db_utility.database_utility import DatabaseUtility
from db_utility.database_table import DatabaseTable

# Configure logging
common_library.configure_logging('export_to_csv.log', logging.INFO)


def export_table_to_csv(db, folder, schema, table_name, limit=None, exported_rows=None, datatable_tables=None):
    """
    Export a portion of a database table to a CSV file.
    Exported rows and their references are tracked to ensure dependencies are exported.

    Args:
        db: DatabaseUtility object.
        folder (str): Path to the folder where the CSV file will be saved.
        schema (str): Name of the database schema containing the table.
        table_name (str): Name of the table to export.
        limit (int): Maximum number of records to export.
        exported_rows (dict): Dictionary to track exported rows and their references.
        datatable_tables (dict): Dictionary of DatabaseTable objects.

    Returns:
        None
    """

    if not exported_rows:
        exported_rows = {}

    # Check if the table has already been exported
    if table_name in exported_rows:
        return

    # Construct the path to the CSV file
    csv_path = os.path.join(folder, f"{table_name}.csv")

    # Get the DatabaseTable object from the table name
    table = datatable_tables[table_name]

    # Open the CSV file in write mode
    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Fetch the records from the table
        rows = db.get_rows(f'{schema}.{table_name}', limit=limit)

        # Write the header row
        writer.writerow([column.column_name for column in table.columns])

        # Write the data rows
        for row in rows:
            writer.writerow([str(value) for value in row])

            # Add the exported row to the dictionary
            exported_rows[table_name] = row[0]

            # Export referenced rows from referenced tables
            export_referenced_records(db, folder, schema, table_name, row, limit, exported_rows, datatable_tables)


def export_referenced_records(db, folder, schema, table_name, row, limit=None, exported_rows=None,
                              datatable_tables=None):
    """
    Export referenced records from referenced tables based on foreign key relationships.

    Args:
        db (DatabaseUtility): DatabaseUtility object.
        folder (str): Path to the folder where the CSV files will be saved.
        schema (str): Name of the database schema.
        table_name (str): Name of the table to export referenced records from.
        row (tuple): The exported row.
        limit (int): Maximum number of records to export.
        exported_rows (dict): Dictionary tracking exported rows and their references.
        datatable_tables (dict): Dictionary of DatabaseTable objects.

    Returns:
        None
    """

    if not exported_rows:
        exported_rows = {}

    # Get the DatabaseTable object from the table name
    table = datatable_tables[table_name]

    # Get the foreign keys for the table
    if table.has_foreign_keys():
        foreign_keys = table.foreign_keys

        # Iterate over the foreign keys and export referenced records
        for fk in foreign_keys:
            fk_column = fk.column_name
            ref_table = fk.referenced_table
            ref_column = fk.referenced_column

            # Check if the referenced table has already been exported
            if ref_table in exported_rows:
                continue

            # Get the referenced value from the row
            column, column_index = table.get_column_by_name(fk_column)
            ref_value = row[column_index]

            # Get the referenced row
            referenced_row_query = db.get_referenced_row_query(schema, ref_table, ref_column, limit)
            ref_row = db.execute_query(referenced_row_query, (ref_value,))

            if ref_row:
                # Export the referenced row
                export_table_to_csv(db, folder, schema, ref_table, limit, exported_rows, datatable_tables)


def export_schema_to_json(db, schema, folder, datatable_tables=None):
    """
    Export the database constraints (primary keys, foreign keys, unique indexes) to a JSON file.

    Args:
        db (DatabaseUtility): DatabaseUtility object.
        schema (str): Name of the database schema.
        folder (str): Path to the folder where the JSON file will be saved.
        datatable_tables (dict): Dictionary of DatabaseTable objects.

    Returns:
        None
    """

    try:
        # Serialize the table objects to JSON
        json_data = {}
        for table_name, table_object in sorted(datatable_tables.items()):
            columns = []
            primary_keys = sorted(table_object.primary_keys)
            foreign_keys = []
            unique_columns = sorted(get_unique_columns(db, schema, table_name))

            # Convert columns to a structured format
            for column in table_object.columns:
                column_data = {
                    'name': column.column_name,
                    'type': column.column_type,
                    'nullable': column.is_nullable,
                    'unique': column.is_unique
                }
                if column.column_size:
                    column_data['size'] = column.column_size
                columns.append(column_data)

            # Convert foreign keys to a structured format
            for foreign_key in table_object.foreign_keys:
                foreign_key_data = {
                    'column': foreign_key.column_name,
                    'referenced_table': foreign_key.referenced_table,
                    'referenced_column': foreign_key.referenced_column,
                    'on_update_action': foreign_key.on_update_action,
                    'on_delete_action': foreign_key.on_delete_action
                }
                foreign_keys.append(foreign_key_data)

            json_data[table_name] = {
                'columns': columns,
                'primary_keys': primary_keys,
                'foreign_keys': foreign_keys,
                'unique_columns': unique_columns
            }

        # Construct the path to the JSON file
        json_path = os.path.join(folder, f'ddl_{schema}.json')

        # Write the JSON data to the file
        with open(json_path, 'w') as jsonfile:
            json.dump(json_data, jsonfile, indent=4)

        logging.info(f"Constraints exported to '{json_path}'")

    except Exception as e:
        logging.exception(f"An error occurred: {str(e)}")


def get_unique_columns(db, schema_name, table_name):
    """
    Get the names of the columns that have a unique index on them.

    Args:
        db (DatabaseUtility): DatabaseUtility object.
        schema_name (str): Name of the database schema.
        table_name (str): Name of the table.

    Returns:
        list: List of column names with unique indexes.
    """
    unique_columns = []

    # Get the column names with a unique index
    unique_index_query, query_parameters = db.get_unique_constraints_query(schema_name, table_name)
    result = db.execute_query(unique_index_query, query_parameters)

    if result:
        unique_columns = [row[0] for row in result]

    return unique_columns


@click.command()
@click.option('--db-type', type=common_library.CaseInsensitiveChoice(DatabaseUtility.SUPPORTED_DATABASES),
              prompt='Enter the database type')
@click.option('--host', prompt='Enter the host', help='Database host')
@click.option('--port', default=None, type=int, help='Enter the database port number')
@click.option('--database', prompt='Enter the database name', help='Database name')
@click.option('--username', prompt='Enter the username', help='Database username')
@click.option('--schema', default='public', help='Schema name')
@click.option('--folder', prompt='Enter the folder path', help='Folder path to save CSV files')
@click.option('--limit', default=None, type=int, help='Maximum number of records to export')
def export_tables_to_csv(db_type, host, port, database, username, schema, folder, limit):
    """
    Export all tables from a database to CSV files.
    Supports PostgreSQL, MySQL, and SQL Server databases.
    """

    password = common_library.get_password()

    db = None
    try:
        db = DatabaseUtility.create(db_type, schema)
        db.connect(host, port, database, username, password)

        # Get a list of all tables
        tables = db.execute_query(f"SELECT table_name FROM information_schema.tables WHERE table_schema='{schema}'")

        folder = os.path.abspath(folder)
        # Create the folder if it doesn't exist
        os.makedirs(folder, exist_ok=True)

        # Create a dictionary to store the DatabaseTable objects
        datatable_tables = {}

        # Populate the datatable_tables dictionary with DatabaseTable objects
        for table in tables:
            table_name = table[0]
            datatable_table = DatabaseTable.from_database(db, schema, table_name)
            datatable_tables[table_name] = datatable_table

        # Export each table to a CSV file
        for table_name in datatable_tables:
            export_table_to_csv(db, folder, schema, table_name, limit, datatable_tables=datatable_tables)

        export_schema_to_json(db, schema, folder, datatable_tables=datatable_tables)

        logging.info("Table export completed successfully.")

    except Exception as e:
        logging.exception(f"An error occurred: {str(e)}")

    finally:
        if db:
            db.close()


if __name__ == '__main__':
    export_tables_to_csv()
