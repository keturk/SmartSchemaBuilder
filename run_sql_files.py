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
import click
import logging
import sqlparse
import getpass

import common.library as common_library
import db_utility.database as database_utility

common_library.configure_logging('run_sql_files.log', logging.INFO)


def execute_ddl_file(db_type, conn, cursor, ddl_file, folder):
    """
    Execute the DDL statements from a file.

    Args:
        db_type (str): Database type.
        conn: Database connection object.
        cursor: Database cursor object.
        ddl_file (str): Name of the DDL file to execute.
        folder (str): Path to the folder where the DDL file is located.

    Returns:
        str: The DDL statements read from the file.
    """

    file_path = os.path.join(folder, ddl_file)
    with open(file_path, 'r') as f:
        ddl_statements = f.read()

    try:
        database_utility.execute_sql_statements(db_type, conn, cursor, ddl_statements)
        conn.commit()
        logging.info(f"Executed DDL file: {ddl_file}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to execute DDL file: {ddl_file}")
        logging.error(str(e))

    return ddl_statements


def extract_table_names(ddl_statements):
    """
    Extract table names from a collection of DDL statements.

    This method is used to find the table ordering, which determines the sequence
    in which insert files should be executed to avoid violating foreign key constraints.

    Args:
        ddl_statements (str): Collection of DDL statements as a single string.

    Returns:
        list[str]: List of table names extracted from the DDL statements.
    """

    # Parse the DDL statements using sqlparse
    parsed_statements = sqlparse.parse(ddl_statements)

    # Extract table names from the DDL statements
    table_names = [
        token.get_real_name()
        for statement in parsed_statements
        if statement.get_type() == "CREATE" and any(
            token.value.upper() in ["TABLE", "TABLE IF EXISTS"]
            for token in statement.tokens
            if isinstance(token, sqlparse.sql.Token) and token.ttype == sqlparse.tokens.Keyword
        )
        for token in statement.tokens
        if isinstance(token, sqlparse.sql.Identifier)
    ]

    return table_names


def execute_insert_file(conn, cursor, folder, table):
    """
    Execute the insert statements from a file for a specific table.

    Args:
        conn: Database connection object.
        cursor: Database cursor object.
        folder (str): Path to the folder where the insert file is located.
        table (str): Name of the table for which insert statements will be executed.

    Returns:
        None
    """

    insert_file_path = os.path.join(folder, f"insert_{table}.sql")
    try:
        with open(insert_file_path, 'r') as insert_file:
            insert_statements = insert_file.read().replace("'NULL'", 'NULL')

            # Execute the insert statements
            cursor.execute(insert_statements)
            # Commit the changes
            conn.commit()

            logging.info(f"Insert statements for table '{table}' executed successfully")
    except IOError as e:
        logging.error(f"Error reading insert file '{insert_file_path}': {str(e)}")
    except Exception as e:
        logging.error(f"Error executing insert statements for table '{table}': {str(e)}")
        conn.rollback()


@click.command()
@click.option('--db-type', type=common_library.CaseInsensitiveChoice(database_utility.SUPPORTED_DATABASES), prompt=True)
@click.option('--host', prompt='Enter the database host address')
@click.option('--port', default=None, type=int, help='Enter the database port number')
@click.option('--database', prompt='Enter the database name')
@click.option('--username', prompt='Enter the username')
@click.option('--folder', prompt='Enter the directory path containing the SQL files')
def run_sql_files(db_type, host, port, database, username, folder):
    """
    This function is the main entry point of the application.
    It runs SQL files in a given directory on a specified database.

    Args:
        db_type (str): The type of the database. It can be one of 'postgresql', 'mysql', or 'sqlserver'.
        host (str): The host address of the database.
        port (int, optional): The port number of the database. If not provided, the default port number for the
                              database type will be used.
        database (str): The name of the database.
        username (str): The username to connect to the database.
        folder (str): The directory path containing the SQL files.
    """

    password = common_library.get_str_from_env('DB_PASSWORD')
    # If the password is not set as an environment variable, prompt the user for it
    if not password:
        password = getpass.getpass('Enter the password: ')

    # Get the list of files in the directory
    files = os.listdir(folder)

    # Find SQL files for DDL and insert operations
    ddl_files = [file_name for file_name in files if file_name.startswith('ddl_') and file_name.endswith('.sql')]
    if not ddl_files:
        raise ValueError('No DDL files found.')

    conn = None
    cursor = None
    try:
        conn = database_utility.connect_to_database(db_type, host, port, database, username, password)
        cursor = conn.cursor()

        # Execute DDL files
        logging.info('Executing DDL files...')
        for ddl_file in ddl_files:
            # Execute the DDL statements from the file
            ddl_statements = execute_ddl_file(db_type, conn, cursor, ddl_file, folder)

            # Extract table names from the DDL statements
            table_names = extract_table_names(ddl_statements)

            logging.info('Executing INSERT files...')
            # Execute INSERT files in the order of table names
            for table in table_names:
                # Execute the insert statements for the table
                execute_insert_file(conn, cursor, folder, table)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        # Rollback any changes
        if conn:
            conn.rollback()

    finally:
        # Close the cursor and database connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    logging.info('Script execution completed.')


if __name__ == '__main__':
    run_sql_files()
