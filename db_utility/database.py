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
import psycopg2
import logging
import mysql.connector
import pyodbc
import pandas

from queue import Queue
from db_utility.database_table import get_table_by_name

# Supported databases
SUPPORTED_DATABASES = ['postgresql', 'mysql', 'sqlserver']

# Default ports for each database type
DEFAULT_PORTS = {
    'postgres': 5432,
    'mysql': 3306,
    'sqlserver': 1433
}


def execute_sql_statements(db_type, conn, cursor, sql_statements):
    """
    Execute SQL statements based on the database platform.

    Args:
        db_type (str): Type of the database.
        conn: Database connection object.
        cursor: Database cursor object.
        sql_statements (str): SQL statements to execute.

    Returns:
        str: The SQL statements executed.

    """
    # Execute SQL statements based on the database platform
    if db_type == 'sqlserver':
        # SQL Server
        cursor.execute(sql_statements)
    elif db_type == 'mysql':
        # MySQL
        statements = sql_statements.split(';')
        for statement in statements:
            if statement.strip():
                cursor.execute(statement)
    elif db_type == 'postgresql':
        # PostgreSQL
        cursor.execute(sql_statements)
    else:
        logging.error("Unsupported database platform")
        conn.rollback()
        return ""

    # Commit the transaction
    conn.commit()
    return sql_statements


def connect_to_database(db_type, host, port, database, username, password):
    """
    Connects to the specified database.

    Args:
        port (int): Port number of the database.
        db_type (str): Type of the database.
        host (str): Host of the database.
        database (str): Database name.
        username (str): Username for authentication.
        password (str): Password for authentication.

    Returns:
        Connection object: The connection to the database.

    Raises:
        ValueError: If an invalid database type is provided.
        Exception: If an error occurs while connecting to the database.
    """
    # Set the default port if not provided
    if not port:
        port = DEFAULT_PORTS.get(db_type)

    if db_type == 'postgresql':
        logging.info('Connecting to PostgreSQL database...')
        conn = psycopg2.connect(host=host, port=port, database=database, user=username, password=password)
    elif db_type == 'mysql':
        logging.info('Connecting to MySQL database...')
        conn = mysql.connector.connect(host=host, port=port, database=database, user=username, password=password)
    elif db_type == 'sqlserver':
        logging.info('Connecting to SQL Server database...')
        conn = pyodbc.connect(
            f'DRIVER={{SQL Server}};SERVER={host},{port};DATABASE={database};UID={username};PWD={password}')
    else:
        raise ValueError(f"Invalid database type: {db_type}")
    return conn


def create(db_type: str, schema_name: str):
    """
    Factory method to create an instance of database utility based on the provided database type.

    Args:
        db_type (str): Type of database. Options are 'postgresql', 'mysql', 'sqlserver'.
        schema_name (str): Name of the schema.

    Returns:
        Instance of respective database utility or None if invalid db_type is provided.

    Raises:
        Exception: If an error occurs while creating the database utility.
    """
    db_type = db_type.lower()
    if db_type == 'postgresql':
        # Importing here to avoid circular import
        from db_utility.postgresql import PostgreSQLUtility
        return PostgreSQLUtility(schema_name)
    elif db_type == 'mysql':
        # Importing here to avoid circular import
        from db_utility.mysql import MySQLUtility
        return MySQLUtility(schema_name)
    elif db_type == 'sqlserver':
        # Importing here to avoid circular import
        from db_utility.sqlserver import SQLServerUtility
        return SQLServerUtility(schema_name)
    else:
        logging.error("Error: Invalid target database selected.")
        return None


class DatabaseUtilityBase:
    """
    Base class for Database Utility.

    This class is intended to be subclassed by utility classes specific to a database.
    Each subclass must implement the abstract methods defined here.
    """
    def __init__(self, schema_name):
        self.schema_name = self.format_identifier(schema_name)
        self.schema_prefix = f"{self.schema_name}." if self.schema_name is not None else ""

    def map_data_type(self, dataframe_type):
        """
        Method to map the data type of dataframe to a specific database column type.

        Args:
            dataframe_type: Data type of dataframe.

        Returns:
            str: The mapped database column type.

        Raises:
            NotImplementedError: If this method is not overridden by a subclass.
        """
        raise NotImplementedError("Subclasses must implement map_data_type() method")

    def get_database_type(self):
        """
        Method to get the type of database.

        Returns:
            str: The type of the database.

        Raises:
            NotImplementedError: If this method is not overridden by a subclass.
        """
        raise NotImplementedError("Subclasses must implement get_database_type() method")

    def format_identifier(self, identifier):
        """
        Method to format identifier.

        Args:
            identifier: Identifier to be formatted.

        Returns:
            str: The formatted identifier.

        Raises:
            NotImplementedError: If this method is not overridden by a subclass.
        """
        raise NotImplementedError("Subclasses must implement format_identifier() method")

    def generate_unique_index(self, formatted_table_name, index_column):
        """
        Method to generate unique index for a given table.

        Args:
            formatted_table_name (str): Name of the table for which to generate the unique index.
            index_column (str): Columns on which the index will be made.

        Returns:
            str: The SQL statement for creating the unique index.

        Raises:
            NotImplementedError: If this method is not overridden by a subclass.
        """
        raise NotImplementedError("Subclasses must implement generate_unique_index() method")

    def generate_create_schema(self):
        """
        Method to generate the SQL statement for creating a schema.

        Returns:
            str: The SQL statement for creating the schema.

        Raises:
            NotImplementedError: If this method is not overridden by a subclass.
        """
        raise NotImplementedError("Subclasses must implement generate_create_schema() method")

    def create_table_begin(self, formatted_table_name):
        """
        Method to start the creation of a table.

        Args:
            formatted_table_name (str): Formatted name of the table to be created.

        Returns:
            str: The SQL statement for beginning the creation of the table.

        Raises:
            NotImplementedError: If this method is not overridden by a subclass.
        """
        raise NotImplementedError("Subclasses must implement create_table_begin() method")

    def create_table_end(self):
        """
        Method to end the creation of a table.

        Returns:
            str: The SQL statement for ending the creation of the table.

        Raises:
            NotImplementedError: If this method is not overridden by a subclass.
        """
        raise NotImplementedError("Subclasses must implement create_table_end() method")

    def create_primary_keys(self, formatted_table_name, formatted_primary_keys):
        """
        Method to create primary keys for a table.

        Args:
            formatted_table_name (str): Formatted name of the table for which to create the primary key.
            formatted_primary_keys (str): Primary key(s) for the table.

        Returns:
            str: The SQL statement for creating the primary keys.

        Raises:
            NotImplementedError: If this method is not overridden by a subclass.
        """
        raise NotImplementedError("Subclasses must implement create_primary_keys() method")

    def generate_table_column(self, column):
        """
        Method to create primary keys for a table.

        Args:
            column (str): Name of the column.

        Returns:
            str: The SQL statement for creating a table column.

        Raises:
            NotImplementedError: If this method is not overridden by a subclass.
        """
        raise NotImplementedError("Subclasses must implement generate_table_column() method")

    def create_foreign_keys(self, formatted_table_name, foreign_keys):
        """
        Method to create foreign keys for a table.

        Args:
            formatted_table_name (str): Formatted name of the table for which to create the foreign keys.
            foreign_keys (list): List of DatabaseForeignKey objects representing the foreign keys.

        Returns:
            str: The SQL statements for creating the foreign keys.

        Raises:
            NotImplementedError: If this method is not overridden by a subclass.
        """
        raise NotImplementedError("Subclasses must implement create_foreign_keys() method")

    @staticmethod
    def generate_table_order(database_tables):
        """
        Generate the order of table creation based on foreign key dependencies.

        Args:
            database_tables (dict): The database tables.

        Returns:
            list: The ordered list of table names for creation.
        """
        graph = {}
        in_degree = {}

        # Initialize the graph and in-degree dictionaries
        for database_table in database_tables.values():
            table_name = database_table.table_name
            graph[table_name] = set()
            in_degree[table_name] = 0

        # Build the graph and in-degree dictionaries based on foreign key relationships
        for database_table in database_tables.values():
            table_name = database_table.table_name
            # Add the table to the graph if it is not already in it
            for foreign_key in database_table.foreign_keys:
                referenced_table = foreign_key.referenced_table
                graph[referenced_table].add(table_name)
                in_degree[table_name] += 1

        # Perform topological sorting to determine the order of table creation
        ordered_tables = []
        queue = Queue()

        # Add all tables with no in-degree to the queue
        for table_name in in_degree:
            # Add the table to the queue if it has no in-degree
            if in_degree[table_name] == 0:
                queue.put(table_name)

        # Perform topological sorting
        while not queue.empty():
            table_name = queue.get()
            ordered_tables.append(table_name)

            # Decrement the in-degree of all tables that reference the current table
            for referenced_table in graph[table_name]:
                in_degree[referenced_table] -= 1
                # Add the referenced table to the queue if it has no in-degree
                if in_degree[referenced_table] == 0:
                    queue.put(referenced_table)

        return ordered_tables

    def generate_ddl(self, folder, database_tables):
        """
        Generates a DDL script for the given database tables.

        Args:
            folder (str): The output directory for the DDL script.
            database_tables (dict): The database tables to generate the DDL script for.

        Raises:
            Exception: If an error occurs while generating the DDL script.
        """
        try:
            # Generate the order of table creation based on foreign key dependencies
            ordered_tables = self.generate_table_order(database_tables)

            ddl = self.generate_create_schema()
            for table_name in ordered_tables:
                database_table = get_table_by_name(database_tables, table_name)
                formatted_table_name = self.format_identifier(database_table.table_name)

                ddl += self.create_table_begin(formatted_table_name)

                for column in database_table.columns:
                    ddl += self.generate_table_column(column)

                if database_table.has_primary_key():
                    formatted_primary_keys = \
                        [self.format_identifier(primary_key) for primary_key in database_table.primary_keys]
                    ddl += self.create_primary_keys(formatted_table_name, ','.join(formatted_primary_keys))

                if database_table.has_foreign_keys():
                    foreign_keys = database_table.foreign_keys
                    ddl += self.create_foreign_keys(formatted_table_name, foreign_keys)

                if database_table.has_unique_indexes():
                    for index_columns in database_table.unique_indexes:
                        ddl += self.generate_unique_index(formatted_table_name, index_columns)

                ddl = ddl.rstrip(",\n")
                ddl += self.create_table_end()
                ddl += "\n\n"

            logging.info("DDL generation complete.")

            # Create the output directory if it doesn't exist
            os.makedirs(folder, exist_ok=True)
            os.makedirs(os.path.join(folder, self.get_database_type()), exist_ok=True)

            # Write the DDL to a file under the provided folder
            ddl_file_path = os.path.join(folder, self.get_database_type(), f"ddl_{self.get_database_type()}.sql")
            with open(ddl_file_path, 'w') as ddl_file:
                ddl_file.write(ddl)

            logging.info(f"DDL is generated and written to: {ddl_file_path}")

        except Exception as e:
            logging.error(f"Error generating DDL script: {str(e)}")
            raise

    def generate_insert_statements(self, folder, database_tables):
        """
        Generates SQL insert statements for the given database tables.

        Args:
            folder (str): The output directory for the SQL insert statements.
            database_tables (dict): The database tables to generate the SQL insert statements for.

        Raises:
            Exception: If an error occurs while generating the insert statements.
        """
        try:
            for database_table in database_tables.values():
                formatted_table_name = self.format_identifier(database_table.table_name)
                logging.info(f"Processing table: {formatted_table_name}")
                dataframe = database_table.dataframe

                columns = dataframe.columns.tolist()

                with open(os.path.join(folder, self.get_database_type(),
                                       f"insert_{formatted_table_name}.sql"), "w", encoding='utf-8') as f:
                    insert_values = []

                    for index, row in dataframe.iterrows():
                        values = [str(row[col]) if row[col] is not pandas.NA else "NULL" for col in columns]
                        formatted_values = [f"'{value}'" if value != "NULL" else "NULL" for value in values]

                        insert_values.append(f"({', '.join(formatted_values)})")

                    insert_statement = "INSERT INTO {} ({}) VALUES\n{};\n".format(
                        f'{self.schema_prefix}{formatted_table_name}',
                        ', '.join(columns),
                        ',\n'.join(insert_values)
                    )

                    f.write(insert_statement)
                    logging.info(f"Generated insert statements for table: {formatted_table_name}")

            logging.info("Finished generating insert statements.")

        except Exception as e:
            logging.error(f"Error generating insert statements: {str(e)}")
            raise
