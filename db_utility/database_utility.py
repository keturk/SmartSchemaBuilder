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
import logging
import pandas

from queue import Queue
from db_utility.database_table import DatabaseTable


class DatabaseUtility:
    """
    Base class for Database Utility

    This class is intended to be subclassed by utility classes specific to a database.
    Each subclass must implement the abstract methods defined here.
    """

    # Supported databases
    SUPPORTED_DATABASES = ['postgresql', 'mysql', 'sqlserver']

    # Default ports for each database type
    DEFAULT_PORTS = {
        'postgresql': 5432,
        'mysql': 3306,
        'sqlserver': 1433
    }

    # Constructor and connection methods
    def __init__(self, schema_name):
        self.schema_name = self.format_identifier(schema_name)
        self.schema_prefix = f"{self.schema_name}." if self.schema_name is not None else ""
        self.connection = None
        self.cursor = None
        self.host = None
        self.port = None
        self.user = None
        self.password = None
        self.database = None

    @staticmethod
    def create(db_type: str, schema_name: str = ''):
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
            from db_utility.postgresql_utility import PostgreSQLUtility
            return PostgreSQLUtility(schema_name)
        elif db_type == 'mysql':
            # Importing here to avoid circular import
            from db_utility.mysql_utility import MySQLUtility
            return MySQLUtility(schema_name)
        elif db_type == 'sqlserver':
            # Importing here to avoid circular import
            from db_utility.sqlserver_utility import SQLServerUtility
            return SQLServerUtility(schema_name)
        else:
            logging.error("Error: Invalid target database selected.")
            return None

    def connect(self, host, port, database, username, password):
        """
        Connects to the specified database.

        Args:
            host (str): Host of the database.
            port (int): Port number of the database.
            database (str): Database name.
            username (str): Username for authentication.
            password (str): Password for authentication.

        Returns:
            Connection object: The connection to the database.

        Raises:
            ValueError: If an invalid database type is provided.
            Exception: If an error occurs while connecting to the database.
        """
        return NotImplementedError("Subclasses must implement connect() method")

    def commit(self):
        """
        Commits the current transaction.

        Returns:
            None

        Raises:
            Exception: If the connection is not established or an error occurs during the commit.
        """
        try:
            if self.connection is None:
                raise Exception("Connection is not established. Call connect() method first.")

            self.connection.commit()

        except Exception as e:
            logging.exception(f"An error occurred while committing the transaction: {e}")

    def rollback(self):
        """
        Rolls back the current transaction.

        Returns:
            None

        Raises:
            Exception: If the connection is not established or an error occurs during the rollback.
        """
        try:
            if self.connection is None:
                raise Exception("Connection is not established. Call connect() method first.")

            self.connection.rollback()

        except Exception as e:
            logging.exception(f"An error occurred while rolling back the transaction: {e}")

    def close(self):
        """
        Closes the cursor and connection to the database.

        Returns:
            None

        Raises:
            Exception: If the connection or cursor is not established.
        """
        try:
            if self.cursor is None:
                raise Exception("Cursor is not established. Call connect() method first.")
            if self.connection is None:
                raise Exception("Connection is not established. Call connect() method first.")

            self.dump_unread_results()

            self.cursor.close()
            self.connection.close()

            # Set the cursor and connection variables to None after closing
            self.cursor = None
            self.connection = None

        except Exception as e:
            logging.exception(f"An error occurred while closing the connection: {e}")

    def dump_unread_results(self):
        """
        Dumps and logs any unread result sets in the cursor.

        Returns:
            None
        """
        return NotImplementedError("Subclasses must implement dump_unread_results() method")

    def execute_sql_statements(self, sql_statements):
        """
        Execute SQL statements based on the database platform.

        Args:
            sql_statements (str): SQL statements to execute.

        Returns:
            str: The SQL statements executed.

        """
        return NotImplementedError("Subclasses must implement execute_sql_statements() method")

    # Query generation methods
    def get_select_query(self, table_name, columns=None, where=None, limit=None):
        """
        Get the SQL query to retrieve rows from a table in PostgreSQL.

        Args:
            table_name (str): The name of the table.
            columns (list[str], optional): The columns to retrieve. Defaults to None, which retrieves all columns.
            where (str, optional): The WHERE clause condition. Defaults to None.
            limit (int, optional): The maximum number of rows to retrieve. Defaults to None.

        Returns:
            str: The generated SQL query.
        """
        # Start building the SELECT query
        query = "SELECT"

        # Add the column names or * for all columns
        if columns is None:
            query += " *"
        else:
            query += " " + ", ".join(columns)

        # Add the table name
        query += " FROM " + table_name

        # Add the WHERE clause if provided
        if where is not None:
            query += " WHERE " + where

        # Add the LIMIT clause if provided
        if limit is not None:
            query += " LIMIT " + str(limit)

        return query

    # Query execution methods
    def execute_query(self, query, parameters=None, fetch_all=True):
        """
        Executes the provided SQL query using the cursor.

        Args:
            query (str): The SQL query to execute.
            parameters (tuple): Optional parameter values for the query (default: None).
            fetch_all (bool): Flag indicating whether to fetch all rows (default: True).

        Returns:
            list or tuple: The result of the query. If fetch_all is True, returns a list of tuples representing
                           the rows. If fetch_all is False, returns a single tuple representing the first row.
                           If no rows are fetched, returns an empty list.

        Raises:
            Exception: If the connection or cursor is not established, or an error occurs during query execution.
        """
        try:
            if self.cursor is None:
                raise Exception("Cursor is not established. Call connect() method first.")

            if parameters is not None:
                self.cursor.execute(query, parameters)
            else:
                self.cursor.execute(query)

            if fetch_all:
                return self.cursor.fetchall()
            else:
                return self.cursor.fetchone() or []

        except Exception as e:
            logging.exception(f"Failed to execute query:\n{query}\n\nError: {e}")
            raise

    def get_rows(self, table_name, limit=None):
        """
        Get all rows from a table.

        Args:
            table_name (str): The name of the table.
            limit (int, optional): The maximum number of rows to retrieve. Defaults to None.

        Returns:
            list[tuple]: The rows retrieved from the table.
        """
        query = self.get_select_query(table_name, limit=limit)
        return self.execute_query(query)

    def get_column_query(self):
        """
        Get the SQL query to retrieve column information from the database.

        Returns:
            str: The SQL query to retrieve column information.
        """
        return NotImplementedError("Subclasses must implement get_column_query() method")

    def get_primary_keys_query(self, schema_name, table_name):
        """
        Get the SQL query to retrieve primary key information from the database.

        Returns:
            str: The SQL query to retrieve primary key information.
        """
        return NotImplementedError("Subclasses must implement get_primary_keys_query() method")

    def get_foreign_key_query(self):
        """
        Get the SQL query to retrieve foreign key information from the database.

        Returns:
            str: The SQL query to retrieve foreign key information.
        """
        return NotImplementedError("Subclasses must implement get_foreign_key_query() method")

    def get_unique_constraints_query(self, schema_name, table_name):
        """
        Get the SQL query to retrieve unique constraint information from the database.

        Args:
            schema_name (str): Name of the schema.
            table_name (str): Name of the table.

        Returns:
            str: The SQL query to retrieve unique constraint information.
        """
        return NotImplementedError("Subclasses must implement get_unique_constraint_query() method")

    def get_referenced_row_query(self, schema, ref_table, ref_column, limit=None):
        """
        Generate a query to retrieve rows from a referenced table based on a specific column value.
        This implementation is specific to PostgreSQL.

        Args:
            schema (str): The schema of the referenced table.
            ref_table (str): The name of the referenced table.
            ref_column (str): The name of the column used for filtering.
            limit (int, optional): The maximum number of rows to retrieve. Defaults to None.

        Returns:
            str: The generated query to retrieve the rows.
        """
        if limit is not None:
            limit_clause = f"LIMIT {limit}"
        else:
            limit_clause = ""

        query = \
            f"SELECT * FROM {self.format_identifier(schema)}.{self.format_identifier(ref_table)} " \
            f"WHERE {self.format_identifier(ref_column)} = %s {limit_clause}"
        return query

    # DDL generation methods
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
                database_table = DatabaseTable.get_table_by_name(database_tables, table_name)
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
            logging.exception(f"Error generating DDL script: {str(e)}")

    # Data insertion methods
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
            logging.exception(f"Error generating insert statements: {str(e)}")
