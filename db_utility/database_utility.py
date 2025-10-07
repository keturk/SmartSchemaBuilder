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
from typing import Optional, List, Dict, Any, Union, Tuple
from queue import Queue
from pathlib import Path

from db_utility.database_table import DatabaseTable
from common.exceptions import (
    DatabaseConnectionError, 
    DatabaseQueryError, 
    DatabaseSchemaError,
    ConfigurationError,
    ValidationError,
    FatalError
)
from common.error_handler import handle_errors, retry_on_failure, safe_execute
from common.validators import InputValidator


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
    def __init__(self, schema_name: str):
        """Initialize the database utility.
        
        Args:
            schema_name: Name of the database schema
        """
        try:
            # Validate schema name
            if schema_name and not InputValidator.non_empty_string(schema_name):
                raise ValidationError("Schema name must be a non-empty string")
            
            self.schema_name = self.format_identifier(schema_name) if schema_name else None
            self.schema_prefix = f"{self.schema_name}." if self.schema_name is not None else ""
            self.connection = None
            self.cursor = None
            self.host = None
            self.port = None
            self.user = None
            self.password = None
            self.database = None
            
            logging.info(f"Database utility initialized for schema: {self.schema_name}")
            
        except Exception as e:
            logging.error(f"Failed to initialize database utility: {e}")
            raise ConfigurationError(f"Database utility initialization failed: {e}")

    @staticmethod
    @handle_errors
    def create(db_type: str, schema_name: str = '') -> Optional['DatabaseUtility']:
        """
        Factory method to create an instance of database utility based on the provided database type.

        Args:
            db_type: Type of database. Options are 'postgresql', 'mysql', 'sqlserver'.
            schema_name: Name of the schema.

        Returns:
            Instance of respective database utility or None if invalid db_type is provided.

        Raises:
            ValidationError: If invalid database type is provided.
            ConfigurationError: If database utility creation fails.
        """
        try:
            # Validate inputs
            if not InputValidator.non_empty_string(db_type):
                raise ValidationError("Database type must be a non-empty string")
            
            if not InputValidator.database_type(db_type):
                raise ValidationError(f"Unsupported database type: {db_type}. Supported types: {DatabaseUtility.SUPPORTED_DATABASES}")
            
            db_type = db_type.lower()
            logging.info(f"Creating database utility for type: {db_type}, schema: {schema_name}")
            
            if db_type == 'postgresql':
                from db_utility.postgresql_utility import PostgreSQLUtility
                return PostgreSQLUtility(schema_name)
            elif db_type == 'mysql':
                from db_utility.mysql_utility import MySQLUtility
                return MySQLUtility(schema_name)
            elif db_type == 'sqlserver':
                from db_utility.sqlserver_utility import SQLServerUtility
                return SQLServerUtility(schema_name)
            else:
                raise ValidationError(f"Invalid database type: {db_type}")
                
        except ImportError as e:
            logging.error(f"Failed to import database utility for {db_type}: {e}")
            raise ConfigurationError(f"Database utility import failed for {db_type}: {e}")
        except Exception as e:
            logging.error(f"Failed to create database utility: {e}")
            raise ConfigurationError(f"Database utility creation failed: {e}")

    @handle_errors
    def connect(self, host: str, port: int, database: str, username: str, password: str) -> Tuple[Any, Any]:
        """
        Connects to the specified database.

        Args:
            host: Host of the database.
            port: Port number of the database.
            database: Database name.
            username: Username for authentication.
            password: Password for authentication.

        Returns:
            Tuple of (connection, cursor) objects.

        Raises:
            ValidationError: If invalid connection parameters are provided.
            DatabaseConnectionError: If connection fails.
        """
        try:
            # Validate connection parameters
            if not InputValidator.non_empty_string(host):
                raise ValidationError("Host must be a non-empty string")
            
            if not InputValidator.port_number(port):
                raise ValidationError(f"Port must be a valid port number (1-65535), got: {port}")
            
            if not InputValidator.non_empty_string(database):
                raise ValidationError("Database name must be a non-empty string")
            
            if not InputValidator.non_empty_string(username):
                raise ValidationError("Username must be a non-empty string")
            
            # Store connection parameters
            self.host = host
            self.port = port
            self.database = database
            self.user = username
            self.password = password
            
            logging.info(f"Attempting to connect to {self.get_database_type()} database: {host}:{port}/{database}")
            
            # This method should be implemented by subclasses
            raise NotImplementedError("Subclasses must implement connect() method")
            
        except ValidationError:
            raise
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise DatabaseConnectionError(
                f"Failed to connect to database: {e}",
                host=host,
                port=port,
                database=database
            )

    @handle_errors
    def commit(self) -> None:
        """
        Commits the current transaction.

        Raises:
            DatabaseConnectionError: If the connection is not established.
            DatabaseQueryError: If an error occurs during the commit.
        """
        try:
            if self.connection is None:
                raise DatabaseConnectionError("Connection is not established. Call connect() method first.")

            self.connection.commit()
            logging.debug("Transaction committed successfully")

        except DatabaseConnectionError:
            raise
        except Exception as e:
            logging.error(f"Failed to commit transaction: {e}")
            raise DatabaseQueryError(f"Transaction commit failed: {e}")

    @handle_errors
    def rollback(self) -> None:
        """
        Rolls back the current transaction.

        Raises:
            DatabaseConnectionError: If the connection is not established.
            DatabaseQueryError: If an error occurs during the rollback.
        """
        try:
            if self.connection is None:
                raise DatabaseConnectionError("Connection is not established. Call connect() method first.")

            self.connection.rollback()
            logging.debug("Transaction rolled back successfully")

        except DatabaseConnectionError:
            raise
        except Exception as e:
            logging.error(f"Failed to rollback transaction: {e}")
            raise DatabaseQueryError(f"Transaction rollback failed: {e}")

    @handle_errors
    def close(self) -> None:
        """
        Closes the cursor and connection to the database.

        Raises:
            DatabaseConnectionError: If the connection or cursor is not established.
        """
        try:
            if self.cursor is None and self.connection is None:
                logging.warning("No active connection to close")
                return

            if self.cursor is None:
                raise DatabaseConnectionError("Cursor is not established. Call connect() method first.")
            if self.connection is None:
                raise DatabaseConnectionError("Connection is not established. Call connect() method first.")

            # Dump any unread results before closing
            self.dump_unread_results()

            # Close cursor and connection
            self.cursor.close()
            self.connection.close()

            # Reset connection variables
            self.cursor = None
            self.connection = None
            
            logging.info("Database connection closed successfully")

        except DatabaseConnectionError:
            raise
        except Exception as e:
            logging.error(f"Failed to close database connection: {e}")
            raise DatabaseConnectionError(f"Connection close failed: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with automatic cleanup."""
        try:
            self.close()
        except Exception as e:
            logging.error(f"Error during context manager cleanup: {e}")
        return False  # Don't suppress exceptions

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
    @handle_errors
    @retry_on_failure(max_retries=3)
    def execute_query(self, query: str, parameters: Optional[Tuple] = None, fetch_all: bool = True) -> Union[List[Tuple], Tuple, List]:
        """
        Executes the provided SQL query using the cursor.

        Args:
            query: The SQL query to execute.
            parameters: Optional parameter values for the query.
            fetch_all: Flag indicating whether to fetch all rows (default: True).

        Returns:
            The result of the query. If fetch_all is True, returns a list of tuples representing
            the rows. If fetch_all is False, returns a single tuple representing the first row.
            If no rows are fetched, returns an empty list.

        Raises:
            DatabaseConnectionError: If the connection or cursor is not established.
            DatabaseQueryError: If an error occurs during query execution.
            ValidationError: If the query is invalid.
        """
        try:
            # Validate inputs
            if not InputValidator.non_empty_string(query):
                raise ValidationError("Query must be a non-empty string")
            
            if self.cursor is None:
                raise DatabaseConnectionError("Cursor is not established. Call connect() method first.")

            logging.debug(f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
            
            # Execute query with or without parameters
            if parameters is not None:
                self.cursor.execute(query, parameters)
            else:
                self.cursor.execute(query)

            # Fetch results based on fetch_all flag
            if fetch_all:
                result = self.cursor.fetchall()
                logging.debug(f"Query returned {len(result)} rows")
                return result
            else:
                result = self.cursor.fetchone()
                return result if result is not None else []

        except DatabaseConnectionError:
            raise
        except ValidationError:
            raise
        except Exception as e:
            logging.error(f"Query execution failed: {e}")
            raise DatabaseQueryError(
                f"Failed to execute query: {e}",
                query=query
            )

    @handle_errors
    def get_rows(self, table_name: str, limit: Optional[int] = None) -> List[Tuple]:
        """
        Get all rows from a table.

        Args:
            table_name: The name of the table.
            limit: The maximum number of rows to retrieve.

        Returns:
            The rows retrieved from the table.

        Raises:
            ValidationError: If table_name is invalid.
            DatabaseQueryError: If query execution fails.
        """
        try:
            # Validate inputs
            if not InputValidator.non_empty_string(table_name):
                raise ValidationError("Table name must be a non-empty string")
            
            if limit is not None and not InputValidator.positive_integer(limit):
                raise ValidationError(f"Limit must be a positive integer, got: {limit}")
            
            query = self.get_select_query(table_name, limit=limit)
            return self.execute_query(query)
            
        except ValidationError:
            raise
        except Exception as e:
            logging.error(f"Failed to get rows from table {table_name}: {e}")
            raise DatabaseQueryError(f"Failed to retrieve rows from table {table_name}: {e}")

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

    @handle_errors
    def generate_ddl(self, folder: Union[str, Path], database_tables: Dict[str, DatabaseTable]) -> str:
        """
        Generates a DDL script for the given database tables.

        Args:
            folder: The output directory for the DDL script.
            database_tables: The database tables to generate the DDL script for.

        Returns:
            The path to the generated DDL file.

        Raises:
            ValidationError: If inputs are invalid.
            DatabaseSchemaError: If DDL generation fails.
            FileProcessingError: If file operations fail.
        """
        try:
            # Validate inputs
            if not InputValidator.directory_path(folder):
                raise ValidationError(f"Invalid output directory: {folder}")
            
            if not database_tables:
                raise ValidationError("No database tables provided for DDL generation")
            
            # Convert to Path object for better handling
            folder_path = Path(folder)
            
            logging.info(f"Generating DDL for {len(database_tables)} tables")
            
            # Generate the order of table creation based on foreign key dependencies
            ordered_tables = self.generate_table_order(database_tables)
            logging.debug(f"Table creation order: {ordered_tables}")

            # Start building DDL
            ddl = self.generate_create_schema()
            
            for table_name in ordered_tables:
                database_table = DatabaseTable.get_table_by_name(database_tables, table_name)
                if database_table is None:
                    logging.warning(f"Table {table_name} not found in database_tables")
                    continue
                
                formatted_table_name = self.format_identifier(database_table.table_name)
                logging.debug(f"Processing table: {formatted_table_name}")

                # Build table DDL
                ddl += self.create_table_begin(formatted_table_name)

                # Add columns
                for column in database_table.columns:
                    ddl += self.generate_table_column(column)

                # Add primary keys
                if database_table.has_primary_key():
                    formatted_primary_keys = [
                        self.format_identifier(primary_key) 
                        for primary_key in database_table.primary_keys
                    ]
                    ddl += self.create_primary_keys(formatted_table_name, ','.join(formatted_primary_keys))

                # Add foreign keys
                if database_table.has_foreign_keys():
                    foreign_keys = database_table.foreign_keys
                    ddl += self.create_foreign_keys(formatted_table_name, foreign_keys)

                # Add unique indexes
                if database_table.has_unique_indexes():
                    for index_columns in database_table.unique_indexes:
                        ddl += self.generate_unique_index(formatted_table_name, index_columns)

                # Clean up trailing comma and add table end
                ddl = ddl.rstrip(",\n")
                ddl += self.create_table_end()
                ddl += "\n\n"

            logging.info("DDL generation complete.")

            # Create the output directory structure
            output_dir = folder_path / self.get_database_type()
            output_dir.mkdir(parents=True, exist_ok=True)

            # Write the DDL to a file
            ddl_file_path = output_dir / f"ddl_{self.get_database_type()}.sql"
            
            with open(ddl_file_path, 'w', encoding='utf-8') as ddl_file:
                ddl_file.write(ddl)

            logging.info(f"DDL generated and written to: {ddl_file_path}")
            return str(ddl_file_path)

        except ValidationError:
            raise
        except Exception as e:
            logging.error(f"DDL generation failed: {e}")
            raise DatabaseSchemaError(f"Failed to generate DDL: {e}")

    # Data insertion methods
    @handle_errors
    def generate_insert_statements(self, folder: Union[str, Path], database_tables: Dict[str, DatabaseTable]) -> List[str]:
        """
        Generates SQL insert statements for the given database tables.

        Args:
            folder: The output directory for the SQL insert statements.
            database_tables: The database tables to generate the SQL insert statements for.

        Returns:
            List of paths to generated insert statement files.

        Raises:
            ValidationError: If inputs are invalid.
            DatabaseSchemaError: If insert statement generation fails.
            FileProcessingError: If file operations fail.
        """
        try:
            # Validate inputs
            if not InputValidator.directory_path(folder):
                raise ValidationError(f"Invalid output directory: {folder}")
            
            if not database_tables:
                raise ValidationError("No database tables provided for insert statement generation")
            
            # Convert to Path object for better handling
            folder_path = Path(folder)
            output_dir = folder_path / self.get_database_type()
            output_dir.mkdir(parents=True, exist_ok=True)
            
            generated_files = []
            
            for database_table in database_tables.values():
                formatted_table_name = self.format_identifier(database_table.table_name)
                logging.info(f"Processing table: {formatted_table_name}")
                
                if database_table.dataframe is None or database_table.dataframe.empty:
                    logging.warning(f"Table {formatted_table_name} has no data to insert")
                    continue
                
                dataframe = database_table.dataframe
                columns = dataframe.columns.tolist()
                
                # Generate insert statements
                insert_values = []
                for index, row in dataframe.iterrows():
                    values = []
                    for col in columns:
                        value = row[col]
                        if pandas.isna(value) or value is pandas.NA:
                            values.append("NULL")
                        else:
                            # Escape single quotes in string values
                            str_value = str(value).replace("'", "''")
                            values.append(f"'{str_value}'")
                    
                    insert_values.append(f"({', '.join(values)})")

                # Create insert statement
                insert_statement = "INSERT INTO {} ({}) VALUES\n{};\n".format(
                    f'{self.schema_prefix}{formatted_table_name}',
                    ', '.join(columns),
                    ',\n'.join(insert_values)
                )

                # Write to file
                insert_file_path = output_dir / f"insert_{formatted_table_name}.sql"
                with open(insert_file_path, 'w', encoding='utf-8') as f:
                    f.write(insert_statement)
                
                generated_files.append(str(insert_file_path))
                logging.info(f"Generated insert statements for table: {formatted_table_name} ({len(insert_values)} rows)")

            logging.info(f"Finished generating insert statements for {len(generated_files)} tables")
            return generated_files

        except ValidationError:
            raise
        except Exception as e:
            logging.error(f"Insert statement generation failed: {e}")
            raise DatabaseSchemaError(f"Failed to generate insert statements: {e}")
