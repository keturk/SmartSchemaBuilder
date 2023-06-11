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
import pyodbc
import logging
import re

import common.library as lib
from db_utility.database_utility import DatabaseUtility


# noinspection SqlNoDataSourceInspection
class SQLServerUtility(DatabaseUtility):
    """
    A SQL Server specific implementation of the Database class.
    """

    # Constructor and connection methods
    def __init__(self, schema_name):
        super().__init__(schema_name)

        # Initialize mapping from pandas data types to SQL Server data types
        # This mapping will be used to correctly generate SQL DDL statements based on pandas dataframes
        self.type_mapping = {
            'int64': 'BIGINT',
            'float64': 'FLOAT',
            'object': 'NVARCHAR(MAX)',
            'bool': 'BIT',
            'datetime64': 'DATETIME2',
            'timedelta64': 'VARCHAR(100)',
            'int32': 'INT',
            'float32': 'REAL',
            'int16': 'SMALLINT',
            'int8': 'TINYINT',
            'uint8': 'TINYINT',
            'category': 'NVARCHAR(MAX)',
            'uint32': 'BIGINT',
            'uint64': 'DECIMAL',
            'datetime64[ns]': 'DATETIME2',
            'datetime64[ns, UTC]': 'DATETIMEOFFSET',
            'timedelta64[ns]': 'VARCHAR(100)',
            'date': 'DATE',
            'time': 'TIME',
            'timetz': 'TIME',
            'time[tz]': 'TIME',
            'bytes': 'VARBINARY(MAX)',
            'str': 'NVARCHAR(MAX)',
            'none': 'NVARCHAR(MAX)',
            'decimal': 'DECIMAL',
            'uuid': 'UNIQUEIDENTIFIER',
            'list': 'NVARCHAR(MAX)',
            'dict': 'NVARCHAR(MAX)',
            'tuple': 'NVARCHAR(MAX)',
            'set': 'NVARCHAR(MAX)',
            'ipv4address': 'VARCHAR(15)',
            'ipv6address': 'VARCHAR(45)',
            'string': 'NVARCHAR(255)',
        }

        logging.info("SQL Server utility created successfully.")

    def connect(self, host, port, database, username, password):
        """
        Connect to the MySQL database.

        Args:
            host (str): Hostname or IP address of the database server.
            port (int): Port number of the database server.
            database (str): Name of the database.
            username (str): Username for authentication.
            password (str): Password for authentication.

        Returns:
            tuple: Database connection and cursor objects.

        """
        # Set the default port if not provided
        if not port:
            port = DatabaseUtility.DEFAULT_PORTS['sqlserver']

        logging.info(f"Connecting to SQL Server database '{database}' on host '{host}' and port '{port}'.")
        self.connection = pyodbc.connect(
            f'DRIVER={{SQL Server}};SERVER={host},{port};DATABASE={database};UID={username};PWD={password}')
        self.cursor = self.connection.cursor()
        self.host = host
        self.port = port
        self.user = username
        self.password = password
        self.database = database

    # Query execution methods
    def execute_sql_statements(self, sql_statements):
        """
        Execute SQL statements using the provided database connection and cursor.

        Args:
            sql_statements (str): SQL statements to execute.

        Returns:
            str: The SQL statements executed.

        """
        # Execute the SQL statements
        self.execute_query(sql_statements)
        self.commit()

        # Return the executed SQL statements
        return sql_statements

    # Query generation methods
    def get_select_query(self, table_name, columns=None, where=None, limit=None):
        """
        Get the SQL query to retrieve rows from a table in SQL Server.

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

        # Add the TOP clause if limit is provided
        if limit is not None:
            query = query.replace("SELECT", "SELECT TOP " + str(limit))

        return query

    def get_column_query(self):
        """
        Get the SQL query to retrieve column information from the database.

        Returns:
            str: The SQL query to retrieve column information.
        """
        return (
            "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = ?"
        )

    def get_primary_keys_query(self, schema_name, table_name):
        """
        Get the primary keys of a table in SQL Server.

        Args:
            schema_name (str): The name of the schema.
            table_name (str): The name of the table.

        Returns:
            list: The list of primary key column names.
        """
        query = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
                AND TABLE_SCHEMA = ?
                AND TABLE_NAME = ?;
        """
        return query, (schema_name, table_name)

    def get_foreign_key_query(self):
        """
        Get the SQL query to retrieve foreign key information from the database.

        Returns:
            str: The SQL query to retrieve foreign key information.
        """
        return ("""
            SELECT
              kcu.COLUMN_NAME,
              ccu.TABLE_NAME AS referenced_table_name,
              ccu.COLUMN_NAME AS referenced_column_name,
              rc.CONSTRAINT_NAME,
              rc.UPDATE_RULE,
              rc.DELETE_RULE
            FROM
              INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
              JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
              JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
                ON rc.UNIQUE_CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
            WHERE
              kcu.TABLE_SCHEMA = ? AND
              kcu.TABLE_NAME = ?;
        """)

    def get_referenced_row_query(self, schema, ref_table, ref_column, limit=None):
        """
        Generate a query to retrieve rows from a referenced table based on a specific column value.
        This implementation is specific to SQL Server.

        Args:
            schema (str): The schema of the referenced table.
            ref_table (str): The name of the referenced table.
            ref_column (str): The name of the column used for filtering.
            limit (int, optional): The maximum number of rows to retrieve. Defaults to None.

        Returns:
            str: The generated query to retrieve the rows.
        """
        if limit is not None:
            limit_clause = f"TOP {limit} "
        else:
            limit_clause = ""

        query = \
            f"SELECT {limit_clause}* FROM {self.format_identifier(schema)}.{self.format_identifier(ref_table)} " \
            f"WHERE {self.format_identifier(ref_column)} = ?"
        return query

    def get_unique_constraints_query(self, schema_name, table_name):
        """
        Returns a list of unique constraints for the given table.

        Args:
            schema_name (str): Name of the schema.
            table_name (str): Name of the table.

        Returns:
            list: List of unique constraints.
        """
        query = """
            SELECT
                ccu.column_name AS column_name
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.constraint_column_usage AS ccu
                    ON tc.constraint_name = ccu.constraint_name
            WHERE
                tc.constraint_type = 'UNIQUE'
                AND tc.table_schema = ?
                AND tc.table_name = ?
        """
        return query, (schema_name, table_name)

    # DDL generation methods
    def map_data_type(self, dataframe_type):
        """
        Map pandas dataframe types to SQL Server data types.

        Args:
            dataframe_type (str): Pandas dataframe type.

        Returns:
            str: Corresponding SQL Server data type.
        """
        logging.debug(f"Mapping pandas data type '{dataframe_type}' to SQL Server data type.")
        return self.type_mapping.get(dataframe_type.lower(), 'TEXT')

    def get_database_type(self):
        """
        Get the type of the target database.

        Returns:
            str: Database type.
        """
        logging.debug("Get database type: SQL Server.")
        return "sqlserver"

    def format_identifier(self, identifier):
        """
        Format SQL identifier (table or column name) by removing special characters and delimiting words with
        underscores.

        Args:
            identifier (str): Identifier to be formatted.

        Returns:
            str: Formatted identifier.
        """
        if identifier is None:
            return None

        logging.debug(f"Formatting identifier '{identifier}'.")

        # Remove any characters that are not alphanumeric or underscore
        formatted_identifier = re.sub(r'[^a-zA-Z0-9_]', '', identifier)

        logging.debug(f"Formatted identifier is '{formatted_identifier}'.")
        return formatted_identifier

    def generate_unique_index(self, formatted_table_name, index_column):
        """
        Generate SQL statement for creating a unique index on specified columns of a table.

        Args:
            formatted_table_name (str): Table name.
            index_column (list): List of columns to create the unique index on.

        Returns:
            str: SQL statement for creating a unique index.
        """
        formatted_index_column = self.format_identifier(index_column.column_name)
        logging.debug(f"Generating unique index for table '{self.schema_prefix}{formatted_table_name}' and columns "
                      f"{formatted_index_column}.")
        constraint_name = lib.truncate(f"idx_{formatted_table_name}_{formatted_index_column}", 128)
        return f"\tCONSTRAINT {constraint_name} UNIQUE ({formatted_index_column}),\n"

    def generate_create_schema(self):
        """
        Method to generate the SQL statement for creating a schema.

        Returns:
            str: The SQL statement for creating the schema.

        Raises:
            NotImplementedError: If this method is not overridden by a subclass.
        """
        return f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{self.schema_name}')\n" \
               f"BEGIN\n" \
               f"   EXEC('CREATE SCHEMA {self.schema_name}')\n" \
               f"END;\n\n" if self.schema_name else ""

    def create_table_begin(self, formatted_table_name):
        """
        Generate the beginning part of the SQL statement for creating a table.

        Args:
            formatted_table_name (str): Formatted table name.

        Returns:
            str: Beginning part of the SQL statement for creating a table.
        """
        logging.debug(f"Creating table begin statement for table '{self.schema_prefix}{formatted_table_name}'.")
        return f'CREATE TABLE {self.schema_prefix}{formatted_table_name} (\n'

    def create_table_end(self):
        """
        Generate the ending part of the SQL statement for creating a table.

        Returns:
            str: Ending part of the SQL statement for creating a table.
        """
        logging.debug("Creating table end statement.")
        return f'\n);\n'

    def create_primary_keys(self, formatted_table_name, formatted_primary_keys):
        """
        Generate the SQL statement for defining primary keys in a table.

        Args:
            formatted_table_name (str): Formatted table name.
            formatted_primary_keys (list): List of formatted primary key column names.

        Returns:
            str: SQL statement for defining primary keys.
        """
        logging.debug(f"Creating primary keys for table '{self.schema_prefix}{formatted_table_name}' "
                      f"and keys {formatted_primary_keys}.")
        if type(formatted_primary_keys) == list:
            return f"\tCONSTRAINT PK_{formatted_table_name} " \
                   f"PRIMARY KEY ({', '.join(formatted_primary_keys)}),\n"
        else:
            return f"\tCONSTRAINT PK_{formatted_table_name} " \
                   f"PRIMARY KEY ({formatted_primary_keys}),\n"

    def generate_table_column(self, column):
        """
        Generate a table column definition.

        Args:
            column (Column): Column object representing the column information.

        Returns:
            str: Generated table column definition.
        """

        formatted_column_name = self.format_identifier(column.column_name)
        nullability = "NOT NULL" if not column.is_nullable else ""

        return f"\t{formatted_column_name} {column.column_type} {nullability},\n"

    def create_foreign_keys(self, formatted_table_name, foreign_keys):
        """
        Generate the SQL statement for creating foreign keys in a table using ALTER TABLE syntax in SQL Server.

        Args:
            formatted_table_name (str): Formatted table name.
            foreign_keys (list): List of foreign key constraints.

        Returns:
            str: SQL statement for creating foreign keys.
        """
        logging.debug(f"Creating foreign keys for table '{formatted_table_name}'.")
        foreign_key_statements = []
        for foreign_key in foreign_keys:
            referenced_table = self.format_identifier(foreign_key.referenced_table)
            referenced_column = self.format_identifier(foreign_key.referenced_column)
            referencing_column = self.format_identifier(foreign_key.column_name)

            constraint_name = lib.truncate(
                f"fk_{formatted_table_name}_{referencing_column}_{referenced_table}_{referenced_column}", 128)
            statement = \
                f"\tCONSTRAINT {constraint_name} FOREIGN KEY ({referencing_column})\n" \
                f"\t\tREFERENCES {self.schema_prefix}{referenced_table} ({referenced_column}),\n"
            foreign_key_statements.append(statement)

        return '\n'.join(foreign_key_statements)
