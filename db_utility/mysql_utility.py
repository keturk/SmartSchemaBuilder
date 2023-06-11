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
import mysql.connector
import logging
import re

import common.library as lib
from db_utility.database_utility import DatabaseUtility
from db_utility.database_column import DatabaseColumn


class MySQLUtility(DatabaseUtility):
    """
    A MySQL specific implementation of the Database class.
    """

    # Constructor and connection methods
    def __init__(self, schema_name):
        super().__init__(schema_name)

        # Initialize mapping from pandas data types to MySQL data types
        # This mapping will be used to correctly generate SQL DDL statements based on pandas dataframes
        self.type_mapping = {
            'int64': 'BIGINT',
            'float64': 'DOUBLE',
            'object': 'TEXT',
            'bool': 'BOOLEAN',
            'datetime64': 'DATETIME',
            'timedelta64': 'VARCHAR(100)',
            'int32': 'INT',
            'float32': 'FLOAT',
            'int16': 'SMALLINT',
            'int8': 'TINYINT',
            'uint8': 'TINYINT',
            'category': 'TEXT',
            'uint32': 'BIGINT',
            'uint64': 'DECIMAL',
            'datetime64[ns]': 'DATETIME',
            'datetime64[ns, utc]': 'DATETIME',
            'timedelta64[ns]': 'VARCHAR(100)',
            'date': 'DATE',
            'time': 'TIME',
            'timetz': 'TIME',
            'time[tz]': 'TIME',
            'bytes': 'LONGBLOB',
            'str': 'VARCHAR(255)',
            'none': 'TEXT',
            'decimal': 'DECIMAL',
            'uuid': 'CHAR(36)',
            'list': 'JSON',
            'dict': 'JSON',
            'tuple': 'JSON',
            'set': 'JSON',
            'ipv4address': 'VARCHAR(15)',
            'ipv6address': 'VARCHAR(45)',
            'string': 'VARCHAR(255)',
        }

        # Initialize the logger for logging purposes
        logging.info("MySQL utility created successfully.")

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
            port = DatabaseUtility.DEFAULT_PORTS['mysql']

        logging.info(f"Connecting to MySQL database '{database}' on host '{host}' and port '{port}'.")
        self.connection = mysql.connector.connect(
            host=host, port=port, database=database, user=username, password=password)
        self.cursor = self.connection.cursor()
        self.host = host
        self.port = port
        self.user = username
        self.password = password
        self.database = database

    def dump_unread_results(self):
        """
        Dumps and logs any unread result sets in the cursor.

        Returns:
            None
        """
        try:
            while True:
                if not self.cursor.nextset():
                    break
                unread_result = self.cursor.fetchall()  # Fetch and store all remaining rows from the unread result set
                for unread_row in unread_result:
                    logging.warning("Unread result: %s", unread_row)  # Log each unread result row

        except Exception as ex:
            logging.error(f"An error occurred while processing remaining result sets: {ex}")

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
        statements = sql_statements.split(';')
        for statement in statements:
            if statement.strip():
                self.execute_query(statement)

        # Commit the transaction
        self.commit()

        # Return the executed SQL statements
        return sql_statements

    # Query generation methods
    def get_column_query(self):
        """
        Get the SQL query to retrieve column information from the database.

        Returns:
            str: The SQL query to retrieve column information.
        """
        return (
            "SELECT column_name, data_type, character_maximum_length, is_nullable "
            "FROM information_schema.columns WHERE table_name = %s"
        )

    def get_primary_keys_query(self, schema_name, table_name):
        """
        Get the primary keys of a table in MySQL.

        Args:
            schema_name (str): The name of the schema.
            table_name (str): The name of the table.

        Returns:
            list: The list of primary key column names.
        """
        query = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
                AND TABLE_NAME = %s
                AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION;
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
              kcu.REFERENCED_TABLE_NAME,
              kcu.REFERENCED_COLUMN_NAME,
              rc.CONSTRAINT_NAME,
              rc.UPDATE_RULE,
              rc.DELETE_RULE
            FROM
              INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
              INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc ON
                kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
            WHERE
              kcu.TABLE_SCHEMA = %s AND
              kcu.TABLE_NAME = %s AND
              kcu.REFERENCED_TABLE_NAME IS NOT NULL;
        """)

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
                column_name
            FROM
                information_schema.key_column_usage
            WHERE
                constraint_schema = %s
                AND constraint_name IN (
                    SELECT constraint_name
                    FROM information_schema.table_constraints
                    WHERE table_schema = %s
                    AND table_name = %s
                    AND constraint_type = 'UNIQUE'
                )
                AND table_schema = %s
                AND table_name = %s
        """
        return query, (schema_name, schema_name, table_name, schema_name, table_name)

    # DDL generation methods
    def map_data_type(self, dataframe_type):
        """
        Map pandas dataframe types to MySQL data types.

        Args:
            dataframe_type (str): Pandas dataframe type.

        Returns:
            str: Corresponding MySQL data type.
        """
        logging.debug(f"Mapping pandas data type '{dataframe_type}' to MySQL data type.")
        return self.type_mapping.get(dataframe_type.lower(), 'TEXT')

    def get_database_type(self):
        """
        Get the type of the target database.

        Returns:
            str: Database type.
        """
        logging.debug("Get database type: MySQL.")
        return "mysql"

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

        # Remove any characters that are not alphanumeric or underscore
        formatted_identifier = re.sub(r'[^a-zA-Z0-9_]', '', identifier)

        logging.debug(f"Formatted identifier is '{formatted_identifier}'.")
        return formatted_identifier

    def generate_unique_index(self, formatted_table_name, index_column: DatabaseColumn):
        """
        Generate SQL statement for creating a unique index on specified columns of a table.

        Args:
            formatted_table_name (str): Table name.
            index_column (list): List of columns to create the unique index on.

        Returns:
            str: SQL statement for creating a unique index.
        """
        formatted_index_column = self.format_identifier(index_column.column_name)
        logging.debug(
            f"Generating unique index for table '{self.schema_prefix}{formatted_table_name}' and "
            f"columns {formatted_index_column}.")

        index_name = lib.truncate(f"idx_{formatted_table_name}_{formatted_index_column}", 64)
        return f"\tUNIQUE INDEX {index_name} ({formatted_index_column}),\n"

    def generate_create_schema(self):
        """
        Method to generate the SQL statement for creating a schema.

        Returns:
            str: The SQL statement for creating the schema.

        Raises:
            NotImplementedError: If this method is not overridden by a subclass.
        """
        return f"CREATE SCHEMA IF NOT EXISTS {self.schema_name};\n\n" if self.schema_name else ""

    def create_table_begin(self, formatted_table_name):
        """
        Generate the beginning part of the SQL statement for creating a table.

        Args:
            formatted_table_name (str): Formatted table name.

        Returns:
            str: Beginning part of the SQL statement for creating a table.
        """
        logging.debug(f"Creating table begin statement for table '{formatted_table_name}'.")
        return f"CREATE TABLE IF NOT EXISTS {self.schema_prefix}{formatted_table_name} (\n"

    def create_table_end(self):
        """
        Generate the ending part of the SQL statement for creating a table.

        Returns:
            str: Ending part of the SQL statement for creating a table.
        """
        logging.debug("Creating table end statement.")
        return "\n);\n\n"

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
            return f"\tPRIMARY KEY ({', '.join(formatted_primary_keys)}),\n"
        else:
            return f"\tPRIMARY KEY ({formatted_primary_keys}),\n"

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
        Generate the SQL statement for creating foreign keys in a table using ALTER TABLE syntax in MySQL.

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
                f"fk_{formatted_table_name}_{referencing_column}_{referenced_table}_{referenced_column}", 64)
            statement = \
                f"\tCONSTRAINT {constraint_name} FOREIGN KEY ({referencing_column})\n" \
                f"\t\tREFERENCES {self.schema_prefix}{referenced_table} ({referenced_column}) ON DELETE CASCADE,\n"
            foreign_key_statements.append(statement)

        return '\n'.join(foreign_key_statements)
