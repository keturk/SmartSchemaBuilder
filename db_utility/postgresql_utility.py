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
import psycopg2
import logging
import re

import common.library as lib
from db_utility.database_utility import DatabaseUtility


class PostgreSQLUtility(DatabaseUtility):
    """
    A PostgreSQL specific implementation of the Database class.
    """

    # Constructor and connection methods
    def __init__(self, schema_name):
        super().__init__(schema_name)

        # Initialize mapping from pandas data types to PostgreSQL data types
        # This mapping will be used to correctly generate SQL DDL statements based on pandas dataframes
        self.type_mapping = {
            'int64': 'bigint',
            'float64': 'double precision',
            'object': 'text',
            'bool': 'boolean',
            'datetime64': 'timestamp',
            'timedelta64': 'interval',
            'int32': 'integer',
            'float32': 'real',
            'int16': 'smallint',
            'int8': 'smallint',
            'uint8': 'smallint',
            'category': 'text',
            'uint32': 'bigint',
            'uint64': 'numeric',
            'datetime64[ns]': 'timestamp',
            'datetime64[ns, utc]': 'timestamptz',
            'timedelta64[ns]': 'interval',
            'date': 'date',
            'time': 'time',
            'timetz': 'timetz',
            'time[tz]': 'timetz',
            'bytes': 'bytea',
            'str': 'text',
            'none': 'text',
            'decimal': 'numeric',
            'uuid': 'uuid',
            'list': 'jsonb',
            'dict': 'jsonb',
            'tuple': 'jsonb',
            'set': 'jsonb',
            'ipv4address': 'inet',
            'ipv6address': 'inet',
            'string': 'text',
        }

        # Initialize the logger for logging purposes
        logging.info("PostgreSQL utility created successfully.")

    def connect(self, host, port, database, username, password):
        """
        Connect to the PostgreSQL database.

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
            port = DatabaseUtility.DEFAULT_PORTS['postgresql']

        logging.info(f"Connecting to PostgreSQL database '{database}' on host '{host}' and port '{port}'.")
        self.connection = psycopg2.connect(
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
            if self.cursor is None:
                raise Exception("Cursor is not established. Call connect() method first.")

            while True:
                result = self.cursor.fetchone()
                if result is None:
                    break
                logging.warning("Unread result: %s", result)

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
        try:
            self.cursor.execute(sql_statements)
        except Exception as e:
            logging.error(f"An error occurred while executing the SQL statements: {e}")
            raise

        # Commit the transaction
        self.connection.commit()

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
        Get the primary keys of a table in PostgreSQL.

        Args:
            schema_name (str): The name of the schema.
            table_name (str): The name of the table.

        Returns:
            list: The list of primary key column names.
        """
        query = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid
                AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass
                AND i.indisprimary;
        """
        return query, (f"{schema_name}.{table_name}",)

    def get_foreign_key_query(self):
        """
        Get the SQL query to retrieve foreign key information from the database.

        Returns:
            str: The SQL query to retrieve foreign key information.
        """
        return ("""
            SELECT
              kcu.column_name,
              ccu.table_name AS referenced_table_name,
              ccu.column_name AS referenced_column_name,
              rc.constraint_name,
              rc.update_rule,
              rc.delete_rule
            FROM
              information_schema.key_column_usage kcu
              JOIN information_schema.constraint_column_usage ccu
                ON kcu.constraint_name = ccu.constraint_name
              JOIN information_schema.table_constraints tc
                ON kcu.constraint_name = tc.constraint_name
              JOIN information_schema.referential_constraints rc
                ON tc.constraint_name = rc.constraint_name
            WHERE
              kcu.table_schema = %s AND
              kcu.table_name = %s 
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
                ccu.column_name AS column_name
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.constraint_column_usage AS ccu
                    ON tc.constraint_name = ccu.constraint_name
            WHERE
                tc.constraint_type = 'UNIQUE'
                AND tc.table_schema = %s
                AND tc.table_name = %s
        """
        return query, (schema_name, table_name)

    # DDL generation methods
    def map_data_type(self, dataframe_type):
        """
        Map pandas dataframe types to PostgreSQL data types.

        Args:
            dataframe_type (str): Pandas dataframe type.

        Returns:
            str: Corresponding PostgreSQL data type.
        """
        logging.debug(f"Mapping pandas data type '{dataframe_type}' to PostgreSQL data type.")
        return self.type_mapping.get(dataframe_type.lower(), 'TEXT')

    def get_database_type(self):
        """
        Get the type of the target database.

        Returns:
            str: Database type.
        """
        logging.debug("Get database type: PostgreSQL.")
        return "postgresql"

    def format_identifier(self, identifier):
        """
        Format SQL identifier (table or column name) by removing special characters and delimiting words with
        underscores.

        Args:
            identifier (str): Identifier to be formatted.

        Returns:
            str: Formatted identifier.
        """
        logging.debug(f"Formatting identifier '{identifier}'.")
        if identifier is None:
            return None

        # Remove any characters that are not alphanumeric or underscore
        formatted_identifier = re.sub(r'[^a-zA-Z0-9_]', ' ', identifier)

        # Delimit words with underscores
        formatted_identifier = re.sub(r'\s+', '_', formatted_identifier)

        # Remove leading and trailing underscores
        formatted_identifier = formatted_identifier.strip('_')

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
        constraint_name = lib.truncate(f"idx_{formatted_table_name}_{formatted_index_column}", 63)
        return f"\tCONSTRAINT {constraint_name} UNIQUE({formatted_index_column}),\n"

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
        logging.debug(f"Creating table begin statement for table '{self.schema_prefix}{formatted_table_name}'.")
        return f"CREATE TABLE IF NOT EXISTS {self.schema_prefix}{formatted_table_name} (\n"

    def create_table_end(self):
        """
        Generate the ending part of the SQL statement for creating a table.

        Returns:
            str: Ending part of the SQL statement for creating a table.
        """
        logging.debug("Creating table end statement.")
        return "\n);\n"

    def create_primary_keys(self, formatted_table_name, formatted_primary_keys):
        """
        Generate the SQL statement for defining primary keys in a table.

        Args:
            formatted_table_name (str): Formatted table name.
            formatted_primary_keys (list): List of formatted primary key column names.

        Returns:
            str: SQL statement for defining primary keys.
        """
        logging.debug(f"Creating primary keys for table '{formatted_table_name}' and keys {formatted_primary_keys}.")
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
        Generate the SQL statement for creating foreign keys in a table using ALTER TABLE syntax.

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
                f"fk_{formatted_table_name}_{referencing_column}_{referenced_table}_{referenced_column}", 63)
            statement = \
                f"\tCONSTRAINT {constraint_name} FOREIGN KEY ({referencing_column})\n" \
                f"\t\tREFERENCES {self.schema_prefix}{referenced_table} ({referenced_column}) ON DELETE CASCADE,\n"
            foreign_key_statements.append(statement)

        return '\n'.join(foreign_key_statements)
