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
import logging
import re

from db_utility.database import DatabaseUtilityBase


class PostgreSQLUtility(DatabaseUtilityBase):
    """
    A PostgreSQL specific implementation of the DatabaseUtilityBase class.
    """

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
        }

        # Initialize the logger for logging purposes
        logging.info("PostgreSQL utility created successfully.")

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

    def generate_unique_index(self, formatted_table_name, index_columns):
        """
        Generate SQL statement for creating a unique index on specified columns of a table.

        Args:
            formatted_table_name (str): Table name.
            index_columns (list): List of columns to create the unique index on.

        Returns:
            str: SQL statement for creating a unique index.
        """
        formatted_index_column = self.format_identifier(index_columns.column_name)
        logging.debug(f"Generating unique index for table '{self.schema_prefix}{formatted_table_name}' and columns " 
                      f"{formatted_index_column}.")
        return f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{formatted_table_name}_{formatted_index_column} " \
               f"ON {self.schema_prefix}{formatted_table_name} ({formatted_index_column});\n"

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

            constraint_name = f"fk_{formatted_table_name}_{referencing_column}_{referenced_table}_{referenced_column}"
            statement = \
                f"ALTER TABLE {self.schema_prefix}{formatted_table_name}\n" \
                f"  DROP CONSTRAINT IF EXISTS {constraint_name};\n" \
                f"ALTER TABLE {self.schema_prefix}{formatted_table_name}\n" \
                f"  ADD CONSTRAINT {constraint_name}\n" \
                f"  FOREIGN KEY ({referencing_column})\n" \
                f"  REFERENCES {self.schema_prefix}{referenced_table} ({referenced_column});\n"
            foreign_key_statements.append(statement)

        return '\n'.join(foreign_key_statements)
