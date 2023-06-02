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


# noinspection SqlNoDataSourceInspection
# A PostgreSQL specific implementation of the DatabaseUtilityBase class
class SQLServerUtility(DatabaseUtilityBase):
    """
    A SQL Server specific implementation of the DatabaseUtilityBase class.
    """

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
        }

        logging.info("SQL Server utility created successfully.")

    def map_data_type(self, dataframe_type):
        """
        Map pandas dataframe types to SQL Server data types.

        Args:
            dataframe_type (str): Pandas dataframe type.

        Returns:
            str: Corresponding SQL Server data type.
        """
        logging.debug(f"Mapping pandas data type '{dataframe_type}' to SQL Server data type.")
        return self.type_mapping.get(dataframe_type, 'TEXT')

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
        return f"IF NOT EXISTS (SELECT 1 FROM sys.indexes " \
               f"WHERE object_id = OBJECT_ID('{self.schema_prefix}{formatted_table_name}') " \
               f"AND name = 'idx_{formatted_table_name}_{formatted_index_column}')\n" \
               f"BEGIN\n" \
               f"    CREATE UNIQUE INDEX idx_{formatted_table_name}_{formatted_index_column} " \
               f"    ON {self.schema_prefix}{formatted_table_name} ({formatted_index_column});\n" \
               f"END;\n"

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
        return f'IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = \'{formatted_table_name}\' AND type = \'U\')\n' \
               f'BEGIN\n' \
               f'\tCREATE TABLE {self.schema_prefix}{formatted_table_name} (\n'

    def create_table_end(self):
        """
        Generate the ending part of the SQL statement for creating a table.

        Returns:
            str: Ending part of the SQL statement for creating a table.
        """
        logging.debug("Creating table end statement.")
        return f'\n\t);\n' \
               f'END;\n\n'

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
            return f"\t\tCONSTRAINT PK_{formatted_table_name} " \
                   f"PRIMARY KEY ({', '.join(formatted_primary_keys)}),\n"
        else:
            return f"\t\tCONSTRAINT PK_{formatted_table_name} " \
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

        return f"\t\t{formatted_column_name} {column.column_type} {nullability},\n"

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

            constraint_name = f"fk_{formatted_table_name}_{referencing_column}_{referenced_table}_{referenced_column}"
            statement = \
                f"IF NOT EXISTS (\n" \
                f"  SELECT * FROM sys.foreign_keys\n" \
                f"      WHERE object_id = OBJECT_ID('{self.schema_prefix}{formatted_table_name}')\n" \
                f"      AND name = '{constraint_name}')\n" \
                f"BEGIN\n" \
                f"  ALTER TABLE {self.schema_prefix}{formatted_table_name}\n" \
                f"      ADD CONSTRAINT {constraint_name}\n" \
                f"      FOREIGN KEY ({referencing_column})\n" \
                f"      REFERENCES {self.schema_prefix}{referenced_table} ({referenced_column}) ON DELETE CASCADE;\n" \
                f"END;\n"
            foreign_key_statements.append(statement)

        return '\n'.join(foreign_key_statements)
