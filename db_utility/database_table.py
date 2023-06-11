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
import common.library as common_library
from db_utility.database_column import DatabaseColumn
from db_utility.database_foreign_key import DatabaseForeignKey


class DatabaseTable:
    """
    Class that represents a database table.
    """

    def __init__(self, dataframe, csv_filename, table_name, columns, primary_keys=None, unique_indexes=None,
                 foreign_keys=None):
        """
        Initialize a DatabaseTable object.

        Args:
            dataframe (DataFrame): The dataframe representing the database table.
            csv_filename (str): The CSV file from which the dataframe was created.
            table_name (str): The name of the database table.
            columns (list): List of DatabaseColumn objects representing the columns in the database table.
            primary_keys (list, optional): List of primary keys in the database table.
            unique_indexes (list, optional): List of unique indexes in the database table.
            foreign_keys (list, optional): List of foreign keys in the database table.
        """
        self.csv_filename = csv_filename.lower()
        self.original_table_name = table_name.lower()
        self.table_name = table_name.lower()
        self.columns = columns
        self.dataframe = dataframe
        self.primary_keys = primary_keys if primary_keys else []
        self.unique_indexes = unique_indexes if unique_indexes else []
        self.foreign_keys = foreign_keys if foreign_keys else []

    def __str__(self):
        """
        String representation of the DatabaseTable object.

        Returns:
            str: String representation of the DatabaseTable object.
        """
        return f'CSV File name: {self.csv_filename}, ' \
               f'Table name: {self.table_name}, ' \
               f'Primary keys: {self.primary_keys}, ' \
               f'Columns: {[str(column) for column in self.columns]}'

    @classmethod
    def from_dataframe(cls, db, dataframe, csv_filename, table_name):
        """
        Create a DatabaseTable object from a dataframe.

        Args:
            db (Database): The Database instance for schema generation.
            dataframe (DataFrame): The dataframe which contains the CSV file format and content.
            csv_filename (str): The CSV file from which the dataframe was created.
            table_name (str): The name of the database table.

        Returns:
            DatabaseTable: The created DatabaseTable object.
        """
        columns = []
        for column in dataframe.columns:
            column_type = db.map_data_type(dataframe[column].dtype.name)
            column_size = len(dataframe[column])
            is_unique = dataframe[column].is_unique
            is_nullable = dataframe[column].isnull().any()

            # Creating a DatabaseColumn object
            db_column = DatabaseColumn(column_name=column, column_type=column_type, column_size=column_size,
                                       is_nullable=is_nullable,
                                       is_unique=is_unique)
            columns.append(db_column)

        return cls(dataframe, csv_filename, table_name, columns)

    @classmethod
    def from_database(cls, db, schema_name, table_name):
        """
        Create a DatabaseTable object from a database connection.

        Args:
            db (Database): The Database for database generation.
            schema_name (str): The name of the database schema.
            table_name (str): The name of the database table.

        Returns:
            DatabaseTable: The created DatabaseTable object.
        """

        # Get column information from the database
        column_query = db.get_column_query()
        # Execute the column query
        columns_data = db.execute_query(column_query, (table_name,))

        # Get primary key information from the database (specific to PostgreSQL)
        primary_key_query, query_parameters = db.get_primary_keys_query(schema_name, table_name)
        # Execute the primary key query
        primary_keys_data = db.execute_query(primary_key_query, query_parameters)

        # Get foreign key information from the database (specific to PostgreSQL)
        foreign_key_query = db.get_foreign_key_query()
        # Execute the foreign key query
        foreign_keys_data = db.execute_query(foreign_key_query, (schema_name, table_name,))

        # Create the DatabaseColumn objects
        columns = []
        for column_data in columns_data:
            column_name, column_type, column_size, is_nullable = column_data
            # Additional processing or mapping of types may be required based on the database system
            db_column = DatabaseColumn(column_name, column_type, column_size, is_nullable, False)
            columns.append(db_column)

        # Create the table name with '.csv' extension
        csv_filename = f"{table_name.lower()}.csv"

        # Create the DatabaseTable object
        db_table = DatabaseTable(dataframe=None, csv_filename=csv_filename, table_name=table_name, columns=columns)

        # Set the primary keys
        db_table.primary_keys = [pk[0] for pk in primary_keys_data]

        # Create the DatabaseForeignKey objects
        foreign_keys = []
        for fk_data in foreign_keys_data:
            column_name, referenced_table, referenced_column, constraint_name, \
                on_update_action, on_delete_action = fk_data

            foreign_key = DatabaseForeignKey(column_name, referenced_table, referenced_column)
            foreign_key.on_update_action = on_update_action
            foreign_key.on_delete_action = on_delete_action
            foreign_keys.append(foreign_key)

        # Set the foreign keys
        db_table.foreign_keys = foreign_keys

        return db_table

    @staticmethod
    def get_table_by_name(tables, table_name):
        """
        Get a table from a list of tables by name.

        Args:
            tables (dict): Dictionary of table objects with table names as keys.
            table_name (str): Name of the table to get.

        Returns:
            DatabaseTable: The DatabaseTable object with the given name, or None if not found.
        """
        # We will first look for the table name in the table names.
        for table in tables.values():
            if common_library.lemma_compare(table.table_name, table_name):
                return table

        # If not found, look in the original table names.
        for table in tables.values():
            if common_library.lemma_compare(table.original_table_name, table_name):
                return table

        return None

    def get_primary_key(self):
        """
        Get the primary key of the database table.

        Returns:
            str: The primary key of the database table.
        """
        if len(self.primary_keys) == 1:
            return self.primary_keys[0]
        return None

    def get_primary_keys_query(self):
        """
        Get the primary keys of the database table.

        Returns:
            list: List of primary keys of the database table.
        """
        if len(self.primary_keys) == 1:
            return [self.primary_keys]
        return self.primary_keys

    def add_primary_key(self, primary_keys):
        """
        Add a primary key to the database table.

        Args:
            primary_keys (list): List of primary keys to be added.
        """
        column_names = [column.column_name for column in self.columns]

        for primary_key in primary_keys:
            if primary_key not in column_names:
                logging.warning(f'Column {primary_key} does not exist in table {self.table_name}')
                return
            if primary_key not in self.primary_keys:
                self.primary_keys.append(primary_key)
                logging.info(f'Added primary key {primary_key} to table {self.table_name}')

    def is_primary_key(self, column):
        """
        Check if a column is a primary key in the database table.

        Args:
            column (DatabaseColumn): The column to be checked.

        Returns:
            bool: True if the column is a primary key, False otherwise.
        """
        return column.column_name in self.primary_keys

    def has_primary_key(self):
        """
        Check if the database table has a primary key.

        Returns:
            bool: True if the database table has a primary key, False otherwise.
        """
        return len(self.primary_keys) > 0

    def add_foreign_key(self, column_name, referenced_table, referenced_column):
        """
        Add a foreign key to the database table.

        Args:
            column_name (str): Name of the foreign key column.
            referenced_table (str): Name of the referenced table.
            referenced_column (str): Name of the referenced column.
        """
        foreign_key = DatabaseForeignKey(column_name, referenced_table, referenced_column)
        self.foreign_keys.append(foreign_key)

    def has_foreign_keys(self):
        """
        Check if the database table has foreign keys.

        Returns:
            bool: True if the database table has foreign keys, False otherwise.
        """
        return len(self.foreign_keys) > 0

    def add_unique_index(self, unique_indexes):
        """
        Add a unique index to the database table.

        Args:
            unique_indexes (list): List of unique indexes to be added.
        """
        if not isinstance(unique_indexes, list):
            unique_indexes = [unique_indexes]  # if not a list, convert to list

        for unique_index in unique_indexes:
            if unique_index.column_name not in [column.column_name for column in self.columns]:
                logging.warning(f'Column {unique_index} does not exist in table {self.table_name}')
                return
            if unique_index not in self.unique_indexes:
                self.unique_indexes.append(unique_index)
                logging.info(f'Added unique index {unique_index} to table {self.table_name}')

    def has_unique_indexes(self):
        """
        Check if the database table has unique indexes.

        Returns:
            bool: True if the database table has unique indexes, False otherwise.
        """
        return len(self.unique_indexes) > 0

    def update_table_name(self, new_name):
        """
        Update the name of the database table.

        Args:
            new_name (str): The new name of the database table.
        """
        logging.info(f'Updating table name from {self.table_name} to {new_name}')
        self.table_name = new_name.lower()

    def get_column_by_name(self, column_name):
        """
        Get the DatabaseColumn object based on the column name.

        Args:
            column_name (str): Name of the column.

        Returns:
            tuple: (column_object, column_index) if found, None otherwise.
        """
        for i, column in enumerate(self.columns):
            if column.column_name == column_name:
                return column, i
        return None, None

    @staticmethod
    def parse_fq_column_name(fq_column_name):
        """
        Parse the fully qualified column name and return the column name.

        Args:
            fq_column_name (str): Column reference string in the format "schema.table.column",
                                  "table.column", or "column".

        Returns:
            str: The column name.
        """
        parts = fq_column_name.split(".")
        column_name = parts[-1]
        return column_name

    def update_column_name(self, old_name, new_name):
        """
        Update the name of a column in the database table.

        Args:
            old_name (str): The old name of the column.
            new_name (str): The new name of the column.
        """
        for column in self.columns:
            if column.column_name == old_name:
                logging.info(f'Updating column name from {old_name} to {new_name}')
                column.column_name = new_name
                break
        else:
            logging.warning(f'Column {old_name} not found in table {self.table_name}')
