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
import re
import click
import pandas as pd
import common.library as common_library
import common.openai_utility as openai
from common.ai_provider import generate_table_names
from common.exceptions import (
    FileProcessingError, CSVProcessingError, SQLGenerationError, 
    ValidationError, ConfigurationError, FatalError
)
from common.error_handler import handle_errors, safe_execute, error_handler
from common.validators import InputValidator
import logging

import json
from db_utility.database_utility import DatabaseUtility
from db_utility.database_table import DatabaseTable
from db_utility.database_column import DatabaseColumn
from db_utility.database_foreign_key import DatabaseForeignKey

common_library.configure_logging('generate_sql_files.log', logging.INFO)


@handle_errors(fallback_value=None, context="table_name_generation")
def get_table_names(database_tables, folder):
    """
    Function to get table names for database tables using AI provider.

    :param database_tables: A dictionary containing database tables with CSV filenames as keys and DatabaseTable
    objects as values.
    :param folder: The folder where CSV files are located. This is used in the prompt for the
    AI provider.
    :return: None. Updates the table names in the database_tables dictionary in-place.
    """
    if not database_tables:
        logging.warning("No database tables provided for name generation")
        return

    # Validate inputs
    try:
        InputValidator.validate_directory_path(folder, must_exist=True)
    except ValidationError as e:
        raise FileProcessingError(f"Invalid folder path: {e}", file_path=folder) from e

    # Getting CSV filenames from the loaded data tables
    csv_filenames = [db_table.csv_filename for db_table in database_tables.values()]

    try:
        # Use the new AI provider system
        suggested_names = generate_table_names(csv_filenames, folder)

        # Assign the suggested names to the corresponding tables
        for csv_file, suggested_name in zip(csv_filenames, suggested_names):
            try:
                # Validate table name
                validated_name = InputValidator.validate_table_name(suggested_name)
                database_tables[csv_file].update_table_name(validated_name)
            except ValidationError as e:
                logging.warning(f"Invalid table name '{suggested_name}' for {csv_file}: {e}")
                # Use fallback naming
                fallback_name = os.path.splitext(csv_file)[0].lower()
                database_tables[csv_file].update_table_name(fallback_name)
    except Exception as e:
        logging.error(f"Failed to generate table names: {e}")
        # Fallback to simple naming
        for csv_file in csv_filenames:
            fallback_name = os.path.splitext(csv_file)[0].lower()
            database_tables[csv_file].update_table_name(fallback_name)


@handle_errors(fallback_value={}, context="csv_loading")
def load_csv_files(folder, csv_filenames, db):
    """
    Function to load CSV files from a given folder into pandas dataframes.

    :param folder: The folder path where the CSV files are located.
    :param csv_filenames: A list of CSV filenames.
    :param db: A Database object.
    :return: A dictionary containing database tables with CSV filenames as keys and DatabaseTable objects as values.
    """
    if not csv_filenames:
        logging.warning("No CSV filenames provided")
        return {}

    # Validate inputs
    try:
        InputValidator.validate_directory_path(folder, must_exist=True)
    except ValidationError as e:
        raise FileProcessingError(f"Invalid folder path: {e}", file_path=folder) from e

    logging.info("Loading CSV files into memory...")

    database_tables = {}
    failed_files = []
    
    for csv_filename in csv_filenames:
        try:
            # Validate CSV file
            full_filename = os.path.join(folder, csv_filename)
            InputValidator.validate_csv_file(full_filename)
            
            table_name = os.path.splitext(os.path.basename(csv_filename))[0]
            csv_delimiter = common_library.get_csv_delimiter(full_filename)
            
            dataframe = pd.read_csv(
                full_filename, 
                delimiter=csv_delimiter, 
                na_values=['NULL', '', 'NaN'],
                keep_default_na=False, 
                skipinitialspace=True
            )
            
            if dataframe.empty:
                logging.warning(f"Empty CSV file: {csv_filename}")
                continue
                
            dataframe = dataframe.convert_dtypes()
            database_tables[csv_filename] = DatabaseTable.from_dataframe(
                db, dataframe, csv_filename, table_name)
            logging.info(f"Successfully loaded CSV file: {csv_filename}")
            
        except pd.errors.EmptyDataError:
            logging.warning(f"Empty CSV file: {csv_filename}")
            failed_files.append(csv_filename)
        except pd.errors.ParserError as e:
            logging.error(f"Error parsing CSV file {csv_filename}: {e}")
            failed_files.append(csv_filename)
        except ValidationError as e:
            logging.error(f"Invalid CSV file {csv_filename}: {e}")
            failed_files.append(csv_filename)
        except Exception as e:
            logging.error(f"Unexpected error loading CSV file {csv_filename}: {e}")
            failed_files.append(csv_filename)

    if failed_files:
        logging.warning(f"Failed to load {len(failed_files)} CSV files: {failed_files}")
    
    if not database_tables:
        raise CSVProcessingError("No CSV files could be loaded successfully", csv_file="all")
    
    logging.info(f"Successfully loaded {len(database_tables)} CSV files.")
    return database_tables


# Primary Keys:
#
# The function identifies primary keys by finding the first n columns in each table that are not duplicated together.
# It starts with n = 1 and incrementally checks for duplicates in the selected columns.
# If duplicates are found, the script moves to the next set of columns until a set is found without duplicates.
# The columns without duplicates are considered as primary keys for the respective table.
def find_primary_keys(database_tables):
    """
    Function to find primary keys in database tables.

    :param database_tables: A dictionary containing database tables with CSV filenames as keys and DatabaseTable
    objects as values.
    :return: None. Updates the primary keys in the database_tables dictionary in-place.
    """

    for database_table in database_tables.values():
        primary_keys = []
        dataframe = database_table.dataframe
        # Try to find primary keys by finding first n columns that are not duplicated together
        for n in range(1, len(dataframe.columns)):
            # Get the first n columns as potential keys
            potential_keys = dataframe.columns[:n]
            # If there are duplicates in the potential keys, then they are not primary keys
            if dataframe[potential_keys].duplicated().any():
                continue
            else:
                # If there are no duplicates, then the potential keys are primary keys
                primary_keys = potential_keys
                break

        # Log the found primary keys for the table
        logging.info(f"Found primary keys for table {database_table.table_name}: {', '.join(primary_keys)}")
        # Update the primary keys in the database table
        database_table.add_primary_key(primary_keys)


# Find unique indexes by checking if a column has unique values within a table.
# It iterates through each column in a table and verifies if the number of unique values in the column is equal to
# the number of rows in the table.
# If a column satisfies this condition and is not a primary key, it is considered a unique index for the table.
def find_unique_indexes(database_tables):
    """
    Function to find unique indexes in database tables.

    :param database_tables: A dictionary containing database tables with CSV filenames as keys and DatabaseTable
    objects as values.
    :return: None. Updates the unique indexes in the database_tables dictionary in-place.
    """

    # Find unique indexes by finding columns that have unique values
    for database_table in database_tables.values():
        unique_indexes = []

        for column in database_table.columns:
            # Skip primary keys
            if database_table.is_primary_key(column):
                continue

            dataframe = database_table.dataframe
            # If the number of unique values in the column is equal to the number of rows in the table,
            # then the column can be a unique index
            if dataframe[column.column_name].nunique() == len(dataframe):
                unique_indexes.append(column)

        if unique_indexes:
            logging.info(f"Found unique indexes for table {database_table.table_name}: "
                         f"{', '.join([idx.column_name for idx in unique_indexes])}")
            database_table.add_unique_index(unique_indexes)


# Foreign Keys:
#
# The function identifies foreign keys in two ways:
#
# 1. For the first case, it iterates through all the tables and compares the column name with the primary key
#    column name of each table. If a match is found, a foreign key relationship is established between the tables.
#    The referenced table must have a single column primary key.
#
# 2. For the second case, it checks if a column name ends with '_id' and attempts to find a table with a
#    corresponding name (without '_id'). If the referenced table exists, a foreign key relationship is established
#    between the tables based on the 'id' column. The referenced table must have a single column primary key.
def find_foreign_keys(database_tables):
    """
    Function to find foreign keys in database tables.

    :param database_tables: A dictionary containing database tables with CSV filenames as keys and DatabaseTable
    objects as values.
    :return: None. Updates the foreign keys in the database_tables dictionary in-place.
    """

    for database_table in database_tables.values():
        table_name = database_table.table_name
        for column in database_table.columns:
            column_name = column.column_name

            # Skip if the column is a primary key
            if database_table.is_primary_key(column):
                continue

            # Check if the column has the same name as the primary key of another table
            for referenced_table in database_tables.values():
                # Skip if the referenced table is the same as the current table
                if referenced_table.table_name == table_name:
                    continue

                # Skip if the referenced table does not have a single column primary key
                referenced_primary_key = referenced_table.get_primary_key()
                if referenced_primary_key and referenced_primary_key == column_name:
                    database_table.add_foreign_key(column_name, referenced_table.table_name, referenced_primary_key)
                    logging.info(f"Found foreign key relationship: {database_table.table_name}.{column_name} "
                                 f"references {referenced_table.table_name}.{referenced_primary_key}")
                    break

            # Check if the column has the same name as a primary key in another table
            if column_name.endswith('_id'):
                referenced_table_name = column_name[:-3].lower()  # Remove '_id' postfix

                if common_library.lemma_compare(table_name, referenced_table_name):
                    continue

                referenced_table = DatabaseTable.get_table_by_name(database_tables, referenced_table_name)

                # Add the foreign key relationship if the referenced table and column exist
                if referenced_table:
                    referenced_primary_key = referenced_table.get_primary_key()
                    if referenced_primary_key:
                        if referenced_primary_key == 'id':
                            database_table.add_foreign_key(column_name, referenced_table.table_name,
                                                           referenced_primary_key.column_names)
                            logging.info(f"Found foreign key relationship: {database_table.table_name}.{column_name} "
                                         f"references {referenced_table.table_name}.{referenced_primary_key}")
                        else:
                            referenced_column, column_index = referenced_table.get_column_by_name('id')
                            if referenced_column:
                                database_table.add_foreign_key(column_name, referenced_table.table_name,
                                                               referenced_column.column_name)
                                logging.info(f"Found foreign key relationship: "
                                             f"{database_table.table_name}.{column_name} references "
                                             f"{referenced_table.table_name}.{referenced_column.column_name}")
                            else:
                                logging.warning(f"Referenced column not found for {table_name}.{column_name} "
                                                f"(Referenced column name: id)")
                else:
                    logging.warning(f"Referenced table not found for {table_name}.{column_name} "
                                    f"(Referenced table name: {referenced_table_name})")


def load_schema_from_json(folder, schema, database_tables):
    """
    Load the schema information from a JSON file and create DatabaseTable, DatabaseColumn, and DatabaseForeignKey instances.

    Args:
        folder (str): Path to the folder containing the JSON file.
        schema (str): The name of the schema.
        database_tables (dict): A dictionary containing DatabaseTable instances.

    Returns:
        bool: True if the schema was loaded successfully, False otherwise.
    """
    json_filename = os.path.join(folder, f"ddl_{schema}.json")

    if not os.path.isfile(json_filename):
        logging.error(f"JSON file '{json_filename}' not found.")
        return False

    try:
        with open(json_filename, 'r') as f:
            schema_data = json.load(f)

        for table_name, table_info in schema_data.items():
            # Find the corresponding DatabaseTable object
            database_table = DatabaseTable.get_table_by_name(database_tables, table_name)

            if database_table is None:
                logging.warning(f"Table '{table_name}' not found in the loaded CSV files.")
                continue

            columns = []
            foreign_keys = []

            # Extract columns information
            for column_info in table_info['columns']:
                if isinstance(column_info, str):
                    column_info = json.loads(column_info)

                column_name = column_info['name']
                column_type = column_info['type']
                nullable = column_info['nullable']
                unique = column_info['unique']

                # Extract column size if available
                column_size = column_info.get('size')

                # Create DatabaseColumn instance
                column = DatabaseColumn(column_name, column_type, column_size, nullable, unique)
                columns.append(column)

            # Extract primary keys
            primary_keys = table_info['primary_keys']

            # Extract foreign keys information
            for foreign_key_info in table_info['foreign_keys']:
                if isinstance(foreign_key_info, str):
                    foreign_key_info = json.loads(foreign_key_info)

                foreign_key_column = foreign_key_info['column']
                referenced_table = foreign_key_info['referenced_table']
                referenced_column = foreign_key_info['referenced_column']
                on_update_action = foreign_key_info['on_update_action']
                on_delete_action = foreign_key_info['on_delete_action']

                # Create DatabaseForeignKey instance
                foreign_key = DatabaseForeignKey(foreign_key_column, referenced_table, referenced_column,
                                                 on_update_action, on_delete_action)
                foreign_keys.append(foreign_key)

            # Update the DatabaseTable object with the extracted information
            database_table.columns = columns
            database_table.primary_keys = primary_keys
            database_table.foreign_keys = foreign_keys

        logging.info(f"Schema loaded from JSON file: '{json_filename}'")
        return True

    except (IOError, json.JSONDecodeError) as e:
        logging.error(f"Error loading schema from JSON file: {str(e)}")
        return False


@click.command()
@click.argument('folder')
@click.option('--folder', prompt='Enter the directory path containing the SQL files')
@click.option('--db-type', type=common_library.CaseInsensitiveChoice(DatabaseUtility.SUPPORTED_DATABASES), prompt=True)
@click.option('--schema', default=None, prompt='Enter the database schema name (optional)')
@handle_errors(fallback_value=None, context="sql_generation", reraise=True)
def generate_sql_files(folder: str, db_type: str, schema: str):
    """
    Main function (entry point) of the application. Generates SQL files based on CSV files in the provided folder.

    :param folder: Path to the folder containing CSV files.
    :param db_type: Name of the target database (PostgreSQL, MySQL, SQL Server).
    :param schema: Name of the database schema (optional).
    :return: None. Creates DDL and insert statement files in the specified folder.
    """
    try:
        # Validate inputs
        folder = os.path.abspath(folder)
        InputValidator.validate_directory_path(folder, must_exist=True)
        db_type = InputValidator.validate_database_type(db_type)
        
        if schema:
            schema = InputValidator.validate_table_name(schema)

        logging.info(f"Folder: {folder}")
        logging.info(f"Target Database: {db_type}")
        if schema:
            logging.info(f"Schema: {schema}")

        # Create a database instance for the specified database type
        db = DatabaseUtility.create(db_type, schema)
        if not db:
            raise ConfigurationError(f"Failed to create database utility for type: {db_type}")

        # Get names of CSV files in folder
        csv_filenames = common_library.get_csv_filenames(folder)
        if not csv_filenames:
            raise FileProcessingError("No CSV files found in the specified folder", file_path=folder)

        # Load CSV files into Pandas dataframes
        database_tables = load_csv_files(folder, csv_filenames, db)
        if not database_tables:
            raise CSVProcessingError("No CSV files could be loaded successfully", csv_file="all")

        # Check if AI provider is available
        from common.ai_provider import get_ai_provider
        ai_provider = get_ai_provider()
        use_ai = ai_provider is not None

        # Try to load existing schema
        schema_found = safe_execute(
            load_schema_from_json, 
            folder, schema, database_tables,
            fallback_value=False,
            context="schema_loading"
        )

        if not schema_found:
            if use_ai:
                # Generate meaningful names for the files using AI provider
                safe_execute(
                    get_table_names,
                    database_tables, folder,
                    fallback_value=None,
                    context="table_name_generation"
                )

            # Find primary keys in the tables
            safe_execute(
                find_primary_keys,
                database_tables,
                fallback_value=None,
                context="primary_key_detection"
            )

            # Find foreign keys in the tables
            safe_execute(
                find_foreign_keys,
                database_tables,
                fallback_value=None,
                context="foreign_key_detection"
            )

            # Find unique indexes in the tables
            safe_execute(
                find_unique_indexes,
                database_tables,
                fallback_value=None,
                context="unique_index_detection"
            )

        # Generate DDL statements
        try:
            db.generate_ddl(folder, database_tables)
        except Exception as e:
            raise SQLGenerationError(f"Failed to generate DDL statements: {e}") from e

        # Generate insert statements
        try:
            db.generate_insert_statements(folder, database_tables)
        except Exception as e:
            raise SQLGenerationError(f"Failed to generate INSERT statements: {e}") from e

        logging.info("SQL file generation completed successfully.")
        
        # Print error summary if there were any errors
        error_summary = error_handler.get_error_summary()
        if error_summary["total_errors"] > 0:
            logging.warning(f"Process completed with {error_summary['total_errors']} errors. Check logs for details.")

    except ValidationError as e:
        logging.error(f"Input validation failed: {e}")
        raise
    except ConfigurationError as e:
        logging.error(f"Configuration error: {e}")
        raise
    except FileProcessingError as e:
        logging.error(f"File processing error: {e}")
        raise
    except CSVProcessingError as e:
        logging.error(f"CSV processing error: {e}")
        raise
    except SQLGenerationError as e:
        logging.error(f"SQL generation error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise FatalError(f"Unexpected error during SQL generation: {e}") from e


if __name__ == '__main__':
    generate_sql_files()
