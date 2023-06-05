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
import csv
import click
import logging
from dotenv import load_dotenv
from pattern.text import singularize
from pattern.text import pluralize
import hashlib

# Load environment variables from .env file
load_dotenv()


def configure_logging(log_file, log_level=logging.INFO):
    # Configure the logging system
    logging.basicConfig(
        level=log_level,  # Set the desired logging level
        format='%(asctime)s [%(levelname)s] %(message)s',  # Define the log message format
        filename=log_file,  # Specify the log file name
        filemode='w'  # Choose the file mode (e.g., 'w' for write, 'a' for append)
    )

    # Create a console handler and set its level to INFO
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create a formatter for the console handler
    console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    # Set the formatter for the console handler
    console_handler.setFormatter(console_formatter)

    # Get the root logger and add the console handler
    root_logger = logging.getLogger()
    root_logger.addHandler(console_handler)


def get_str_from_env(key: str, default_value: str = None) -> str:
    """
    Retrieves a string value of a given environment variable.

    Args:
        key (str): The name of the environment variable.
        default_value (str): The default value to be returned if the environment variable is not set.

    Returns:
        str: The value of the environment variable.

    Raises:
        ValueError: If the environment variable is not set and no default value is provided.
    """
    value = os.environ.get(key)
    if value is None:
        if default_value is None:
            logging.error(f'Environment variable "{key}" is not set.')
            raise ValueError(f'Environment variable "{key}" is not set.')
        else:
            return default_value

    return str(value)


def get_int_from_env(key: str, default_value: int = None) -> int:
    """
    Retrieves an integer value of a given environment variable.

    Args:
        key (str): The name of the environment variable.
        default_value (int): The default value to be returned if the environment variable is not set.

    Returns:
        int: The value of the environment variable.

    Raises:
        ValueError: If the environment variable is not set and no default value is provided.
                    Or if the environment variable is not a valid integer.
    """
    value = os.environ.get(key)

    if value is None:
        if default_value is None:
            logging.error(f"Environment variable '{key}' is not set.")
            raise ValueError(f"Environment variable '{key}' is not set.")
        else:
            return default_value

    try:
        return int(value)
    except ValueError:
        logging.error(f"Environment variable '{key}' is not a valid integer.")
        raise ValueError(f"Environment variable '{key}' is not a valid integer.")


def get_csv_delimiter(file_path, fallback_delimiter=','):
    """
    Get the delimiter used in a CSV file.

    Args:
        file_path (str): Path to the CSV file.
        fallback_delimiter (str, optional): Fallback delimiter to use when detection fails. Defaults to ','.

    Returns:
        str: The detected delimiter.

    Example:
        delimiter = get_csv_delimiter('your_file.csv')
        print(f"The delimiter used in the CSV file is: '{delimiter}'")
    """
    try:
        with open(file_path, 'r') as file:
            # Read a sample of the file to determine the delimiter
            sample = file.read(4096)  # Adjust the sample size as needed

            # Use the csv.Sniffer to detect the delimiter
            try:
                dialect = csv.Sniffer().sniff(sample)
                delimiter = dialect.delimiter
            except csv.Error:
                delimiter = fallback_delimiter

        return delimiter
    except IOError:
        logging.error(f"Failed to open file: {file_path}")
        raise


def get_csv_filenames(folder):
    """
    Retrieves the names of all CSV files in a given folder.

    Args:
        folder (str): Path of the directory.

    Returns:
        list: A list containing the names of all CSV files in the folder.
    """
    try:
        csv_filenames = [os.path.basename(file) for file in os.listdir(folder) if file.endswith('.csv')]

        # Check if any CSV files found
        if not csv_filenames:
            logging.error("Error: No CSV files found in the specified folder.")
            return None

        logging.info(f"CSV files found: {len(csv_filenames)}")

        return csv_filenames
    except OSError:
        logging.error(f"Failed to access folder: {folder}")
        raise


def is_plural(word):
    singular_form = singularize(word)
    return word != singular_form


def plural(word):
    if is_plural(word):
        return word
    return pluralize(word)


def singular(word):
    if is_plural(word):
        return singularize(word)
    return word


def lemma_compare(word1, word2):
    # Check if the singular or plural forms of words match
    return singular(word1) == singular(word2) or plural(word1) == plural(word2)


def truncate(constraint_name, max_length=128):
    """
    Truncates a long constraint name and appends a 5-digit hash of the non-truncated name for uniqueness.

    Args:
        constraint_name (str): The original constraint name.
        max_length (int): The maximum length allowed for the constraint name (default: 128).

    Returns:
        str: The truncated constraint name with a 5-digit hash of the non-truncated name.

    """

    # Calculate the maximum length for the truncated constraint name
    max_constraint_length = max_length - 5  # Subtract 5 for the 5-digit hash

    # Check if the constraint name exceeds the maximum length
    if len(constraint_name) > max_constraint_length:
        # Truncate the constraint name to fit within the maximum length
        truncated_name = constraint_name[:max_constraint_length]
    else:
        # Use the original constraint name if it's already within the maximum length
        truncated_name = constraint_name

    # Generate a hash of the non-truncated constraint name
    name_hash = hashlib.md5(constraint_name.encode()).hexdigest()[:5]

    # Append the hash to the truncated constraint name
    final_name = truncated_name + name_hash

    return final_name


class CaseInsensitiveChoice(click.ParamType):
    """
    Custom click.ParamType class to create case-insensitive choice parameters for command-line interface.
    """

    def __init__(self, choices):
        self.choices = choices

    def convert(self, value, param, ctx):
        """
        Convert the input value to one of the choices (case-insensitive).

        Args:
            value (str): The input value.
            param (click.Parameter): The parameter object.
            ctx (click.Context): The command context.

        Returns:
            str: The choice corresponding to the input value.

        Raises:
            click.BadParameter: If the input value does not match any of the choices.
        """
        if value is None:
            return None
        value_lower = value.lower()
        choices_lower = [choice.lower() for choice in self.choices]
        if value_lower not in choices_lower:
            self.fail(
                f'Invalid choice: {value}. '
                f'Choose from {", ".join(self.choices)}.',
                param,
                ctx,
            )
        return self.choices[choices_lower.index(value_lower)]
