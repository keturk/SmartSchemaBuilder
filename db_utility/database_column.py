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


class DatabaseColumn:
    """
    Class that represents a database column.
    """

    def __init__(self, column_name, column_type, column_size, is_nullable, is_unique):
        """
        Initialize a DatabaseColumn object.

        Args:
            column_name (str): The name of the database column.
            column_type (str): The type of the database column.
            is_nullable (bool): Flag indicating whether the column can contain null values.
            is_unique (bool): Flag indicating whether the column values are unique.
        """
        self.column_name = column_name.lower()
        self.column_type = column_type
        self.column_size = column_size
        self.is_nullable = is_nullable
        self.is_unique = is_unique

    def __str__(self):
        """
        String representation of the DatabaseColumn object.

        Returns:
            str: String representation of the DatabaseColumn object.
        """
        return f'Column name: {self.column_name}, Column type: {self.column_type}, Nullable: {self.is_nullable}, ' \
               f'Unique: {self.is_unique}'
