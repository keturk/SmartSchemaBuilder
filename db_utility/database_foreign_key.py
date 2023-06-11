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


class DatabaseForeignKey:
    """
    Class that represents a foreign key in a database table.
    """

    def __init__(self, column_name, referenced_table, referenced_column, on_update_action=None, on_delete_action=None):
        """
        Initialize a DatabaseForeignKey object.

        Args:
            column_name (str): Name of the foreign key column.
            referenced_table (str): Name of the referenced table.
            referenced_column (str): Name of the referenced column.
            on_update_action (str, optional): On-update action for the foreign key.
            on_delete_action (str, optional): On-delete action for the foreign key.
        """
        self.column_name = column_name
        self.referenced_table = referenced_table
        self.referenced_column = referenced_column
        self.on_update_action = on_update_action
        self.on_delete_action = on_delete_action

    def __str__(self):
        """
        String representation of the DatabaseForeignKey object.

        Returns:
            str: String representation of the DatabaseForeignKey object.
        """
        return f'Column name: {self.column_name}, ' \
               f'Referenced table: {self.referenced_table}, ' \
               f'Referenced column: {self.referenced_column}, ' \
               f'On-update action: {self.on_update_action}, ' \
               f'On-delete action: {self.on_delete_action}'
