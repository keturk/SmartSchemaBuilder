# Smart Schema Builder

Smart Schema Builder is a Python-based toolset that simplifies the generation of SQL DDL and INSERT statements from CSV files, as well as facilitates exporting a database to CSV files. It also provides a seamless solution for executing the generated SQL files against a target database, creating tables and populating them with data. The tool consists of three main Python scripts: `generate_sql_files.py`, `run_sql_files.py`, and `export_to_csv.py`.

With Smart Schema Builder, you can easily generate and populate a database based on the structure and content of CSV files. The tool can suggest meaningful table names using the OpenAI API, enhancing the clarity and usability of the generated SQL statements. Once you have generated the SQL files, the `run_sql_files.py` script allows you to effortlessly execute them against a database, automating the process of creating tables and inserting data from the CSV files.

In addition to generating SQL statements and exporting data to CSV files, Smart Schema Builder provides the capability to seamlessly execute the SQL files against a target database.

## Features

- Automatic generation of SQL DDL statements to create database tables based on the structure of CSV files.
- Automatic generation of SQL INSERT statements to populate the created tables with data from the CSV files.
- (Optional) Intelligent suggestion of meaningful table names using the OpenAI API, enhancing the usability and clarity of the generated SQL statements.
- Support for multiple databases (PostgreSQL, MySQL, and SQL Server), allowing the generated SQL statements to be compatible with different database systems.
- Ability to execute generated SQL files against a target database, automating the creation of tables and inserting data from the CSV files.
- Capability to export tables from a database to CSV files, facilitating tasks such as data backup, sharing data with external systems, and data analysis outside the database environment.
- Customizable and extensible architecture that allows for the addition of support for additional database types.
- Integration with existing workflows, simplifying the process of schema creation, data population, and data export from CSV files.
- Error handling and logging, providing visibility into the execution process and aiding in troubleshooting.
- Open-source nature, inviting contributions from the community to enhance and extend the functionality of the Smart Schema Builder tool.

## Directory Structure

- [common](./common): Contains utility functions and classes used throughout the project.
- [db_utility](./db_utility): Contains the database utility classes for different database types.
- [docs](./docs): Contains the documentation for the Smart Schema Builder tool.
- [test_data](./test_data): Contains the sample data used for testing the Smart Schema Builder tool.
- [export_to_csv.py](./export_to_csv.py): Script to export tables from a database to CSV files.
- [generate_sql_files.py](./generate_sql_files.py): Script to generate SQL files from CSV files.
- [run_sql_files.py](./run_sql_files.py): Script to execute SQL files against the database.
- [requirements.txt](./requirements.txt): List of required Python packages.

## Documentation

For more information and documentation on the Smart Schema Builder tool, refer to the following sections:

- [Installation](./docs/installation.md)
- [Usage](./docs/usage.md)
- [Scripts Description](./docs/scripts_description.md)
- [Workflows](./docs/workflows.md)
- [FAQ](./docs/faq.md)
- [Contributing](./docs/contributing.md)
- [License](./docs/license.md)