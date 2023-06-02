# Smart Schema Builder

Smart Schema Builder is a Python-based toolset that simplifies the generation of SQL DDL and INSERT statements from CSV 
files, as well as facilitates exporting a database to CSV files. It also provides a seamless solution for executing the 
generated SQL files against a target database, creating tables and populating them with data. The tool consists of three 
main Python scripts: `generate_sql_files.py`, `run_sql_files.py`, and `export_to_csv.py`.

With Smart Schema Builder, you can easily generate and populate a database based on the structure and content of 
CSV files. The tool can suggest meaningful table names using the OpenAI API, enhancing the clarity and usability of the 
generated SQL statements. Once you have generated the SQL files, the `run_sql_files.py` script allows you to effortlessly 
execute them against a database, automating the process of creating tables and inserting data from the CSV files.

In addition to generating SQL statements and exporting data to CSV files, Smart Schema Builder provides the capability 
to seamlessly execute the SQL files against a target database. 

## Features

- Automatic generation of SQL DDL statements to create database tables based on the structure of CSV files.
- Automatic generation of SQL INSERT statements to populate the created tables with data from the CSV files.
- (Optional) Intelligent suggestion of meaningful table names using the OpenAI API, enhancing the usability and clarity of the generated SQL statements.
- Support for multiple databases (PostgreSQL, MySQL, and SQL Server), allowing the generated SQL statements to be compatible with different database systems.
- Ability to execute generated SQL files against a target database using the `run_sql_files.py` script, automating the creation of tables and population of data from CSV files.
- Capability to export tables from a database to CSV files using the `export_to_csv.py` script, facilitating tasks such as data backup, sharing data with external systems, and data analysis outside the database environment.
- Customizable and extensible architecture that allows for the addition of support for additional database types, enabling users to adapt the tool to their specific database environment.
- Seamless integration with existing workflows, simplifying the process of schema creation, data population, and data export from CSV files.
- Robust error handling and logging, providing visibility into the execution process and aiding in troubleshooting.
- Open-source nature, inviting contributions from the community to enhance and extend the functionality of the Smart Schema Builder tool.


## `export_to_csv.py`

The `export_to_csv.py` script allows exporting tables from an existing database to CSV files. It performs the following tasks:

1. It retrieves a list of tables from the specified database.
2. It creates a directory to store the exported CSV files.
3. For each table, it fetches all rows and column names from the table. It writes the data to a CSV file, with the first row containing the column names.

## `generate_sql_files.py`

The `generate_sql_files.py` script is responsible for generating SQL DDL and INSERT statements based on the provided CSV files. It performs the following tasks:

1. It loads the CSV files from the specified folder into `pandas` dataframes from them.
2. If an `OPENAI_API_KEY` is provided, it utilizes the OpenAI API for suggesting more meaningful table names based on the CSV file names.
3. It attempts to interpret the data and determine primary keys, foreign keys, nullable columns and unique indexes.
4. It generates SQL DDL statements to create the database tables based on the table structure inferred from the CSV files.
5. It generates SQL INSERT statements to populate the tables with data extracted from the CSV files.

## `run_sql_files.py`

The `run_sql_files.py` script is responsible for executing the generated SQL files against a target database. It performs the following tasks:

1. It reads and executes the DDL statements from the generated SQL files, creating the database tables.
2. It parses the DDL statements using `sqlparse` to extract the order of table creation. This ensures inserting data into tables with foreign key constraints is done in the correct order.
3. It reads and executes the INSERT statements from the generated SQL files in the correct order, populating the tables with data.

**Example: Exporting a database from MySQL to PostgreSQL**

Suppose you have a MySQL database called `mydb` and you want to migrate it to a PostgreSQL database called `newdb`.

1. Export the schema and data from the MySQL database to CSV files using `export_to_csv.py`:
   ```bash
   python export_to_csv.py --db-type mysql --host <mysql_host> --port <mysql_port> --database mydb --username <mysql_username> --directory /path/to/csv_files
   ```
   Replace `<mysql_host>`, `<mysql_port>`, and `<mysql_username>` with the appropriate values for your MySQL setup.
   This will create CSV files in the specified directory (`/path/to/csv_files`) representing the schema and data of the MySQL database.

2. Generate SQL files from the CSV files using `generate_sql_files.py`:
   ```bash
   python generate_sql_files.py /path/to/csv_files --db-type postgresql 
   ```
   Replace `/path/to/csv_files` with the path to the directory containing the exported CSV files. 
   This will generate SQL DDL and INSERT statements based on the CSV files, including suggested table names using the OpenAI API.

3. Execute the generated SQL files against the PostgreSQL database using `run_sql_files.py`:
   ```bash
   python run_sql_files.py --db-type postgresql --host <postgresql_host> --port <postgresql_port> --database newdb --username <postgresql_username> --directory /path/to/sql_files
   ```
   Replace `<postgresql_host>`, `<postgresql_port>`, `<postgresql_username>`, and `/path/to/sql_files` with the appropriate values for your PostgreSQL setup.
   This will create the tables and populate them with data in the PostgreSQL database. The migration from MySQL to PostgreSQL is now complete, and you have successfully exported the database schema and data from one database system to another. **However, it is important to note that the migration process is the best effort and may not be able to fully migrate all primary keys, foreign keys, unique indexes, and other database constraints. The success of the migration depends on the quality and compatibility of the data between the source and target databases. It is recommended to carefully review and validate the migrated data to ensure its integrity and correctness in the new database system.**

**Example: Creating a test database from a CSV dataset**

Suppose you downloaded a dataset from [Kaggle](https://www.kaggle.com/) in CSV format that you want to import into a test database 
for analysis and testing purposes.

1. Download the public dataset and place it in a directory. You can find an example database [here](https://www.kaggle.com/datasets/arashnic/fitbit), which can be used for testing purposes.

2. Generate SQL files from the CSV file using `generate_sql_files.py`:
   ```bash
   python generate_sql_files.py /path/to/csv_file --db-type <database_type>
   ```
   Replace `/path/to/csv_file` with the path to the CSV file, and `<database_type>` with the target database type (`postgresql`, `mysql`, or `sqlserver`).
   This will generate SQL DDL and INSERT statements based on the CSV file.

3. Execute the generated SQL files against the target database using `run_sql_files.py`:
   ```bash
   python run_sql_files.py --db-type <database_type> --host <database_host> --port <database_port> --database <database_name> --username <username> --directory /path/to/sql_files
   ```
   Replace `<database_type>`, `<database_host>`, `<database_port>`, `<database_name>`, `<username>`, and `/path/to/sql_files` with the appropriate values for your database setup.
   This will create the tables and populate them with data in the target database.
   You now have a test database with the data from the public dataset, ready for analysis and testing.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/keturk/smart-schema-builder.git
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Setting up environment Variables

The Smart Schema Builder tool utilizes the following environment variables. 

- `DB_PASSWORD`: Specifies the password for the database. This environment variable is used by `export_to_csv.py` and `run_sql_files.py` scripts.

- `OPENAI_API_KEY`: Specifies the API key for the OpenAI API. This environment variable is used by the `generate_sql_files.py` script. If this variable is not defined, the functionality dependent on the OpenAI API will be skipped.

- `OPENAI_ENGINE`: Specifies the OpenAI engine to be used. This environment variable is used by the `generate_sql_files.py` script.

- `OPENAI_MAX_TOKENS`: Specifies the maximum number of tokens to be used in the OpenAI API calls. This environment variable is used by the `generate_sql_files.py` script.

Make sure to set the appropriate values for these environment variables in the `.env` file before running the Smart Schema Builder tool.

## Usage

1. Run the script to export tables from a database to CSV files:
   ```bash
   python export_to_csv.py --db-type <database_type> --host <database_host> --port <database_port> --database <database_name> --username <username> --directory /path/to/exported_csv
   ```
   Replace `<database_type>`, `<database_host>`, `<database_port>`, `<database_name>`, `<username>`, and `/path/to/exported_csv` with the appropriate values for your database setup.
   This will export the tables from the specified database to CSV files in the specified directory.

2. Run the script to generate SQL files from CSV files:
   ```bash
   python generate_sql_files.py /path/to/csv_files --db-type <database_type>
   ```
   Replace `/path/to/csv_files` with the path to the directory containing the CSV files, and `<database_type>` with the target database type (`postgresql`, `mysql`, or `sqlserver`).

3. SQL files will be generated in the `/path/to/csv_files/<database_type>` directory.

4. (Optional) Modify the generated SQL files as needed before executing them against the database.

5. Run the script to execute the SQL files against the database:
   ```bash
   python run_sql_files.py --db-type <database_type> --host <database_host> --port <database_port> --database <database_name> --username <username> --directory /path/to/sql_files
   ```
   Replace `<database_type>`, `<database_host>`, `<database_port>`, `<database_name>`, `<username>`, and `/path/to/sql_files` with the appropriate values for your database setup.

## Directory Structure

- `common`: Contains utility functions and classes used throughout the project.
- `db_utility`: Contains the database utility classes for different database types.
- `export_to_csv.py`: Script to export tables from a database to CSV files.
- `generate_sql_files.py`: Script to generate SQL files from CSV files.
- `run_sql_files.py`: Script to execute SQL files against the database.
- `requirements.txt`: List of required Python packages.

## Contributing

Contributions to Smart Schema Builder are welcome! Please refer to the [contribution guidelines](CONTRIBUTING.MD) for more information.

## License

Smart Schema Builder is released under the [MIT License](LICENSE).
