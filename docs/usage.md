# Usage

The Smart Schema Builder tool consists of three main scripts: `export_to_csv.py`, `generate_sql_files.py`, and `run_sql_files.py`. Each script serves a specific purpose in the workflow of generating SQL statements, exporting/importing data, and executing SQL files against a target database.

To use the Smart Schema Builder tool, follow the instructions below for each script:

1. Run the script to export tables from a database to CSV files:
   ```bash
   python export_to_csv.py --db-type <database_type> --host <database_host> --port <database_port> --database <database_name> --username <username> --directory /path/to/exported_csv --limit <limit>
   ```
   Replace `<database_type>`, `<database_host>`, `<database_port>`, `<database_name>`, `<username>`, `/path/to/exported_csv`, and `<limit>` with the appropriate values for your database setup.
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

Make sure to check the specific documentation and examples for each script to understand their usage in detail.