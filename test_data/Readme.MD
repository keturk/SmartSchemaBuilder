# Medical Database Project

This project contains CSV files representing a sample medical database. Here is the process followed for generating the data: 
1. The database structure drew inspiration from the table names provided by the [Medical Laboratories with Payments](https://web.archive.org/web/20210506204505/http://www.databaseanswers.org/data_models/medical_laboratories_with_payments/index.htm).  
2. [ChatGPT](https://chat.openai.com/) suggested columns based on the table names and context, contributing to the comprehensive database design.
3. Once the database was created on PostgreSQL, [dbSchema](https://dbschema.com/) facilitated the random generation of values.
4. The resulting database was efficiently exported to CSV files using the [export_to_csv.py](https://github.com/keturk/SmartSchemaBuilder/blob/master/export_to_csv.py) script.
5. By leveraging the [generate_sql_files.py](https://github.com/keturk/SmartSchemaBuilder/blob/master/generate_sql_files.py) script, SQL scripts were generated, capturing the structure and data of the database.
6. To ensure accuracy and reliability, the generated scripts underwent execution against MySQL, PostgreSQL, and SQL Server databases. This verification process was facilitated by the [run_sql_files.py](https://github.com/keturk/SmartSchemaBuilder/blob/master/run_sql_files.py) script.

## Project Structure

Below is the directory structure for the test data:

```
test_data
└── medical
    ├── CSV files
    ├── mysql
    │   ├── SQL scripts generated for MySQL
    ├── postgresql
    │   ├── SQL scripts generated for PostgreSQL
    └── sqlserver
        ├── SQL scripts generated for SQL Server
```

## Environment Setup
Please refer to [Project Readme.MD](https://github.com/keturk/SmartSchemaBuilder/blob/master/Readme.MD) for environment setup.

## Exporting to CSV
### Exporting to CSV from PostgreSQL
```bash
python export_to_csv.py --db-type PostgreSQL --host localhost --database postgres --username postgres --schema medical --folder .\test_data\medical
```

### Exporting to CSV from MySQL
```bash
python export_to_csv.py --db-type MySQL --host localhost --database mysql --username root --schema medical --folder .\test_data\medical
```

### Exporting to CSV from SQL Server
```bash
python export_to_csv.py --db-type SQLServer --host 10.96.143.92 --database master --username sa --schema medical --folder .\test_data\medical
```

## Generating SQL Scripts
### Generating SQL Scripts for PostgreSQL
```bash
python generate_sql_files.py --folder .\test_data\medical --db-type PostgreSQL --schema medical  
```

### Generating SQL Scripts for MySQL
```bash
python generate_sql_files.py --folder .\test_data\medical --db-type MySQL --schema medical  
```

### Generating SQL Scripts for SQL Server
```bash
python generate_sql_files.py --folder .\test_data\medical --db-type SQLServer --schema medical  
```


## Running SQL Scripts
### Running SQL Scripts for PostgreSQL
```bash
python run_sql_files.py --folder .\test_data\medical\postgresql --db-type PostgreSQL --host localhost --database postgres --username postgres
```

### Running SQL Scripts for MySQL
```bash
python run_sql_files.py --folder .\test_data\medical\mysql --db-type MySQL --host localhost --database mysql --username root
```

### Running SQL Scripts for SQL Server
```bash
python run_sql_files.py --folder .\test_data\medical\sqlserver --db-type SQLServer --host localhost --database master --username sa
```