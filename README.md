# UKHSA-test

## TASK (Option 1 - On-premise SQL Server)
Location = UKHSA_TEST/etl_on_prem

In this task, I wrote a reproducible ETL pipeline to process a CSV data using python code deployable in an on-premise SQL server.

The ETL pipeline will:
1. Extract: Load data from a CSV file.
2. Transform: Clean and validate the data.
3. Load: Write the processed data to a SQL database.

 
### Solution
Logging Function:
Add detailed logging using Python’s logging module.

Configuration:
Define database and CSV details to make the pipeline reusable.

Extract Function:
Reads a CSV file into a Pandas DataFrame with error handling for common issues (e.g., file not found, empty file).

Transform Function:
Handles basic data cleaning, including removing duplicates, handling missing values, and standardising column names.

Load Function:
Connects to the SQL database using sqlalchemy and pyodbc.
Writes the DataFrame to a SQL table. If the table exists, it will be replaced.

ETL Pipeline:
Integrates the Extract, Transform, and Load steps and provides a comprehensive error-handling mechanism.


### Considerations
1. Security:
- The script supports environment-based configuration for secure and dynamic switching.
- Avoids hardcoding sensitive credentials by allowing the use of environment variables.

2. Scalability:
- Configurable to work with on-premise MySQL or AWS RDS by adjusting the environment variable.
- Modularised steps (extract, transform, and load) for better extensibility.

3. Maintainability:
- Uses structured logging for traceability and debugging.
- Includes robust error handling in each step to ensure pipeline reliability.
- This implementation is designed for deployment in both on-premise and cloud environments.


## TASK (Option 2 - AWS Cloud SQL Server)
Location = UKHSA_TEST/etl_on_aws

In this task, I wrote a reproducible ETL pipeline to process a CSV data using python code deployable in an AWS RDS SQL server.

The ETL pipeline will:
1. Extract: Load data from a CSV file.
2. Transform: Clean and validate the data.
3. Load: Write the processed data to a SQL database.

 
### Solution
Logging Function:
Add detailed logging using Python’s logging module.

Configuration:
Define database and CSV details to make the pipeline reusable.

Extract Function:
Reads a CSV file into a Pandas DataFrame with error handling for common issues (e.g., file not found, empty file).

Transform Function:
Handles basic data cleaning, including removing duplicates, handling missing values, and standardising column names.

Load Function:
Connects to the SQL database using sqlalchemy and pyodbc.
Writes the DataFrame to a SQL table. If the table exists, it will be replaced.

ETL Pipeline:
Integrates the Extract, Transform, and Load steps and provides a comprehensive error-handling mechanism.


### Considerations
1. Security:
- The script supports environment-based configuration for secure and dynamic switching.
- Avoids hardcoding sensitive credentials by allowing the use of AWS Secret Manager.

2. Scalability:
- Configurable to work on AWS RDS thereby enhancing scalability.
- Modularised steps (extract, transform, and load) for better extensibility.

3. Maintainability:
- Uses structured logging for traceability and debugging.
- Includes robust error handling in each step to ensure pipeline reliability.
- This implementation is designed for deployment in both on-premise and cloud environments.
