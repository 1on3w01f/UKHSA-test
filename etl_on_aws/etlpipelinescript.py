# AWS Cloud MySQL ETL Pipeline
import os
import pandas as pd
from sqlalchemy import create_engine
import logging
import boto3
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="aws_etl.log",
    filemode="a"
)

def get_secret(etlMysql):
    client = boto3.client("secretsmanager", region_name="us-east-1")
    response = client.get_secret_value(SecretId=etlMysql)
    secret = json.loads(response["SecretString"])
    return secret["password"]

# Configuration for AWS MySQL
AWS_CONFIG = {
    "db_type": "mysql",
    "host": "etl-mysql.cja00qcqaue2.us-east-1.rds.amazonaws.com",
    "port": 3306,
    "database": "etl_pipeline_db",
    "username": "admin",
    "password": get_secret("etlMysql"),
    "csv_file": "employee_data.csv",
}

# Step 1: Extract data from CSV
def extract_data(csv_file):
    try:
        logging.info(f"Extracting data from {csv_file}")
        data = pd.read_csv(csv_file)
        logging.info("Data extraction successful.")
        return data
    except FileNotFoundError:
        logging.error(f"File not found: {csv_file}")
        raise
    except pd.errors.EmptyDataError:
        logging.error("The file is empty.")
        raise
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise

# Step 2: Transform data
def transform_data(data):
    try:
        logging.info("Starting data transformation.")
        # Drop duplicates
        data = data.drop_duplicates()
        # Drop rows with missing values
        data = data.dropna()
        # Standardize column names
        data.columns = [col.strip().lower().replace(" ", "_") for col in data.columns]
        logging.info("Data transformation completed.")
        return data
    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise

# Step 3: Load data to AWS MySQL
def load_data(data, config):
    try:
        logging.info("Connecting to AWS MySQL database.")
        connection_string = (
            f"mysql+pymysql://{config['username']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['database']}"
        )
        engine = create_engine(connection_string)
        table_name = "etl_processed_data"
        data.to_sql(table_name, engine, if_exists="replace", index=False)
        logging.info(f"Data successfully loaded into {table_name} table.")
    except Exception as e:
        logging.error(f"Error during loading: {e}")
        raise

# Main ETL pipeline function
def etl_pipeline(config):
    try:
        logging.info("Starting ETL pipeline for AWS.")
        # Extract
        raw_data = extract_data(config["csv_file"])
        # Transform
        processed_data = transform_data(raw_data)
        # Load
        load_data(processed_data, config)
        logging.info("ETL pipeline completed successfully.")
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")

# Entry point
if __name__ == "__main__":
    etl_pipeline(AWS_CONFIG)