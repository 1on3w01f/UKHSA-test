import pandas as pd
from sqlalchemy import create_engine
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="on_premise_etl.log",
    filemode="a"
)

# Configuration for On-Premise MySQL
ON_PREMISE_CONFIG = {
    "db_type": "mysql",
    "host": "localhost",
    "port": 3306,
    "database": "etl_pipeline_db",
    "username": "root",
    "password": os.getenv("DB_PASSWORD"),
    "csv_file": "employee_data.csv",
}

# ETL_Step 1: Extract data from CSV
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

# ETL_Step 2: Transform data
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

# ETL_Step 3: Load data to MySQL
def load_data(data, config):
    try:
        logging.info("Connecting to on-premise MySQL database.")
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
        logging.info("Starting ETL pipeline for on-premise.")
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
    etl_pipeline(ON_PREMISE_CONFIG)
