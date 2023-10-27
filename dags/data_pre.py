from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
import pandas as pd


# Function for data cleaning and concatenation
def clean_and_concatenate_data():
    # Replace 'test.csv' and 'train.csv' with the paths to your data files
    test_df = pd.read_csv('/opt/airflow/data/Test.csv')
    train_df = pd.read_csv('/opt/airflow/data/Train.csv')
    
    # Concatenate the dataframes
    combined_df = pd.concat([test_df, train_df], ignore_index=True)
    
    # Data cleaning operations
    cleaned_df = combined_df.drop_duplicates()  # Remove duplicate rows
    cleaned_df = cleaned_df.dropna()             # Remove rows with missing values
    
    # Save the cleaned data to a new file (e.g., 'cleaned_data.csv')
    cleaned_df.to_csv('/opt/airflow/data/cleaned_data.csv', index=False)

# Function for data verification
def verify_data():
    # Replace 'cleaned_data.csv' with the path to the cleaned data file
    cleaned_df = pd.read_csv('/opt/airflow/data/cleaned_data.csv')
    
    # Data verification checks
    has_duplicates = any(cleaned_df.duplicated())  # Check for duplicate rows
    has_missing_values = any(cleaned_df.isnull().any())  # Check for missing values
    
    if has_duplicates:
        print("Data contains duplicate rows.")
    else:
        print("No duplicate rows found in data.")
    
    if has_missing_values:
        print("Data contains missing values.")
    else:
        print("No missing values found in data.")
    
    print("Data verification completed.")
    
    
    
def load_csv_to_postgres(conn_id,**kwargs):
    # Read CSV file into a DataFrame
    cleaned_df = pd.read_csv('/opt/airflow/data/cleaned_data.csv',header = 0)
    # Lowercase column names
    cleaned_df.columns = cleaned_df.columns.str.lower()
    
    # Replace 'postgres_default' with your connection ID
    postgres_conn_id = conn_id
    
    # Specify and table names
    table_name = 'created_data'
    
    # Create PostgreSQL connection
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    
    connection_uri = "postgresql://airflow:airflow@postgres:5432/airflow"
    engine = create_engine(connection_uri)
    cleaned_df.to_sql(table_name, con=engine, schema='schema', if_exists='append', index=False)


# Create the DAG with the specified default arguments
with DAG("dataProcessing_and_ingestion", start_date=datetime(2023, 8, 4),
    schedule_interval="@daily", catchup=False) as dag:

    # Task to perform data cleaning and concatenation
    data_cleaning_task = PythonOperator(
        task_id='data_cleaning_task',
        python_callable=clean_and_concatenate_data,
    )

    # Task to load cleaned data from CSV into PostgreSQL using COPY with header
    create_schema_task = PostgresOperator(
        task_id='create_schema_task',
        sql=[
        """
        CREATE SCHEMA IF NOT EXISTS schema;
        """,
        """
        DROP TABLE schema.created_data;
        """,
        """
        CREATE TABLE IF NOT EXISTS schema.created_data (
            Item_Identifier VARCHAR(50),
            Item_Weight INT,
            Item_Fat_Content VARCHAR(50),
            Item_Visibility FLOAT,
            Item_Type VARCHAR(50),
            Item_MRP FLOAT,
            Outlet_Identifier VARCHAR(50),
            Outlet_Establishment_Year INT,
            Outlet_Size VARCHAR(50),
            Outlet_Location_Type VARCHAR(50),
            Outlet_Type VARCHAR(50),
            Item_Outlet_Sales FLOAT
        );
        """
    ],
        postgres_conn_id="postgres_default",  # Replace with your connection ID
        database="airflow",
        dag=dag,
    )
    
    

    # Task to verify the cleaned data
    data_verification_task = PythonOperator(
        task_id='data_verification_task',
        python_callable=verify_data,
    )

    # Task to create schema and table, then load cleaned data from CSV into PostgreSQL using Pandas
    load_csv_to_postgres_task = PythonOperator(
    task_id='load_csv_to_postgres_task',
    python_callable=load_csv_to_postgres,
    op_args=['postgres_default'],  # Pass the connection ID as an argument
    provide_context=True,
    dag=dag
    )
    
    # Define the task dependencies
    data_cleaning_task >> data_verification_task >> create_schema_task >> load_csv_to_postgres_task
