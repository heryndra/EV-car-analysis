from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
from elasticsearch import Elasticsearch
'''
=================================================
Milestone 3

Nama  : M.Gifhari Heryndra
Batch : HCK-015

The primary objective of working with the dataset on Battery Electric Vehicles (BEVs) and Plug-in Hybrid Electric Vehicles (PHEVs) registered through the Washington State Department of Licensing (DOL) is to gain insights into the characteristics, distribution, and trends of EV registrations within the state. This information is valuable for government agencies, policymakers, researchers, the automotive industry, and environmental organizations.
=================================================
'''

def load_csv_to_postgres():
    """
    Loads data from a CSV file into a PostgreSQL table.

    This function reads data from a specified CSV file and loads it into a 
    PostgreSQL database. The database connection details are hardcoded within the function.

    """

    database = "airflow_m3"
    username = "airflow_m3"
    password = "airflow_m3"
    host = "postgres"

    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    engine = create_engine(postgres_url)

    conn=engine.connect()

    df = pd.read_csv('/opt/airflow/dags/P2M3_gifhari_data_raw.csv')
    df.to_sql('table_m3',conn,index=False,if_exists='replace')

def ambil_data():

    """
    Loads data from a CSV file into a PostgreSQL table.

    This function performs the same task as `load_csv_to_postgres` by reading
    data from a CSV file and loading it into a PostgreSQL database. The database 
    connection details are hardcoded within the function.
    
    """
    
    database = "airflow_m3"
    username = "airflow_m3"
    password = "airflow_m3"
    host = "postgres"

    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    engine = create_engine(postgres_url)

    conn = engine.connect()

    df = pd.read_csv('/opt/airflow/dags/P2M3_gifhari_data_raw.csv')
    df.to_sql('table_m3',conn, index = False, if_exists='replace')

def cleaning():
    """
    Cleans the data by removing whitespace, converting to lowercase, removing duplicates, and handling missing values.

    This function reads data from a CSV file, cleans the data by stripping whitespace
    from the columns, converting column names to lowercase, replacing spaces with underscores, 
    and removing any non-word characters. It also removes duplicate rows and rows with missing values.
    The cleaned data is then saved to a new CSV file.
    
    """

    df = pd.read_csv('/opt/airflow/dags/P2M3_gifhari_data_raw.csv')

    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ', '_') 
    df.columns = df.columns.str.replace(r'[^\w\s]', '')

    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)

    df.to_csv('/opt/airflow/dags/P2M3_gifhari_data_clean.csv')

def upload_to_elasticsearch():
    """
    Uploads cleaned data from a CSV file to an Elasticsearch index.

    This function reads cleaned data from a specified CSV file and uploads each row 
    as a document to an Elasticsearch index. The Elasticsearch connection details are 
    hardcoded within the function.
    
    """
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_gifhari_data_clean.csv')

    for i , r in df.iterrows():
        doc = r.to_dict()
        res=es.index(index="table_m3",id = i+1 , body = doc)
        print(f"Response from Elasticsearch: {res}")

default_args = {
    'owner':'Gifhari',
    'start_date':datetime(2023, 12, 24, 12, 00)

} 

with DAG(
    "P2M3_gifharii_DAG_hck",
    description = 'Milestone_3',
    schedule_interval = '30 6 * * *',
    default_args = default_args,
    catchup = False,
) as dag:
# Task : 1
    load_csv_task = PythonOperator(
        task_id = 'load_csv_to_postgres',
        python_callable = load_csv_to_postgres) 
    
#task: 2
    ambil_data_pg = PythonOperator(
        task_id = 'ambil_data_postgres',
        python_callable = ambil_data)
# task: 3
    edit_data = PythonOperator(
        task_id = 'edit_data',
        python_callable = cleaning)
# task:4
    upload_data = PythonOperator(
        task_id = 'upload_data_elastic',
        python_callable = upload_to_elasticsearch)
    
    load_csv_task >> ambil_data_pg >> edit_data >> upload_data

    """
    This structure provides a clear and organized workflow for your data pipeline, making it easier to follow and maintain.
    Each function is well-documented with docstrings, explaining their purpose, parameters, and return values.
    """
    

    
    








              
