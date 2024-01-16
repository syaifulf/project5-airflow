from datetime import timedelta, datetime
import subprocess

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from airflow.hooks.postgres_hook import PostgresHook


# If modifying these scopes, delete the file token.json
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly", 
          'https://www.googleapis.com/auth/drive.file', 
          'https://www.googleapis.com/auth/drive.readonly', 
          'https://www.googleapis.com/auth/drive']


def __install_packages(package_list):
    for package in package_list:
        try:
            subprocess.check_call(['pip3', 'install', package])
            print(f'Successfully installed {package}')
        except subprocess.CalledProcessError:
            print(f'Error installing {package}')


def __authenticate():
    import json
    from google.oauth2.credentials import Credentials

    packages_to_install = ['google-api-python-client', 'google-auth-httplib2', 'google-auth', 'google-auth-oauthlib']
    __install_packages(packages_to_install)

    creds = None
    # get token from airflow variable
    token_path = Variable.get(key="dag_transfer_gdrive_to_postgresql_prod")
    json_token = json.loads(token_path)

    if json_token:
        print("Credentials from token", json_token['token'])
        creds = Credentials(
                        token=json_token['token'],
                        refresh_token=json_token['refresh_token'],
                        token_uri=json_token['token_uri'],
                        client_id=json_token['client_id'],
                        client_secret=json_token['client_secret'],
                        scopes=json_token['scopes']
                    )
        print(creds)
    
    return creds


def __get_folders_id(service, folder_name):
    query = f"sharedWithMe=true and name='{folder_name}'"
    results = (
        service.files()
        .list(q=query, pageSize=10, fields="nextPageToken, files(id, name)")
        .execute()
    )
    items = results.get("files", [])
    return items

def __download_file(service, file_id, file_name):
    import io
    from googleapiclient.http import MediaIoBaseDownload

    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    with open(file_name, "wb") as f:
        f.write(fh.read())
    print(f"Downloaded file {file_name} from Google Drive.")

def _check_file_if_exist(ti):
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError

    try:
        creds = __authenticate()
        service = build("drive", "v3", credentials=creds, cache_discovery=False)

        dataset_folder_name = 'dataset'
        dataset_folders = __get_folders_id(service, dataset_folder_name)

        if dataset_folders == []:
            print(f"The '{dataset_folder_name}' files does not exist in 'Shared with me' in folder 'dataset'.")
            return "end"

        # get id of 'dataset' folder
        dataset_folder_id = dataset_folders[0]['id']
        print(f"The '{dataset_folder_name}' folder exists in 'Shared with me'.")
        
        # get files in 'dataset' folder
        dataset_files = (
            service.files()
            .list(q=f"'{dataset_folder_id}' in parents", pageSize=10, fields="nextPageToken, files(id, name)")
            .execute()
        ).get("files", [])
        
        print("dataset_files :", dataset_files)

        # download files in 'dataset' folder
        if dataset_files != []:
            list_name = []
            for file in dataset_files:
                file_name = file['name']
                file_id = file['id']
                __download_file(service, file_id, file_name)
                list_name.append(file_name)

            # push list_name to xcom
            ti.xcom_push(key="dataset_files", value=list_name)
            return "process_csv"
        else:
            print("No files found in 'dataset' folder.")
            return "end"
            
    except HttpError as error:
        print(f"An error occurred: {error}")

def __compose_values(row):
    return f"{row['IP']}_{row['UserAgent']}_{row['Country']}_{row['Languages']}_{row['Interests']}"

def _process_csv(ti):
    import csv

    # pull xcom
    dataset_files = ti.xcom_pull(key="dataset_files", task_ids="check_file_if_exist")

    # Process each file
    for file in dataset_files:
        print("file :", file)
        input_file = file
        output_file = file.split('.')[0] + '_processed.csv'

        with open(input_file, 'r', newline='') as csvfile:
            # Open the output CSV file for writing
            list_processed_name = []
            with open(output_file, 'w', newline='') as output_csvfile:
                # Create CSV reader and writer objects
                reader = csv.DictReader(csvfile)
                fieldnames = reader.fieldnames + ['id']
                writer = csv.DictWriter(output_csvfile, fieldnames=fieldnames)

                # Write the header to the output CSV file
                writer.writeheader()

                # Process each row
                for row in reader:
                    # Compose the new field value
                    new_field_value = __compose_values(row)

                    # Add the new field to the row
                    row['id'] = new_field_value

                    # Write the modified row to the output CSV file
                    writer.writerow(row)
                
                list_processed_name.append(output_file)
            ti.xcom_push(key="list_processed_name", value=list_processed_name)


def __create_ddl():
    import os
    import psycopg2

    # install psycopg2
    packages_to_install = ['psycopg2-binary']
    __install_packages(packages_to_install)
    
    ddl = """
    CREATE TABLE IF NOT EXISTS public.network_data (
        id TEXT PRIMARY KEY,
        IP VARCHAR(15),
        UserAgent TEXT,
        Country VARCHAR(255),
        Languages VARCHAR(255),
        Interests TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    connection = None
    print("Connecting to the PostgreSQL database... os.environ.get('POSTGRES_USER')", os.environ.get('POSTGRES_USER'))
    try:
        connection = psycopg2.connect(
            host='postgres',
            port=5432,
            dbname='airflow',
            user='airflow',
            password='airflow'
        )

        with connection.cursor() as cursor:
            cursor.execute(ddl)
        connection.commit()

        connection.close()

        print("Table creation successful.")
    except Exception as e:
        print(f"Error: {e}")
        raise e
        
def __upsert_data(ti):
    import pandas as pd
    import psycopg2
    from sqlalchemy import create_engine

    db_params = {
        'host':'postgres',
        'port':5432,
        'dbname':'airflow',
        'user':'airflow',
        'password':'airflow'
    }

    list_processed_name = ti.xcom_pull(key='list_processed_name', task_ids="process_csv")

    for file in list_processed_name:
        df = pd.read_csv(file) 
        # conn = psycopg2.connect(**db_params)
        # cur = conn.cursor()
        engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["dbname"]}')
        df.to_sql('network_data', engine, if_exists='replace', index=False) 
        # cur.close()
        # conn.close()


with DAG(
    dag_id='dag_transfers_gdrive_to_postgresql',
    start_date=datetime(2022, 5, 28),
    schedule_interval='00 23 * * *',
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    check_file_if_exist = PythonOperator(
        task_id='check_file_if_exist',
        python_callable=_check_file_if_exist,
        execution_timeout=timedelta(minutes=5),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    process_csv = PythonOperator(
        task_id='process_csv',
        python_callable=_process_csv,
        execution_timeout=timedelta(minutes=20),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
    
    create_ddl = PythonOperator(
        task_id='create_ddl',
        python_callable=__create_ddl,
        execution_timeout=timedelta(minutes=5),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    upsert_data = PythonOperator(
        task_id='upsert_data',
        python_callable=__upsert_data,
        execution_timeout=timedelta(minutes=5),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )


    end_task = EmptyOperator(
        task_id='end'
    )

    start_task >> check_file_if_exist >> [process_csv, end_task]
    process_csv >> create_ddl >> upsert_data >> end_task