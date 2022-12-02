### 1. Import packages & dependencies

## 1.1. Import packages
import os
from decouple import config
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

### 2. Connect to biqquery

## 2.1. Define credential (service account's key)
credential_path = os.path.join(os.path.dirname(__file__), '.key', 'sa_key.json')
credential = service_account.Credentials.from_service_account_file(credential_path)

## 2.2. Construct client object
project_id = config('PROJECT_ID')
client = bigquery.Client(credentials= credential,project=project_id)

### 3. Create dataset

dataset = 'automobile_dataset'
def bq_create_dataset(client, dataset):
    dataset_ref = client.dataset(dataset)

    try:
        dataset = client.get_dataset(dataset_ref)
        print('Dataset {} already exists.'.format(dataset))
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'asia-southeast2'
        dataset = client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))
    return dataset

data = bq_create_dataset(client=client, dataset=dataset)

### 4. Load csv file

## 4.1. Define file path
file_path = Path(os.path.join(os.path.dirname(__file__), 'data'))

## 4.2. Define schema for each file

## 4.3. Defina schema

schema_automobile = [
    ['symboling','NUMERIC','NULLABLE'],
    ['normalized_losses','NUMERIC','NULLABLE'],
    ['make','STRING','NULLABLE'],
    ['fuel_type','STRING','NULLABLE'],
    ['aspiration','STRING','NULLABLE'],
    ['num_of_doors','STRING','NULLABLE'],
    ['body_style','STRING','NULLABLE'],
    ['drive_wheels','STRING','NULLABLE'],
    ['engine_location','STRING','NULLABLE'],
    ['wheel_base','NUMERIC','NULLABLE'],
    ['length','NUMERIC','NULLABLE'],
    ['width','NUMERIC','NULLABLE'],
    ['height','NUMERIC','NULLABLE'],
    ['curb_weight','NUMERIC','NULLABLE'],
    ['engine_type','STRING','NULLABLE'],
    ['num_of_cylinders','STRING','NULLABLE'],
    ['engine_size','NUMERIC','NULLABLE'],
    ['fuel_system','STRING','NULLABLE'],
    ['bore','NUMERIC','NULLABLE'],
    ['stroke','NUMERIC','NULLABLE'],
    ['compression_ratio','NUMERIC','NULLABLE'],
    ['horsepower','NUMERIC','NULLABLE'],
    ['peak_rpm','NUMERIC','NULLABLE'],
    ['city_mpg','NUMERIC','NULLABLE'],
    ['highway_mpg','NUMERIC','NULLABLE'],
    ['price','NUMERIC','NULLABLE']
]


## 4.4. Load file

def bq_create_table(client, dataset, table_name, schema):
    dataset_ref = client.dataset(dataset)

    # Prepares a reference to the table
    table_ref = dataset_ref.table(table_name)

    try:
        table =  client.get_table(table_ref)
        print('table {} already exists.'.format(table))
    except NotFound:
        a = []
        for i in range(len(schema)):
            b = bigquery.SchemaField(schema[i][0], schema[i][1], mode=schema[i][2])
            a.append(b)
        
        
        table = bigquery.Table(table_ref, schema=a)
        table = client.create_table(table)
        
        print('table {} created.'.format(table.table_id))
    return table

def load_job(client, table_ref, csv_file, schema):

    load_job_configuration = bigquery.LoadJobConfig()
    
    a = []
    for i in range(len(schema)):
        b = bigquery.SchemaField(schema[i][0], schema[i][1], mode=schema[i][2])
        a.append(b)
    load_job_configuration.schema = a

    # load_job_configuration.autodetect = True
    load_job_configuration.source_format = bigquery.SourceFormat.CSV
    load_job_configuration.skip_leading_rows = 1
    load_job_configuration.allow_quoted_newlines = True
    load_job_configuration.write_disposition = "WRITE_TRUNCATE"

    with open(csv_file, 'rb') as source_file:
        client.load_table_from_file(
            source_file,
            destination=table_ref,          
            location='asia-southeast2',
            job_config=load_job_configuration
        )

def load_file(file_path, file_name, client, schema, dataset):
    # Create Table
    table_name = file_name[0:-4]
    table = bq_create_table(client, dataset, table_name, schema)
            
    # Load CSV file
    csv_file = file_path.joinpath(file_name)
    table_ref = '{}.{}.{}'.format(table.project, table.dataset_id, table.table_id)
            
    load_job(client=client, table_ref=table_ref, csv_file=csv_file, schema=schema)
    return print('Data loaded for {}.{}.{}'.format(table.project, table.dataset_id, table.table_id))

file_name = 'automobile.csv'
load_file(file_path, file_name, client, schema_automobile, dataset)