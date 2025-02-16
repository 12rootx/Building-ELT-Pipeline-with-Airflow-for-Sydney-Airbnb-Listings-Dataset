#########################################################
#
#   00. Import Libraries
#
#########################################################

import os
import logging
import requests
import pandas as pd
import shutil
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


#########################################################
#
#   01. DAG Settings
#
#########################################################

# Define the DAG arguments
dag_default_args = {
    'owner': 'rootx',
    'start_date': datetime(2020, 5, 1),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

# Define the DAG 
dag = DAG(
    dag_id='bde-airbnb',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   02. Load Environment Variables
#
#########################################################

airbnb_path = "/home/airflow/gcs/data/airbnb"
nsw_lga_path = airbnb_path + "/NSW_LGA/"
census_lga_path = airbnb_path + "/Census_LGA/"
listings_path = airbnb_path + "/listings/"


#########################################################
#
#   03. Custom Logics for Operator
#
#########################################################

# 3.1 Load RAW_NSW_LGA_CODE data

def import_load_raw_lga_code_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    lga_code_file_path = nsw_lga_path + 'NSW_LGA_CODE.csv'
    if not os.path.exists(lga_code_file_path):
        logging.info("No NSW_LGA_CODE.csv file found.")
        return None
   
    # Generate dataframe by reading the CSV file
    df = pd.read_csv(lga_code_file_path)

    if len(df) > 0:
        col_names = ', '.join(df.columns)
        values = df.to_dict('split')['data']
        # Cast each value to a string
        values = [[str(value) for value in row] for row in values]

        insert_sql = f"""
                    INSERT INTO bronze.raw_nsw_lga_code({col_names})
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
        logging.info(f"Executed DataFrame shape: {df.shape}")

        # Move the processed file to the archive folder
        archive_folder = os.path.join(nsw_lga_path, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(lga_code_file_path, os.path.join(archive_folder, 'NSW_LGA_CODE.csv'))
    else:
        logging.info("No data found in NSW_LGA_CODE.csv.")
    return None


# 3.2 Load RAW_NSW_LGA_SUBURB data

def import_load_raw_lga_suburb_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    lga_suburb_file_path = nsw_lga_path + 'NSW_LGA_SUBURB.csv'
    if not os.path.exists(lga_suburb_file_path):
        logging.info("No NSW_LGA_SUBURB.csv file found.")
        return None
   
    # Generate dataframe by reading the CSV file
    df = pd.read_csv(lga_suburb_file_path)

    if len(df) > 0:
        col_names = ['LGA_NAME', 'SUBURB_NAME']
        values = df[col_names].to_dict('split')['data']
         # Cast each value to a string
        values = [[str(value) for value in row] for row in values]

        insert_sql = f"""
                    INSERT INTO bronze.raw_nsw_lga_suburb(lga_name, suburb_name)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
        logging.info(f"Executed DataFrame shape: {df.shape}")

        # Move the processed file to the archive folder
        archive_folder = os.path.join(nsw_lga_path, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(lga_suburb_file_path, os.path.join(archive_folder, 'NSW_LGA_SUBURB.csv'))
    else:
        logging.info("No data found in NSW_LGA_SUBURB.csv.")
    return None


# 3.3 Load RAW_2016CENSUS_G01_NSW_LGA data

def import_load_raw_census_lga_g01_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    census_lga_g01_file_path = census_lga_path + '2016Census_G01_NSW_LGA.csv'
    if not os.path.exists(census_lga_g01_file_path):
        logging.info("No 2016Census_G01_NSW_LGA.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(census_lga_g01_file_path)

    if len(df) > 0:
        col_names = ', '.join(df.columns)
        values = df.to_dict('split')['data']
         # Cast each value to a string
        values = [[str(value) for value in row] for row in values]

        insert_sql = f"""
                    INSERT INTO bronze.raw_2016census_g01_nsw_lga({col_names})
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
        logging.info(f"Executed DataFrame shape: {df.shape}")

        # Move the processed file to the archive folder
        archive_folder = os.path.join(census_lga_path, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(census_lga_g01_file_path, os.path.join(archive_folder, '2016Census_G01_NSW_LGA.csv'))
    else:
        logging.info("No data found in 2016Census_G01_NSW_LGA.csv.")
    return None


# 3.4 Load RAW_2016CENSUS_G012_NSW_LGA data

def import_load_raw_census_lga_g02_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    census_lga_g02_file_path = census_lga_path + '2016Census_G02_NSW_LGA.csv'
    if not os.path.exists(census_lga_g02_file_path):
        logging.info("No 2016Census_G02_NSW_LGA.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(census_lga_g02_file_path)

    if len(df) > 0:
        col_names = ', '.join(df.columns)
        values = df.to_dict('split')['data']
         # Cast each value to a string
        values = [[str(value) for value in row] for row in values]

        insert_sql = f"""
                    INSERT INTO bronze.raw_2016census_g02_nsw_lga({col_names})
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
        logging.info(f"Executed DataFrame shape: {df.shape}")

        # Move the processed file to the archive folder
        archive_folder = os.path.join(census_lga_path, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(census_lga_g02_file_path, os.path.join(archive_folder, '2016Census_G02_NSW_LGA.csv'))
    else:
        logging.info("No data found in 2016Census_G02_NSW_LGA.csv.")
    return None



# 3.5 Load RAW_LISTINGS data

def import_load_raw_listings_func(year, month, **kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    file_name = f'{month:02d}_{year}.csv'
    listings_file_path = listings_path + file_name
    if not os.path.exists(listings_file_path):
        logging.info(f"No {file_name} file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(listings_file_path)

    if len(df) > 0:
        col_names = ', '.join(df.columns)
        values = df.to_dict('split')['data']
         # Cast each value to a string
        values = [[str(value) for value in row] for row in values]

        insert_sql = f"""
                    INSERT INTO bronze.raw_listings({col_names})
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
        logging.info(f"Executed DataFrame shape: {df.shape}")

        # Move the processed file to the archive folder
        archive_folder = os.path.join(listings_path, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(listings_file_path, os.path.join(archive_folder, file_name))
    return None

'''
# origin code only for 05_2020.csv
def import_load_raw_listings_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    listings_file_path = listings_path + '05_2020.csv'
    if not os.path.exists(listings_file_path):
        logging.info("No 05_2020.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(listings_file_path)

    if len(df) > 0:
        col_names = ', '.join(df.columns)
        values = df.to_dict('split')['data']
         # Cast each value to a string
        values = [[str(value) for value in row] for row in values]

        insert_sql = f"""
                    INSERT INTO bronze.raw_listings({col_names})
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
        logging.info(f"Executed DataFrame shape: {df.shape}")

        # Move the processed file to the archive folder
        archive_folder = os.path.join(listings_path, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(listings_file_path, os.path.join(archive_folder, '05_2020.csv'))
    return None
'''

#########################################################
#
#   04. Function to trigger dbt Cloud Job
#
#########################################################

def trigger_dbt_cloud_job(**kwargs):

    # Get the dbt Cloud URL, account ID, and job ID from Airflow Variables
    dbt_cloud_url = Variable.get("DBT_CLOUD_URL")
    dbt_cloud_account_id = Variable.get("DBT_CLOUD_ACCOUNT_ID")
    dbt_cloud_job_id = Variable.get("DBT_CLOUD_JOB_ID")
    
    # Define the URL for the dbt Cloud job API dynamically using URL, account ID, and job ID
    url = f"https://{dbt_cloud_url}/api/v2/accounts/{dbt_cloud_account_id}/jobs/{dbt_cloud_job_id}/run/"
    
    # Get the dbt Cloud API token from Airflow Variables
    dbt_cloud_token = Variable.get("DBT_CLOUD_API_TOKEN")
    
    # Define the headers and body for the request
    headers = {
        'Authorization': f'Token {dbt_cloud_token}',
        'Content-Type': 'application/json'
    }
    data = {
        "cause": "Triggered via API"
    }
    
    # Make the POST request to trigger the dbt Cloud job
    response = requests.post(url, headers=headers, json=data)
    
    # Check if the response is successful
    if response.status_code == 200:
        logging.info("Successfully triggered dbt Cloud job.")
        return response.json()
    else:
        logging.error(f"Failed to trigger dbt Cloud job: {response.status_code}, {response.text}")
        raise AirflowException("Failed to trigger dbt Cloud job.")


#########################################################
#
#   05. DAG Operator Setup
#
#########################################################

import_load_raw_lga_code_task = PythonOperator(
    task_id="import_load_raw_lga_code",
    python_callable=import_load_raw_lga_code_func,
    provide_context=True,
    dag=dag
)


import_load_raw_lga_suburb_task = PythonOperator(
    task_id="import_load_raw_lga_suburb",
    python_callable=import_load_raw_lga_suburb_func,
    provide_context=True,
    dag=dag
)


import_load_raw_census_lga_g01_task = PythonOperator(
    task_id="import_load_raw_census_lga_g01",
    python_callable=import_load_raw_census_lga_g01_func,
    provide_context=True,
    dag=dag
)


import_load_raw_census_lga_g02_task = PythonOperator(
    task_id="import_load_raw_census_lga_g02",
    python_callable=import_load_raw_census_lga_g02_func,
    provide_context=True,
    dag=dag
)


# Generate all possible months and years in chronological order
years_months = [(2020, month) for month in range(5, 13)] + \
                [(2021, month) for month in range(1, 5)]

# Keep track of the previous task for dependency chaining
previous_task = None  
task_cnt = 0

# Create a loop to iterate tasks by month
for year, month in years_months:

    # Create task to load data by month
    import_load_raw_listings_task = PythonOperator(
    task_id=f'import_load_raw_listings_{year}_{month:02d}',
    python_callable=import_load_raw_listings_func,
    op_kwargs={'year': year, 'month': month},
    provide_context=True,
    execution_timeout=timedelta(minutes=15),
    dag=dag
    )
    
    # Create task to call dbt job after the loading
    trigger_dbt_job_task = PythonOperator(
    task_id=f'trigger_dbt_job_{year}_{month:02d}',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    execution_timeout=timedelta(minutes=15),
    dag=dag
    )

    # Chain the reference tasks before loading monthly data 
    if task_cnt<1:
        [import_load_raw_lga_code_task, import_load_raw_lga_suburb_task, \
         import_load_raw_census_lga_g01_task, import_load_raw_census_lga_g02_task] \
            >> import_load_raw_listings_task
    task_cnt += 1

    # Chain the dependencies only when the previous task has been successfully executed
    if previous_task is not None:
        previous_task >> import_load_raw_listings_task

    # Set the current listings task to trigger the dbt job
    import_load_raw_listings_task >> trigger_dbt_job_task

    # Update the previous task to be the current month's dbt job task for the next loop iteration
    previous_task = trigger_dbt_job_task

