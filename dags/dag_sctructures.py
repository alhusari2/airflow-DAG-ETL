from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta 
import pandas as pd
from helpers.class_etl import DataCleaner
import json


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create a function to run the ETL process for KC
def run_kc_etl(**kwargs):
    ti = kwargs['ti']
    service_account_file = "/home/dxxn/airflow/dags/helpers/dag-kinerja-ro-f5ecabd590ff.json"
    spreadsheet_name_kc = "KINERJA KANCA RO JAKARTA 1"
    spreadsheet_name_kcp = "KINERJA_KANCA_RO_Value_Only"

    data_cleaner = DataCleaner(service_account_file, spreadsheet_name_kc, spreadsheet_name_kcp)
    kc_data = data_cleaner.cleaning_data_KC()
    print(kc_data)
    kc_data_json = kc_data.to_json(orient='split')  # Convert to json for XCom
    ti.xcom_push(key='kc_data', value=kc_data_json)

# Create a function to run the ETL process for KCP
def run_kcp_etl(**kwargs):
    ti = kwargs['ti']
    service_account_file = "/home/dxxn/airflow/dags/helpers/dag-kinerja-ro-f5ecabd590ff.json"
    spreadsheet_name_kc = "KINERJA KANCA RO JAKARTA 1"
    spreadsheet_name_kcp = "KINERJA_KANCA_RO_Value_Only"

    data_cleaner = DataCleaner(service_account_file, spreadsheet_name_kc, spreadsheet_name_kcp)
    kcp_data = data_cleaner.cleaning_data_KCP()
    print(kcp_data)
    kcp_data_json = kcp_data.to_json(orient='split')  # Convert to json for XCom
    ti.xcom_push(key='kcp_data', value=kcp_data_json)


# Function to combine the data
def combine_data(ti):
    # Pull the data from XCom
    kc_data_etl = ti.xcom_pull(key='kc_data', task_ids='extract_kc_data')
    kcp_data_etl = ti.xcom_pull(key='kcp_data', task_ids='extract_kcp_data')
    # Ensure the data is not None
    if kc_data_etl is None or kcp_data_etl is None:
        raise ValueError("No data received from XCom for KC or KCP tasks")
    
    #load as json
    kc_data_json = json.loads(kc_data_etl)
    kcp_data_json = json.loads(kcp_data_etl)
    
    # Convert back to DataFrame
    kc_data = pd.DataFrame(kc_data_json['data'], columns=kc_data_json['columns'])
    kcp_data = pd.DataFrame(kcp_data_json['data'], columns=kcp_data_json['columns'])

    if kc_data is not None and kcp_data is not None:
        combined_data = pd.concat([kc_data, kcp_data], ignore_index=True)
        combined_output_path = "/home/dxxn/airflow/dags/combined_data.xlsx"
        combined_data.to_excel(combined_output_path, index=False)
        print(f"Data gabungan berhasil disimpan di: {combined_output_path}")
    else:
        print("Tidak ada data yang bisa digabungkan.")

# Create DAG
with DAG(
    'ETL_Google_Sheet_DAG',
    default_args=default_args,
    description='ETL DAG for processing Google Sheets data',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task to extract KC data
    extract_kc_data_task = PythonOperator(
        task_id='extract_kc_data',
        python_callable=run_kc_etl
    )

    # Task to extract KCP data
    extract_kcp_data_task = PythonOperator(
        task_id='extract_kcp_data',
        python_callable=run_kcp_etl
    )

    # Task to combine data
    combine_data_task = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data
    )
    # Task to send the combined Excel file via email
    send_email_task = EmailOperator(
        task_id='send_email',
        to='alhusari2@gmail.com',  # Replace with your email
        subject='Combined KC and KCP Data',
        html_content='<p>Update combined KC and KCP data.</p>',
        files=["/home/dxxn/airflow/dags/combined_data.xlsx"],  # File path returned by combine_data task
    )

    # Define task dependencies
    extract_kc_data_task >> extract_kcp_data_task >> combine_data_task >> send_email_task