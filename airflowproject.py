from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

#2.1 Define DAG Arguments

default_args {
    'owner': 'owner',  
    'start_date': datetime.now(),
    'email': ['owner@email.com'],  
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#2.2 Define DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',  
)

#3.1 Unzip Data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command=(
        'tar -xzvf /home/project/airflow/dags/finalassignment/tolldata.tgz '
        '-C /home/project/airflow/dags/finalassignment/staging'
    ),
    dag=dag,
)

#3.2 Extract Data from CSV 
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=(
        "cut -d, -f1,2,3,4 "
        "/home/project/airflow/dags/finalassignment/staging/vehicle-data.csv"
        "> /home/project/airflow/dags/finalassignment/staging/csv_data.csv"
    ),
    dag=dag,
)

#3.3 Extract Data from TSV
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=(
        "cut -f5,6,7 "
        "/home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv "
        "> /home/project/airflow/dags/finalassignment/staging/tsv_data.csv"
    ),
    dag=dag,
)

#3.4 Extract Data from Fixed Width
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=(
        "awk '{print substr($0, 11, 6), substr($0, 20, 6)}' "
        "/home/project/airflow/dags/finalassignment/staging/payment-data.txt "
        "> /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv"
    ),
    dag=dag,
)

#3.5 Consolidate Data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=(
        "paste -d, "
        "/home/project/airflow/dags/finalassignment/staging/csv_data.csv"
        "/home/project/airflow/dags/finalassignment/staging/tsv_data.csv"
        "/home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv"
        "> /home/project/airflow/dags/finalassignment/staging/extracted_data.csv"
    ),
    dag=dag,
)

#3.6 Transform Data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=(
        "awk -F, '{OFS=\",\"; $4=toupper($4); print}' "
        "/home/project/airflow/dags/finalassignment/staging/extracted_data.csv "
        "> /home/project/airflow/dags/finalassignment/staging/transformed_data.csv"
    ),
    dag=dag,
)


#3.7 Task Pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv \
>> extract_data_from_fixed_width >> consolidate_data \
>> transform_data