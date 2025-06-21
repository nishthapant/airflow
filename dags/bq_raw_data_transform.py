from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6,20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['np.work.alerts@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False
}


with DAG(
    'bq_raw_data_transform',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'email'],
) as dag:

    run_extraction_model = BashOperator(
        task_id='extract_raw_data',
        bash_command='cd /opt/airflow/dbt && dbt run --select staging.stg_raw_data'
    )

    run_cleaner_model = BashOperator(
        task_id='clean_raw_data',
        bash_command='cd /opt/airflow/dbt && dbt run --select staging.stg_cleaned_data'
    )

    run_validator_model = BashOperator(
        task_id='validate_clean_data',
        bash_command='cd /opt/airflow/dbt && dbt run --select staging.stg_validate_data'
    )

    send_email = EmailOperator(
        task_id="send_success_email",
        to="np.work.alerts@gmail.com",
        subject="AIRFLOW: DBT Raw Data Transformation Completed.",
        html_content="<p>Message: Raw data successfully extracted, cleaned and transformed. Staged for loading.</p>"
    )

run_extraction_model >> run_cleaner_model >> run_validator_model >> send_email