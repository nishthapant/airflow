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
    'create_staging_data',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'staging', 'email'],
) as dag:

    create_staging_data = BashOperator(
        task_id='create_staging_data',
        bash_command='cd /opt/airflow/dbt && dbt run --select staging'
    )

    send_email = EmailOperator(
        task_id="send_success_email",
        to="np.work.alerts@gmail.com",
        subject="AIRFLOW: DBT Raw Data Transformation Completed.",
        html_content="<p>Message: Raw data successfully extracted, cleaned and transformed. In staging.</p>"
    )

create_staging_data >> send_email