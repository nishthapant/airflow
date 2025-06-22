from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['np.work.alerts@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

with DAG(
    'create_data_marts',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'marts', 'email'],
) as dag:

    create_data_marts = BashOperator(
        task_id='create_data_marts',
        bash_command='cd /opt/airflow/dbt && dbt run --select marts'
    )

    send_email = EmailOperator(
        task_id="send_success_email",
        to="np.work.alerts@gmail.com",
        subject="AIRFLOW: Data marts successfully created.",
        html_content="<p>Message: Fact and dimension tables have been successfully created.</p>"
    )

create_data_marts >> send_email