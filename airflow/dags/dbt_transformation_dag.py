from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'spotify-analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'dbt_spotify_transformations',
    default_args=default_args,
    description='Run dbt transformations on Spotify data',
    schedule_interval='@daily',
    catchup=False,
    tags=['spotify', 'dbt'],
)

# Run dbt deps
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command='cd /opt/airflow && /usr/local/bin/docker-compose run --rm dbt deps',
    dag=dag,
)

# Run dbt models  
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow && /usr/local/bin/docker-compose run --rm dbt run',
    dag=dag,
)

# Test dbt models
dbt_test = BashOperator(
    task_id='dbt_test', 
    bash_command='cd /opt/airflow && /usr/local/bin/docker-compose run --rm dbt test',
    dag=dag,
)

# Generate docs
dbt_docs = BashOperator(
    task_id='dbt_docs_generate',
    bash_command='cd /opt/airflow && /usr/local/bin/docker-compose run --rm dbt docs generate',
    dag=dag,
)

# Set dependencies
dbt_deps >> dbt_run >> dbt_test >> dbt_docs
