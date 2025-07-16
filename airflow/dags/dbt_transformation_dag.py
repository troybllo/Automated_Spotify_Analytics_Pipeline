from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.docker import DockerOperator

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
dbt_deps = DockerOperator(
    task_id='dbt_deps',
    image='spotify_analytics-dbt',
    command='deps',
    docker_url='unix://var/run/docker.sock',
    network_mode='spotify_network',
    dag=dag,
)

# Run dbt models
dbt_run = DockerOperator(
    task_id='dbt_run',
    image='spotify_analytics-dbt',
    command='run',
    docker_url='unix://var/run/docker.sock',
    network_mode='spotify_network',
    dag=dag,
)

# Test dbt models
dbt_test = DockerOperator(
    task_id='dbt_test',
    image='spotify_analytics-dbt',
    command='test',
    docker_url='unix://var/run/docker.sock',
    network_mode='spotify_network',
    dag=dag,
)

# Generate docs
dbt_docs = DockerOperator(
    task_id='dbt_docs_generate',
    image='spotify_analytics-dbt',
    command='docs generate',
    docker_url='unix://var/run/docker.sock',
    network_mode='spotify_network',
    dag=dag,
)

# Set dependencies
dbt_deps >> dbt_run >> dbt_test >> dbt_docs
