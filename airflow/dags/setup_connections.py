from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Connection
from airflow import settings


def create_spotify_postgres_connection():
    """Create Postgres connection for Spotify database"""
    new_conn = Connection(
        conn_id="spotify_postgres",
        conn_type="postgres",
        host="postgres",
        schema="spotify_analytics",
        login="spotify_user",
        password="spotify_password",
        port=5432,
    )

    session = settings.Session()
    existing = session.query(Connection).filter_by(conn_id="spotify_postgres").first()

    if not existing:
        session.add(new_conn)
        session.commit()
        print("Created spotify_postgres connection")
    else:
        print("Connection spotify_postgres already exists")

    session.close()


dag = DAG(
    "setup_connections",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["setup"],
)

create_conn_task = PythonOperator(
    task_id="create_postgres_connection",
    python_callable=create_spotify_postgres_connection,
    dag=dag,
)
