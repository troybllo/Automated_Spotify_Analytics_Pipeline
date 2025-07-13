import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/airflow")
from app.extractors.data_extractor import SpotifyDataExtractor

default_args = {
    "owner": "spotify_analytics",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 12),
    "email": ["troybello25@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_spotify_data(**context):
    from app.utils.logger import get_logger

    logger = get_logger(__name__)

    try:
        extractor = SpotifyDataExtractor()

        recently_played_df = extractor.extract_recently_played()

        top_tracks_dfs = extractor.extract_top_tracks()

        top_artists_dfs = extractor.extract_top_artists()

        # Collect all unique track IDs
        all_track_ids = set()
        all_track_ids.update(recently_played_df["track_id"].unique())
        for df in top_tracks_dfs.values():
            all_track_ids.update(df["track_id"].unique())

        context["task_instance"].xcom_push(
            key="extraction_timestamp", value=datetime.now().isoformat()
        )

        logger.success("Spotify data extraction completed")
        return True

    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise


def load_to_postgres(**context):
    from app.loader import DataLoader
    from app.utils.logger import get_logger

    logger = get_logger(__name__)

    try:
        loader = DataLoader()
        loader.load_all()

        loader.check_loaded_data()

        logger.success("Data loaded successfully")
        return True
    except Exception as e:
        logger.error(f"Loading failed: {str(e)}")


def validate_data(**context):
    pg_hook = PostgresHook(postgres_conn_id="spotify_postgres")

    recently_played_check = """
        SELECT COUNT(*) as count
        FROM raw_data.recently_played
        WHERE played_at >= CURRENT_DATE - INTERVAL '7 days'
    """

    result = pg_hook.get_first(recently_played_check)
    if result[0] == 0:
        raise ValueError("No recent listening data found!")

    todays_data_check = """
        SELECT COUNT(*) as count 
        FROM raw_data.recently_played 
        WHERE DATE(load_timestamp) = CURRENT_DATE
    """

    todays_result = pg_hook.get_first(todays_data_check)
    print(f"Loaded {todays_result[0]} records today")

    return True


def clean_old_files(**context):
    import glob
    from datetime import datetime, timedelta
    from pathlib import Path

    data_dir = Path("/opt/airflow/data/raw")
    cutoff_date = datetime.now() - timedelta(days=7)

    for file_path in data_dir.glob("*.csv"):
        file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        if file_time < cutoff_date:
            file_path.unlink()
            print(f"removed old file: {file_path.name}")


dag = DAG(
    "spotify_daily_etl",
    default_args=default_args,
    description="Daily Spotify data extraction and loading",
    schedule_interval="@daily",
    catchup=False,
    tags=["spotify", "etl"],
)

# Task 1: Extract data from Spotify API
extract_task = PythonOperator(
    task_id="extract_spotify_data",
    python_callable=extract_spotify_data,
    dag=dag,
)

# Task 2: Load to PostgreSQL
load_task = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    dag=dag,
)

# Task 3: Validate data
validate_task = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    dag=dag,
)

# Task 4: Clean old files
cleanup_task = PythonOperator(
    task_id="clean_old_files",
    python_callable=clean_old_files,
    dag=dag,
)

# Task 5: Data quality checks
with TaskGroup("data_quality_checks", dag=dag) as quality_checks:
    check_duplicates = PostgresOperator(
        task_id="check_duplicates",
        postgres_conn_id="spotify_postgres",
        sql="""
        SELECT COUNT(*) as duplicate_count
        FROM (
            SELECT played_at, track_id, COUNT(*) as cnt
            FROM raw_data.recently_played
            WHERE DATE(played_at) = CURRENT_DATE - INTERVAL '1 day'
            GROUP BY played_at, track_id
            HAVING COUNT(*) > 1
        ) duplicates;
        """,
    )

    check_nulls = PostgresOperator(
        task_id="check_null_values",
        postgres_conn_id="spotify_postgres",
        sql="""
        SELECT 
            SUM(CASE WHEN track_id IS NULL THEN 1 ELSE 0 END) as null_track_ids,
            SUM(CASE WHEN played_at IS NULL THEN 1 ELSE 0 END) as null_played_at
        FROM raw_data.recently_played
        WHERE DATE(played_at) = CURRENT_DATE - INTERVAL '1 day';
        """,
    )

# Set task dependencies
extract_task >> load_task >> validate_task >> quality_checks >> cleanup_task
