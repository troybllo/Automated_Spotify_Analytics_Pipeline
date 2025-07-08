import pandas as pd
from sqlalchemy import create_engine, table, text
from typing import Optional, List, Dict, Literal
import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager
from .config import settings
from .logger import get_logger


logger = get_logger(__name__)


class DatabaseConnection:
    def __init__(self):
        self.connection_string = settings.database_url
        self.engine = None

    def get_engine(self):
        if not self.engine:
            self.engine = create_engine(self.connection_string)
            logger.info("Created database engine")
        return self.engine

    @contextmanager
    def get_connection(self):
        conn = psycopg2.connect(self.connection_string)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {str(e)}")
            raise
        finally:
            conn.close()

    def execute_query(self, query: str, params: Optional[Dict] = None):
        engine = self.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return result

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = "raw_data",
        if_exists: Literal["fail", "replace", "append"] = "append",
    ):
        try:
            logger.info(f"Loading {len(df)} rows to {schema}.{table_name}")
            engine = self.get_engine()

            df.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method="multi",
                chunksize=1000,
            )

            logger.success(f"Successfully loaded data to {schema}.{table_name}")

        except Exception as e:
            logger.error(f"Error loading data to {schema}.{table_name}: {str(e)}")
            raise

    def read_table(self, table_name: str, schema: str = "raw_data") -> pd.DataFrame:
        engine = self.get_engine()
        query = f"SELECT * FROM {schema}.{table_name}"
        return pd.read_sql(query, engine)

    def table_exists(self, table_name: str, schema: str = "raw_data") -> bool:
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = :schema 
            AND table_name = :table 
        )
        """
        result = self.execute_query(query, {"schema": schema, "table": table_name})
        return bool(result.scalar())


db = DatabaseConnection()
