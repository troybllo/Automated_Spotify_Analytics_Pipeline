import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
import pandas as pd
from datetime import datetime
import glob
from app.utils.database import db
from app.utils.logger import get_logger
from app.utils.config import settings

logger = get_logger(__name__)


class DataLoader:
    def __init__(self):
        self.data_dir = settings.data_dir

    def load_recently_played(self):
        pattern = str(self.data_dir / "recently_played_*.csv")
        files = glob.glob(pattern)

        if not files:
            logger.warning("No recently_played files found")
            return

        for file in files:
            logger.info(f"Loading {file}")
            df = pd.read_csv(file)

            df["explicit"] = df["explicit"].astype(bool)

            df["played_at"] = pd.to_datetime(df["played_at"])
            df["extracted_at"] = pd.to_datetime(df["extracted_at"])

            db.load_dataframe(df, "recently_played", schema="raw_data")

            logger.success(f"Loaded {len(df)} rows from {Path(file).name}")

    def load_top_tracks(self):
        """Load top tracks"""
        pattern = str(self.data_dir / "top_tracks_*.csv")
        files = glob.glob(pattern)

        if not files:
            logger.warning("No top_tracks files found")
            return

        for file in files:
            logger.info(f"Loading {file}")
            df = pd.read_csv(file)

            # Convert boolean and timestamps
            df["explicit"] = df["explicit"].astype(bool)
            df["extracted_at"] = pd.to_datetime(df["extracted_at"])

            # Load to database
            db.load_dataframe(df, "top_tracks", schema="raw_data")

            logger.success(f"Loaded {len(df)} rows from {Path(file).name}")

    def load_top_artists(self):
        """Load top artists"""
        pattern = str(self.data_dir / "top_artists_*.csv")
        files = glob.glob(pattern)

        if not files:
            logger.warning("No top_artists files found")
            return

        for file in files:
            logger.info(f"Loading {file}")
            df = pd.read_csv(file)

            # Convert timestamps
            df["extracted_at"] = pd.to_datetime(df["extracted_at"])

            # Load to database
            db.load_dataframe(df, "top_artists", schema="raw_data")

            logger.success(f"Loaded {len(df)} rows from {Path(file).name}")

    def load_all(self):
        """Load all CSV files to database"""
        logger.info("Starting data load process")

        self.load_recently_played()
        self.load_top_tracks()
        self.load_top_artists()

        logger.success("Data load process completed!")

    def check_loaded_data(self):
        """Print summary of loaded data"""
        logger.info("Checking loaded data...")

        tables = ["recently_played", "top_tracks", "top_artists"]

        for table in tables:
            query = f"SELECT COUNT(*) as count FROM raw_data.{table}"
            result = db.execute_query(query)
            count = result.scalar()
            logger.info(f"{table}: {count} rows")


def main():
    """Main function"""
    loader = DataLoader()

    # Load all data
    loader.load_all()

    # Check what was loaded
    loader.check_loaded_data()


if __name__ == "__main__":
    main()
