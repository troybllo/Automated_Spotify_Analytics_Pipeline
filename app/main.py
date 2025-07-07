import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from app.extractors.data_extractor import SpotifyDataExtractor
from app.utils.logger import get_logger
from app.utils.config import settings

logger = get_logger(__name__)


def main():
    logger.info("Starting Spotify Analytics Data Extraction")
    logger.info(f"Environment: {settings.app_env}")
    logger.info(f"Data directory: {settings.data_dir}")

    try:
        extractor = SpotifyDataExtractor()

        extractor.run_full_extraction()

        logger.success("Data extraction completed successfully")

    except KeyboardInterrupt:
        logger.warning("Extraction interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
