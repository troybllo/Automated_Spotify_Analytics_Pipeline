import sys
from loguru import logger
from .config import settings

logger.remove()

logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level=settings.log_level,
    colorize=True,
)

# Add file logger for production
if settings.app_env == "production":
    logger.add(
        "logs/spotify_analytics_{time}.log",
        rotation="1 day",
        retention="7 days",
        level="INFO",
    )


def get_logger(name: str):
    """Get logger instance with custom name"""
    return logger.bind(name=name)
