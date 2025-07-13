import spotipy
from spotipy.oauth2 import SpotifyOAuth
from typing import Dict, List, Optional
import json
from datetime import datetime
from ..utils.config import settings
from ..utils.logger import get_logger

logger = get_logger(__name__)


class SpotifyClient:
    def __init__(self):
        self.auth_manager = SpotifyOAuth(
            client_id=settings.spotify_client_id,
            redirect_uri=settings.spotify_redirect_uri,
            client_secret=settings.spotify_client_secret,
            scope=" ".join(settings.spotify_scopes),
            cache_path="/opt/airflow/.spotify_cache",
            open_browser=False,
            show_dialog=False,
        )

        try:
            token_info = self.auth_manager.get_cached_token()
            if not token_info:
                logger.error(
                    "No cached token found! Please authenticate locally first."
                )
                raise Exception(
                    "No cached Spotify token. Run authentication locally first."
                )
        except Exception as e:
            logger.error(f"Token validation failed: {str(e)}")
            raise

        self.client = spotipy.Spotify(auth_manager=self.auth_manager)
        logger.info("Spotify client initialized successfully")

    def get_recently_played(self, limit: int = 50) -> Optional[Dict]:
        # Users recently played songs

        try:
            logger.info(f"Fetching {limit} recently played tracks")
            results = self.client.current_user_recently_played(limit=limit)
            if results:
                logger.success(f"successfully fetched {len(results['items'])} tracks")
                return results
            else:
                logger.warning("No recently played tracks found")
                return None
        except Exception as e:
            logger.error(f"Error fetching recently played tracks: {str(e)}")
            raise

    def get_top_tracks(
        self, time_range: str = "medium_term", limit: int = 50
    ) -> Optional[Dict]:
        # Users top tracks

        try:
            logger.info(f"Fetching top {limit} tracks for {time_range}")
            results = self.client.current_user_top_tracks(
                time_range=time_range, limit=limit
            )
            if results:
                logger.success(
                    f"Successfully fetched {len(results['items'])} top tracks"
                )
                return results
            else:
                logger.warning("No top tracks found")
                return None
        except Exception as e:
            logger.error(f"Error fetching top tracks: {str(e)}")
            raise

    def get_top_artists(
        self, limit: int = 50, time_range: str = "medium_term"
    ) -> Optional[Dict]:
        # Users top artists
        try:
            logger.info(f"Fetching top {limit} artists for {time_range}")
            results = self.client.current_user_top_artists(
                time_range=time_range, limit=limit
            )
            if results:
                logger.success(
                    f"Successfully fetched {len(results['items'])} top artists"
                )
                return results
            else:
                logger.warning("No top artists found")
                return None
        except Exception as e:
            logger.error(f"Error fetching top artists: {str(e)}")
            raise
