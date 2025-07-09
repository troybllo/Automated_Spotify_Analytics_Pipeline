import pandas as pd
from typing import Dict, List
from datetime import datetime, time
import json
from pathlib import Path
from pandas.io.common import file_path_to_url
from spotipy import Spotify
from sqlalchemy.sql.functions import now
from app.extractors.spotify_client import SpotifyClient
from app.utils.logger import get_logger
from ..utils.config import settings

logger = get_logger(__name__)


class SpotifyDataExtractor:
    def __init__(self):
        self.client = SpotifyClient()
        self.data_dir = settings.data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def extract_recently_played(self) -> pd.DataFrame:
        data = self.client.get_recently_played(limit=50)

        if not data:
            logger.warning(f"No recently played songs data available")
            return pd.DataFrame()

        tracks = []
        for item in data["items"]:
            track = item["track"]
            tracks.append(
                {
                    "played_at": item["played_at"],
                    "track_id": track["id"],
                    "track_name": track["name"],
                    "artist_ids": ",".join(
                        [artist["id"] for artist in track["artists"]]
                    ),
                    "artist_names": ",".join(
                        [artist["name"] for artist in track["artists"]]
                    ),
                    "album_id": track["album"]["id"],
                    "album_name": track["album"]["name"],
                    "duration_ms": track["duration_ms"],
                    "popularity": track["popularity"],
                    "explicit": track["explicit"],
                    "extracted_at": datetime.now().isoformat(),
                }
            )

        df = pd.DataFrame(tracks)

        filename = f"recently_played_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = self.data_dir / filename
        df.to_csv(filepath, index=False)
        logger.success(f"Saved {len(df)} recently played tracks to {filename}")

        return df

    def extract_top_tracks(self) -> Dict[str, pd.DataFrame]:
        time_ranges = ["short_term", "medium_term", "long_term"]
        all_tracks = {}

        for time in time_ranges:
            data = self.client.get_top_tracks(time_range=time)

            if not data:
                logger.warning(f"No top tracks data available for {time}")
                continue

            tracks = []
            for idx, track in enumerate(data["items"]):
                tracks.append(
                    {
                        "rank": idx + 1,
                        "time_range": time,
                        "track_id": track["id"],
                        "track_name": track["name"],
                        "artist_ids": ",".join(
                            [artist["id"] for artist in track["artists"]]
                        ),
                        "artist_names": ",".join(
                            [artist["name"] for artist in track["artists"]]
                        ),
                        "album_id": track["album"]["id"],
                        "album_name": track["album"]["name"],
                        "duration_ms": track["duration_ms"],
                        "popularity": track["popularity"],
                        "explicit": track["explicit"],
                        "extracted_at": datetime.now().isoformat(),
                    }
                )

            df = pd.DataFrame(tracks)
            all_tracks[time] = df

            filename = f"top_tracks_{time_ranges}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            filepath = self.data_dir / filename
            df.to_csv(filepath, index=False)
            logger.success(f"Saved {len(df)} top tracks ({time}) to {filename}")

        return all_tracks

    def extract_top_artists(self) -> Dict[str, pd.DataFrame]:
        logger.info("Starting extraction of tp artists")

        time_ranges = ["short_term", "medium_term", "long_term"]
        all_artists = {}

        for time in time_ranges:
            data = self.client.get_top_artists(time_range=time)

            if not data:
                logger.warning(f"No top artists data available for {time}")
                continue

            artists = []
            for idx, artist in enumerate(data["items"]):
                artists.append(
                    {
                        "rank": idx + 1,
                        "time_range": time,
                        "artist_id": artist["name"],
                        "genres": ",".join(artist["genres"]),
                        "popularity": artist["popularity"],
                        "followers": artist["followers"]["total"],
                        "extracted_at": datetime.now().isoformat(),
                    }
                )

            df = pd.DataFrame(artists)
            all_artists[time] = df

            filename = (
                f"top_artists{time}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            filepath = self.data_dir / filename
            df.to_csv(filepath, index=False)
            logger.success(f"Saved {len(df)} top artists ({time}) to {filename}")

        return all_artists

    def run_full_extraction(self):
        logger.info("Starting full data extraction")

        recently_played_df = self.extract_recently_played()
        top_tracks_dfs = self.extract_top_tracks()
        self.extract_top_artists()

        all_track_ids = set()
        all_track_ids.update(recently_played_df["track_id"].unique())
        for df in top_tracks_dfs.values():
            all_track_ids.update(df["track_id"].unique())

        logger.success("Full extraction completed successfully")
