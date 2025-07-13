import json
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
from dotenv import load_dotenv

load_dotenv()

print("Generating Spotify token...")

# First generate to current directory
auth_manager = SpotifyOAuth(
    client_id=os.getenv("SPOTIFY_CLIENT_ID"),
    client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
    redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
    scope="user-read-recently-played user-top-read user-library-read user-read-playback-state playlist-read-private playlist-read-collaborative",
    cache_path=".spotify_cache_temp",
)

# Get token
token = auth_manager.get_access_token(as_dict=False)
print("✅ Token generated!")

# Copy to airflow directory
import shutil

try:
    shutil.copy(".spotify_cache_temp", "airflow/.spotify_cache")
    print("✅ Token copied to airflow/.spotify_cache")

    # Verify
    with open("airflow/.spotify_cache", "r") as f:
        cache = json.load(f)
        print(f"Token expires at: {cache.get('expires_at')}")
except Exception as e:
    print(f"Error copying token: {e}")
    print("\nManually copy the token:")
    print("cp .spotify_cache_temp airflow/.spotify_cache")
