import os

# Superset specific config
ROW_LIMIT = 5000

# Flask App Builder configuration
SECRET_KEY = 'YOUR_SECRET_KEY_HERE'

# Database connection
SQLALCHEMY_DATABASE_URI = 'postgresql://spotify_user:spotify_password@postgres:5432/spotify_analytics'

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

# Set this API key to enable Mapbox visualizations
MAPBOX_API_KEY = ''

# Disable druid for now
DRUID_IS_ACTIVE = False

# Configure which features are enabled
ENABLE_TEMPLATE_PROCESSING = True
ENABLE_PROXY_FIX = True

# Default cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
}

# Theme customization
APP_NAME = "Spotify Analytics"
APP_ICON = "/static/assets/images/superset-logo-horiz.png"

# Disable example dashboards
LOAD_EXAMPLES = False

# Configure time grains
TIME_GRAIN_EXPRESSIONS = {
    None: "{}",
    "PT1S": "DATE_TRUNC('second', {})",
    "PT1M": "DATE_TRUNC('minute', {})",
    "PT1H": "DATE_TRUNC('hour', {})",
    "P1D": "DATE_TRUNC('day', {})",
    "P1W": "DATE_TRUNC('week', {})",
    "P1M": "DATE_TRUNC('month', {})",
    "P3M": "DATE_TRUNC('quarter', {})",
    "P1Y": "DATE_TRUNC('year', {})",
}
