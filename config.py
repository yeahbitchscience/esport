"""
Central configuration for the Polymarket Esports Anomaly Bot.
All settings are loaded from environment variables with sensible defaults.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─── Polling ────────────────────────────────────────────────────────────────
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))  # 5 min

# ─── Discord ────────────────────────────────────────────────────────────────
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
DISCORD_ERROR_WEBHOOK_URL = os.getenv("DISCORD_ERROR_WEBHOOK_URL", DISCORD_WEBHOOK_URL)

# ─── Alerting Thresholds ────────────────────────────────────────────────────
ALERT_SCORE_THRESHOLD = int(os.getenv("ALERT_SCORE_THRESHOLD", "40"))
DEDUP_HOURS = int(os.getenv("DEDUP_HOURS", "24"))
CHEAP_SIDE_THRESHOLD = float(os.getenv("CHEAP_SIDE_THRESHOLD", "0.10"))
TIME_MISMATCH_HOURS = float(os.getenv("TIME_MISMATCH_HOURS", "1.0"))

# ─── Caching ────────────────────────────────────────────────────────────────
LIQUIPEDIA_CACHE_TTL = int(os.getenv("LIQUIPEDIA_CACHE_TTL", "21600"))  # 6 hours
TOURNAMENT_REFRESH_INTERVAL = int(os.getenv("TOURNAMENT_REFRESH_INTERVAL", "21600"))

# ─── Database ───────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "data/bot.db")

# ─── Logging ────────────────────────────────────────────────────────────────
LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))  # 10 MB
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))

# ─── Data Files ─────────────────────────────────────────────────────────────
DATA_DIR = os.getenv("DATA_DIR", "data")
TEAM_ALIASES_FILE = os.path.join(DATA_DIR, "team_aliases.json")
DISBANDED_TEAMS_FILE = os.path.join(DATA_DIR, "disbanded_teams.json")
ORG_AFFILIATES_FILE = os.path.join(DATA_DIR, "org_affiliates.json")

# ─── Polymarket Gamma API ───────────────────────────────────────────────────
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
POLYMARKET_BASE_URL = "https://polymarket.com"
GAMMA_EVENTS_ENDPOINT = f"{GAMMA_API_BASE}/events"
GAMMA_MARKETS_ENDPOINT = f"{GAMMA_API_BASE}/markets"
GAMMA_TAGS_ENDPOINT = f"{GAMMA_API_BASE}/tags"

# Tags to scan — covers all major esports categories on Polymarket
ESPORTS_TAGS = [
    "esports",
    "gaming",
    "cs2", "counter-strike",
    "valorant",
    "league-of-legends", "lol",
    "dota-2", "dota2",
    "call-of-duty", "cod",
    "overwatch", "ow",
]

# ─── Liquipedia ─────────────────────────────────────────────────────────────
LIQUIPEDIA_BASE = "https://liquipedia.net"
LIQUIPEDIA_API_SUFFIX = "/api.php"

# Game → Liquipedia wiki prefix
LIQUIPEDIA_WIKIS = {
    "cs2": "counterstrike",
    "counter-strike": "counterstrike",
    "valorant": "valorant",
    "league-of-legends": "leagueoflegends",
    "lol": "leagueoflegends",
    "dota-2": "dota2",
    "dota2": "dota2",
    "call-of-duty": "callofduty",
    "cod": "callofduty",
    "overwatch": "overwatch",
    "ow": "overwatch",
    "rocket-league": "rocketleague",
    "apex-legends": "apexlegends",
    "rainbow-six": "rainbowsix",
}

LIQUIPEDIA_RATE_LIMIT = 2.0  # seconds between general requests
LIQUIPEDIA_PARSE_RATE_LIMIT = 30.0  # seconds between parse requests
LIQUIPEDIA_USER_AGENT = os.getenv(
    "LIQUIPEDIA_USER_AGENT",
    "PolymarketEsportsBot/1.0 (contact: esportsbot@example.com)"
)

# ─── Retry Logic ────────────────────────────────────────────────────────────
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds, exponential
CONSECUTIVE_FAILURE_ALERT_THRESHOLD = 3

# ─── Scoring Weights ────────────────────────────────────────────────────────
SCORE_BOOSTER_REPEAT_OFFENDER = 2.0
SCORE_BOOSTER_LIQUIPEDIA_DRIFT = 1.5
SCORE_BOOSTER_BOTH = 2.5
SCORE_MAX = 100

RECOMMENDATION_BUY = "BUY_CHEAP_SIDE"
RECOMMENDATION_MONITOR = "MONITOR"
RECOMMENDATION_INVESTIGATE = "INVESTIGATE"

BUY_THRESHOLD = 70
MONITOR_THRESHOLD = 40

# ─── Fuzzy Matching ─────────────────────────────────────────────────────────
FUZZY_MATCH_THRESHOLD = 80  # fuzzywuzzy score 0-100
