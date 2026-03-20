# Polymarket Esports Anomaly Bot

A production-ready bot that scans Polymarket esports betting markets for anomalies — wrong teams, renamed teams, disbanded teams, impossible matches, and more. When it finds something suspicious, it calculates a severity score and sends a Discord alert.

## Why This Works

Polymarket sources team data from Liquipedia but fills market cards manually. This creates opportunities where markets list teams that don't exist, have been renamed, or are playing different opponents. Per Polymarket's January 2026 rules, any cancelled or non-existent match resolves 50/50. Buying the cheap side at 3–10 cents can return up to 17x.

**Real example:** Polymarket listed "Telluride Bush Gaming vs Las Vegas Falcons" for a Call of Duty match. Reality: Las Vegas Falcons was renamed Riyadh Falcons a year prior. The real match was Telluride Bush vs Falcons Academy Green. Market resolved 50/50. Cheap side paid 17x.

## Quick Start

### 1. Install

```bash
cd e:\esport
pip install -r requirements.txt
```

### 2. Configure

Create a `.env` file:

```env
# Required
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/YOUR/WEBHOOK

# Optional overrides (defaults shown)
POLL_INTERVAL_SECONDS=300
ALERT_SCORE_THRESHOLD=40
DEDUP_HOURS=24
CHEAP_SIDE_THRESHOLD=0.10
TIME_MISMATCH_HOURS=1.0
LIQUIPEDIA_CACHE_TTL=21600
LIQUIPEDIA_USER_AGENT=PolymarketEsportsBot/1.0 (contact: you@example.com)
```

### 3. Run

```bash
# Production mode — sends Discord alerts
python main.py

# Dry run — logs anomalies to console, no Discord alerts
python main.py --dry-run
```

## What It Does

Every 5 minutes, the bot:

1. **Polls Polymarket** for all open esports markets (CoD, CS2, LoL, Valorant, Dota2, Overwatch, etc.)
2. **Cross-references Liquipedia** (the ground truth) for each market
3. **Runs 14 anomaly filters** on each market
4. **Scores** each market's anomalies and recommends an action
5. **Sends Discord alerts** for markets above the score threshold

## The 14 Anomaly Filters

| # | Filter | What It Catches | Severity |
|---|--------|-----------------|----------|
| 13 | **REPEAT_OFFENDER** | Same wrong team name appeared in a prior resolved 50/50 market in this tournament | 10 |
| 14 | **LIQUIPEDIA_DRIFT** | Liquipedia shows different teams than Polymarket for this timeslot | 9 |
| 1 | **RENAMED_TEAM** | Team name is an old name that was renamed | 8 |
| 2 | **DISBANDED_TEAM** | Team no longer exists | 8 |
| 11 | **AFFILIATE_CONFUSION** | Org name confused with sub-team (e.g. "Falcons" ≠ "Falcons Academy Green") | 7 |
| 4 | **WRONG_OPPONENT** | Team is playing but vs a different team than listed | 7 |
| 3 | **IMPOSSIBLE_MATCH** | Team is scheduled elsewhere at the same time | 6 |
| 12 | **CROSS_GAME_CONFLICT** | Team scheduled in different game at same time | 6 |
| 5 | **TIME_MISMATCH** | Match time off by >1 hour from Liquipedia | 5 |
| 6 | **WRONG_TOURNAMENT** | Match assigned to wrong tournament | 5 |
| 7 | **ROSTER_MISMATCH** | Player rosters don't match between platforms | 4 |
| 9 | **ALREADY_PLAYED** | Match time passed but market still open | 4 |
| 10 | **DUPLICATE_MARKET** | Same match listed twice with different prices | 3 |
| 8 | **LIQUIDITY_ANOMALY** | Low volume + cheap side — calculates potential multiplier | 3 |

## Scoring

- **Base score** = sum of (severity × weight) for each triggered flag
- **Boosters:**
  - REPEAT_OFFENDER present → score × 2.0
  - LIQUIPEDIA_DRIFT present → score × 1.5
  - Both present → score × 2.5
- **Recommendations:**
  - Score ≥ 70 → `BUY_CHEAP_SIDE`
  - 40–69 → `MONITOR`
  - < 40 → `INVESTIGATE`

## Adding New Team Data

The bot's edge comes from its knowledge base. Update these JSON files in `data/`:

### Add a team rename
Edit `data/team_aliases.json`:
```json
{
    "call-of-duty": {
        "Old Team Name": "New Team Name"
    }
}
```

### Add a disbanded team
Edit `data/disbanded_teams.json`:
```json
{
    "cs2": [
        "Disbanded Team Name"
    ]
}
```

### Add an org/affiliate mapping
Edit `data/org_affiliates.json`:
```json
{
    "Org Name": {
        "main": "Main Team",
        "affiliates": ["Academy Team", "Sub Team"],
        "games": ["cs2", "valorant"]
    }
}
```

> **Hot reload:** The bot automatically reloads these data files every 6 hours. No restart needed after editing.

## Architecture

```
main.py                    → Entry point, crash handling
bot.py                     → Main polling loop (5-min cycles)
polymarket_client.py       → Polymarket Gamma API client
liquipedia_client.py       → Liquipedia MediaWiki API scraper
anomaly_detector.py        → All 14 anomaly filters
scoring.py                 → Severity scoring & recommendations
discord_notifier.py        → Discord webhook alerts + dedup
database.py                → SQLite persistence layer
config.py                  → Environment-based configuration
logger.py                  → Rotating file + console logging
data/
  team_aliases.json        → Old name → current name mappings
  disbanded_teams.json     → Inactive/disbanded teams
  org_affiliates.json      → Org → sub-team mappings
```

## Files Created at Runtime

```
data/bot.db                → SQLite database (market history, alert dedup, cache)
logs/bot.log               → Rotating log file (10 MB, 5 backups)
```

## Reliability

- **Retries** — all API calls retry 3× with exponential backoff
- **Graceful degradation** — if Liquipedia is down, dependent filters skip instead of crash
- **Health alerts** — Discord warning after 3+ consecutive failures
- **Crash alerts** — Discord notification on fatal errors
- **Deduplication** — no duplicate alerts within 24 hours
- **Structured logging** — rotating file handler, 10 MB × 5 backups

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DISCORD_WEBHOOK_URL` | (required) | Discord webhook for anomaly alerts |
| `DISCORD_ERROR_WEBHOOK_URL` | same as above | Separate webhook for health/crash alerts |
| `POLL_INTERVAL_SECONDS` | 300 | Time between polling cycles |
| `ALERT_SCORE_THRESHOLD` | 40 | Minimum score to trigger alert |
| `DEDUP_HOURS` | 24 | Hours before re-alerting same market |
| `CHEAP_SIDE_THRESHOLD` | 0.10 | Max price for "cheap side" flag |
| `TIME_MISMATCH_HOURS` | 1.0 | Hours of time drift to flag |
| `LIQUIPEDIA_CACHE_TTL` | 21600 | Liquipedia cache duration (6h) |
| `LIQUIPEDIA_USER_AGENT` | PolymarketEsportsBot/1.0 | Required by Liquipedia |
| `DB_PATH` | data/bot.db | SQLite database path |
| `LOG_DIR` | logs | Log file directory |
