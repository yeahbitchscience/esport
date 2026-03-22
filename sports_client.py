"""
Sports client for scraping traditional sports ground-truth data.

Uses the free, un-authenticated ESPN scoreboard API.
All data is cached in memory with a short TTL to limit outgoing requests.
"""

from dataclasses import dataclass, field
from datetime import datetime
import time
from typing import Dict, List, Optional
from logger import log
import config
from curl_cffi import requests
from liquipedia_client import MatchInfo

class SportsClient:
    """Client for checking traditional sports matches via ESPN API."""

    def __init__(self):
        self.session = requests.Session(impersonate="chrome")
        # In-memory cache for scoreboards. Structure: { game_slug: (timestamp, data) }
        self._cache: Dict[str, tuple[float, List[MatchInfo]]] = {}

    def _fetch_scoreboard(self, game: str) -> List[MatchInfo]:
        """Fetch and parse the scoreboard for a given sport from ESPN."""
        game_lower = game.lower()
        if game_lower not in config.ESPN_ENDPOINTS:
            log.debug(f"Game '{game}' has no mapped ESPN endpoint.")
            return []

        url = config.ESPN_ENDPOINTS[game_lower]

        # Check cache
        if game_lower in self._cache:
            timestamp, cached_matches = self._cache[game_lower]
            if time.time() - timestamp < config.ESPN_CACHE_TTL:
                return cached_matches

        matches = []
        for attempt in range(config.MAX_RETRIES):
            try:
                resp = self.session.get(url, timeout=10)
                try:
                    resp.raise_for_status()
                    data = resp.json()
                    events = data.get("events", [])
                    
                    for event in events:
                        competitions = event.get("competitions", [])
                        if not competitions:
                            continue
                        
                        comp = competitions[0]
                        competitors = comp.get("competitors", [])
                        if len(competitors) < 2:
                            continue
                            
                        # ESPN usually returns Home first, Away second (or vice-versa), we just extract names
                        team1 = competitors[0].get("team", {})
                        team2 = competitors[1].get("team", {})
                        
                        team_a_name = team1.get("displayName", team1.get("name", ""))
                        team_b_name = team2.get("displayName", team2.get("name", ""))
                        
                        # Parse start time
                        date_str = event.get("date")  # e.g., "2024-03-23T00:00Z"
                        scheduled_time = None
                        if date_str:
                            try:
                                scheduled_time = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                            except ValueError:
                                pass
                                
                        matches.append(MatchInfo(
                            team_a=team_a_name,
                            team_b=team_b_name,
                            scheduled_time=scheduled_time,
                            game=game_lower,
                            is_completed=comp.get("status", {}).get("type", {}).get("completed", False)
                        ))
                    
                    self._cache[game_lower] = (time.time(), matches)
                    return matches
                finally:
                    resp.close()
            except Exception as e:
                log.warning(f"ESPN API failed for {game} (attempt {attempt + 1}): {e}")
                time.sleep(config.RETRY_BACKOFF_BASE ** (attempt + 1))
                
        log.error(f"ESPN API failed for {game} after {config.MAX_RETRIES} attempts")
        return []

    def find_match(self, game: str, team1: str, team2: str, fuzzy_threshold: int = config.FUZZY_MATCH_THRESHOLD) -> Optional[MatchInfo]:
        """Find a match in the ESPN scoreboard involving the two teams."""
        from fuzzywuzzy import fuzz
        
        matches = self._fetch_scoreboard(game)
        if not matches:
            return None

        best_match = None
        best_score = 0

        for match in matches:
            # We want to match BOTH team1 and team2 against match.team_a and match.team_b
            # Permutation 1: team1 -> team_a, team2 -> team_b
            s1_a = fuzz.token_sort_ratio(team1.lower(), match.team_a.lower())
            s2_b = fuzz.token_sort_ratio(team2.lower(), match.team_b.lower())
            score1 = (s1_a + s2_b) / 2

            # Permutation 2: team1 -> team_b, team2 -> team_a
            s1_b = fuzz.token_sort_ratio(team1.lower(), match.team_b.lower())
            s2_a = fuzz.token_sort_ratio(team2.lower(), match.team_a.lower())
            score2 = (s1_b + s2_a) / 2

            max_score = max(score1, score2)
            if max_score > best_score and max_score >= fuzzy_threshold:
                best_score = max_score
                best_match = match

        return best_match

    def get_upcoming_matches(self, game: str, tournament: str = None) -> List[MatchInfo]:
        """Polymorphic implementation to return all matches from the scoreboard."""
        return self._fetch_scoreboard(game)
        
    def get_team_info(self, game: str, team_name: str):
        """Traditional sports don't natively disband mid-season in a detectable wiki format."""
        from liquipedia_client import TeamInfo
        # If the team exists in the scoreboard, return an active TeamInfo to bypass disband filters safely.
        matches = self._fetch_scoreboard(game)
        for m in matches:
            if team_name.lower() in m.team_a.lower() or team_name.lower() in m.team_b.lower():
                return TeamInfo(name=team_name, game=game, status="active", page_exists=True)
        return TeamInfo(name=team_name, game=game, status="active", page_exists=False)

    def fuzzy_match_team(self, game: str, team_name: str) -> Optional[str]:
        """Find the exact formatted team name from the scoreboard."""
        from fuzzywuzzy import fuzz
        matches = self._fetch_scoreboard(game)
        best_match = None
        best_score = 0
        for m in matches:
            for team in (m.team_a, m.team_b):
                score = fuzz.token_sort_ratio(team_name.lower(), team.lower())
                if score > best_score and score >= config.FUZZY_MATCH_THRESHOLD:
                    best_score = score
                    best_match = team
        return best_match
        
    def get_team_roster(self, game: str, team_name: str) -> set:
        """ESPN scoreboard doesn't provide full active roster lists natively."""
        return set()
