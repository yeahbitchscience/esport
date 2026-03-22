"""
Liquipedia client for scraping esports ground-truth data.

Uses the public MediaWiki API (action=parse) with proper rate limiting
and caching. All data is cached in SQLite with a configurable TTL.
"""

import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz

import config
from database import Database
from logger import log


@dataclass
class MatchInfo:
    """A match found on Liquipedia."""
    team_a: str = ""
    team_b: str = ""
    scheduled_time: Optional[datetime] = None
    tournament: str = ""
    game: str = ""
    stage: str = ""
    format: str = ""  # e.g. Bo3, Bo5
    is_completed: bool = False
    score_a: str = ""
    score_b: str = ""


@dataclass
class TeamInfo:
    """Team information from Liquipedia."""
    name: str = ""
    game: str = ""
    region: str = ""
    status: str = "active"  # active, disbanded, renamed, inactive
    renamed_to: str = ""  # If renamed, what's the new name
    org: str = ""
    roster: List[str] = field(default_factory=list)
    coach: str = ""
    aliases: List[str] = field(default_factory=list)
    page_exists: bool = True


class LiquipediaClient:
    """Client for scraping Liquipedia via MediaWiki API."""

    def __init__(self, db: Database):
        self.db = db
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": config.LIQUIPEDIA_USER_AGENT,
            "Accept-Encoding": "gzip",
        })
        self._last_request_time = 0.0
        self._last_parse_time = 0.0

    def _rate_limit(self, is_parse: bool = False):
        """Enforce rate limits per Liquipedia policy."""
        now = time.time()
        if is_parse:
            elapsed = now - self._last_parse_time
            if elapsed < config.LIQUIPEDIA_PARSE_RATE_LIMIT:
                wait = config.LIQUIPEDIA_PARSE_RATE_LIMIT - elapsed
                log.debug(f"Liquipedia parse rate limit: waiting {wait:.1f}s")
                time.sleep(wait)
            self._last_parse_time = time.time()
        else:
            elapsed = now - self._last_request_time
            if elapsed < config.LIQUIPEDIA_RATE_LIMIT:
                wait = config.LIQUIPEDIA_RATE_LIMIT - elapsed
                time.sleep(wait)
            self._last_request_time = time.time()

    def _get_api_url(self, game: str) -> str:
        """Get the API URL for a specific game's wiki."""
        wiki = config.LIQUIPEDIA_WIKIS.get(game.lower(), game.lower())
        return f"{config.LIQUIPEDIA_BASE}/{wiki}{config.LIQUIPEDIA_API_SUFFIX}"

    def _api_request(self, game: str, params: Dict, is_parse: bool = False) -> Optional[Dict]:
        """Make a rate-limited API request."""
        self._rate_limit(is_parse)
        url = self._get_api_url(game)

        for attempt in range(config.MAX_RETRIES):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                try:
                    resp.raise_for_status()
                    return resp.json()
                finally:
                    resp.close()
            except requests.exceptions.RequestException as e:
                wait = config.RETRY_BACKOFF_BASE ** (attempt + 1)
                log.warning(f"Liquipedia API failed (attempt {attempt + 1}): {e}")
                if attempt < config.MAX_RETRIES - 1:
                    time.sleep(wait)
                else:
                    log.error(f"Liquipedia API failed after {config.MAX_RETRIES} attempts")
                    return None

    def _parse_page(self, game: str, page_title: str) -> Optional[str]:
        """Fetch and parse a wiki page, returning HTML content."""
        cache_key = f"lp_parse:{game}:{page_title}"
        cached = self.db.get_cache(cache_key)
        if cached:
            return cached

        data = self._api_request(game, {
            "action": "parse",
            "page": page_title,
            "format": "json",
            "prop": "text|categories",
        }, is_parse=True)

        if not data or "parse" not in data:
            return None

        html = data["parse"].get("text", {}).get("*", "")
        self.db.set_cache(cache_key, html)
        return html

    def _search_pages(self, game: str, query: str, limit: int = 5) -> List[Dict]:
        """Search for pages on Liquipedia."""
        cache_key = f"lp_search:{game}:{query}"
        cached = self.db.get_cache(cache_key)
        if cached:
            return cached

        self._rate_limit()
        data = self._api_request(game, {
            "action": "opensearch",
            "search": query,
            "limit": limit,
            "format": "json",
        })

        if not data or not isinstance(data, list) or len(data) < 2:
            return []

        results = [{"title": title} for title in data[1]]
        self.db.set_cache(cache_key, results)
        return results

    # ─── Public Methods ─────────────────────────────────────────────────

    def get_upcoming_matches(self, game: str, tournament: str = "") -> List[MatchInfo]:
        """
        Get upcoming/scheduled matches from Liquipedia.
        Scrapes the Matches page or tournament page for upcoming matches.
        """
        cache_key = f"lp_matches:{game}:{tournament}"
        cached = self.db.get_cache(cache_key)
        if cached:
            return [self._dict_to_match(m) for m in cached]

        matches = []

        # Try tournament-specific page first
        if tournament:
            tournament_matches = self._scrape_tournament_matches(game, tournament)
            matches.extend(tournament_matches)

        # Also get from the general upcoming matches page
        general_page = "Liquipedia:Upcoming_and_ongoing_matches"
        html = self._parse_page(game, general_page)
        if html:
            parsed = self._parse_matches_html(html, game)
            matches.extend(parsed)

        # Deduplicate
        seen = set()
        unique = []
        for m in matches:
            key = f"{m.team_a}|{m.team_b}|{m.scheduled_time}"
            if key not in seen:
                seen.add(key)
                unique.append(m)

        # Cache as dicts
        self.db.set_cache(cache_key, [self._match_to_dict(m) for m in unique])
        log.info(f"Found {len(unique)} matches for {game}" +
                 (f" / {tournament}" if tournament else ""))
        return unique

    def get_team_info(self, game: str, team_name: str) -> TeamInfo:
        """
        Get team information from Liquipedia.
        Checks team page for status, roster, org, and aliases.
        """
        cache_key = f"lp_team:{game}:{team_name}"
        cached = self.db.get_cache(cache_key)
        if cached:
            return self._dict_to_team(cached)

        info = TeamInfo(name=team_name, game=game)

        # Try direct page lookup
        html = self._parse_page(game, team_name)
        if not html:
            # Try searching
            results = self._search_pages(game, team_name)
            if results:
                # Try first result
                html = self._parse_page(game, results[0]["title"])
                if html:
                    info.name = results[0]["title"]

        if not html:
            info.page_exists = False
            self.db.set_cache(cache_key, self._team_to_dict(info))
            return info

        soup = BeautifulSoup(html, "html.parser")

        # Check for redirect / rename indicators
        info = self._parse_team_page(soup, info)

        self.db.set_cache(cache_key, self._team_to_dict(info))
        return info

    def search_team(self, game: str, query: str) -> List[str]:
        """Fuzzy search for a team name on Liquipedia. Returns matching page titles."""
        results = self._search_pages(game, query, limit=10)
        return [r["title"] for r in results]

    def get_tournament_matches(self, game: str, tournament_slug: str) -> List[MatchInfo]:
        """Get all matches from a specific tournament page."""
        cache_key = f"lp_tournament:{game}:{tournament_slug}"
        cached = self.db.get_cache(cache_key)
        if cached:
            return [self._dict_to_match(m) for m in cached]

        matches = self._scrape_tournament_matches(game, tournament_slug)
        self.db.set_cache(cache_key, [self._match_to_dict(m) for m in matches])
        return matches

    def check_team_exists(self, game: str, team_name: str) -> bool:
        """Quick check if a team page exists on Liquipedia."""
        info = self.get_team_info(game, team_name)
        return info.page_exists

    def get_team_roster(self, game: str, team_name: str) -> List[str]:
        """Get the current roster for a team."""
        info = self.get_team_info(game, team_name)
        return info.roster

    def fuzzy_match_team(self, game: str, team_name: str) -> Optional[str]:
        """
        Try to find the best Liquipedia match for a team name.
        Returns the matched page title or None.
        """
        # Direct lookup first
        info = self.get_team_info(game, team_name)
        if info.page_exists:
            return info.name

        # Search and fuzzy match
        results = self.search_team(game, team_name)
        if not results:
            return None

        best_score = 0
        best_match = None
        for result in results:
            score = fuzz.ratio(team_name.lower(), result.lower())
            token_score = fuzz.token_sort_ratio(team_name.lower(), result.lower())
            final_score = max(score, token_score)
            if final_score > best_score:
                best_score = final_score
                best_match = result

        if best_score >= config.FUZZY_MATCH_THRESHOLD:
            return best_match
        return None

    # ─── Private Parsing Methods ────────────────────────────────────────

    def _scrape_tournament_matches(self, game: str, tournament_slug: str) -> List[MatchInfo]:
        """Scrape matches from a tournament page."""
        html = self._parse_page(game, tournament_slug)
        if not html:
            # Try searching for the tournament
            results = self._search_pages(game, tournament_slug)
            for result in results:
                html = self._parse_page(game, result["title"])
                if html:
                    break

        if not html:
            return []

        return self._parse_matches_html(html, game, tournament=tournament_slug)

    def _parse_matches_html(self, html: str, game: str, tournament: str = "") -> List[MatchInfo]:
        """Parse match information from HTML content."""
        soup = BeautifulSoup(html, "html.parser")
        matches = []

        # Look for match containers — Liquipedia uses various templates
        # Common class patterns: match-row, bracket-game, matchlist
        match_containers = soup.find_all(
            class_=re.compile(r'match-row|bracket-game|matchlistslot|match2', re.IGNORECASE)
        )

        for container in match_containers:
            match = self._parse_single_match(container, game, tournament)
            if match and match.team_a and match.team_b:
                matches.append(match)

        # Also try table-based matches
        if not matches:
            tables = soup.find_all("table", class_=re.compile(r'match|bracket', re.IGNORECASE))
            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    match = self._parse_table_match(row, game, tournament)
                    if match and match.team_a and match.team_b:
                        matches.append(match)

        # Fallback: look for "vs" patterns in text
        if not matches:
            matches = self._parse_vs_patterns(soup, game, tournament)

        return matches

    def _parse_single_match(self, container, game: str, tournament: str) -> MatchInfo:
        """Parse a single match from a container element."""
        match = MatchInfo(game=game, tournament=tournament)

        # Extract team names — look for team name spans/links
        team_elements = container.find_all(
            class_=re.compile(r'team-template-text|team-left|team-right|teamname', re.IGNORECASE)
        )
        teams = []
        for el in team_elements:
            # Get the text from links or direct text
            link = el.find("a")
            name = link.get_text(strip=True) if link else el.get_text(strip=True)
            if name and len(name) > 1:
                teams.append(name)

        if len(teams) >= 2:
            match.team_a = teams[0]
            match.team_b = teams[1]
        elif len(teams) == 1:
            match.team_a = teams[0]

        # Extract time
        timer = container.find(class_=re.compile(r'timer-object|match-countdown', re.IGNORECASE))
        if timer:
            timestamp = timer.get("data-timestamp")
            if timestamp:
                try:
                    match.scheduled_time = datetime.fromtimestamp(
                        int(timestamp), tz=timezone.utc
                    )
                except (ValueError, TypeError, OSError):
                    pass

        # Extract score
        scores = container.find_all(class_=re.compile(r'score|bracket-score', re.IGNORECASE))
        if len(scores) >= 2:
            match.score_a = scores[0].get_text(strip=True)
            match.score_b = scores[1].get_text(strip=True)
            if match.score_a and match.score_b and match.score_a != "-":
                match.is_completed = True

        # Extract format (Bo3, Bo5, etc.)
        format_el = container.find(class_=re.compile(r'bestof|match-header', re.IGNORECASE))
        if format_el:
            match.format = format_el.get_text(strip=True)

        return match

    def _parse_table_match(self, row, game: str, tournament: str) -> Optional[MatchInfo]:
        """Parse a match from a table row."""
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            return None

        match = MatchInfo(game=game, tournament=tournament)

        # Look for team links
        links = row.find_all("a")
        team_names = []
        for link in links:
            href = link.get("href", "")
            text = link.get_text(strip=True)
            # Filter out non-team links
            if (text and len(text) > 1 and
                not text.startswith("http") and
                ":" not in text and
                not text.lower().startswith(("edit", "view", "talk"))):
                team_names.append(text)

        if len(team_names) >= 2:
            match.team_a = team_names[0]
            match.team_b = team_names[1]
            return match

        return None

    def _parse_vs_patterns(self, soup: BeautifulSoup, game: str, tournament: str) -> List[MatchInfo]:
        """Fallback: look for 'Team A vs Team B' text patterns."""
        matches = []
        text = soup.get_text()
        vs_pattern = re.compile(
            r'([A-Z][A-Za-z0-9\s\.\-]+?)\s+vs\.?\s+([A-Z][A-Za-z0-9\s\.\-]+?)(?:\n|\r|$)',
            re.MULTILINE
        )
        for m in vs_pattern.finditer(text):
            team_a = m.group(1).strip()
            team_b = m.group(2).strip()
            if len(team_a) > 1 and len(team_b) > 1:
                matches.append(MatchInfo(
                    team_a=team_a,
                    team_b=team_b,
                    game=game,
                    tournament=tournament,
                ))
        return matches

    def _parse_team_page(self, soup: BeautifulSoup, info: TeamInfo) -> TeamInfo:
        """Parse team status, roster, and metadata from a team page."""
        text = soup.get_text()
        text_lower = text.lower()

        # Check for disbanded/inactive indicators
        if any(indicator in text_lower for indicator in [
            "disbanded", "this team has disbanded", "team is disbanded",
            "no longer active", "organization has closed",
        ]):
            info.status = "disbanded"

        # Check for rename indicators
        rename_patterns = [
            r'renamed\s+to\s+(.+?)(?:\.|,|\n)',
            r'now\s+(?:known\s+)?as\s+(.+?)(?:\.|,|\n)',
            r'rebranded\s+(?:to|as)\s+(.+?)(?:\.|,|\n)',
        ]
        for pattern in rename_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                info.status = "renamed"
                info.renamed_to = match.group(1).strip()
                break

        # Check for redirect notice
        redirect_div = soup.find(class_=re.compile(r'redirectMsg|redirect', re.IGNORECASE))
        if redirect_div:
            link = redirect_div.find("a")
            if link:
                info.status = "renamed"
                info.renamed_to = link.get_text(strip=True)

        # Extract roster — look for player cards or roster tables
        roster = []

        # Method 1: Player cards
        player_elements = soup.find_all(class_=re.compile(
            r'roster-card|player|wikitable-roster|block-player', re.IGNORECASE
        ))
        for el in player_elements:
            # Look for player ID/name
            name_el = el.find(class_=re.compile(r'name|ID|player-name', re.IGNORECASE))
            if name_el:
                name = name_el.get_text(strip=True)
                if name and len(name) > 1:
                    roster.append(name)

        # Method 2: Player links in roster section
        if not roster:
            roster_section = soup.find(id=re.compile(r'roster|Active', re.IGNORECASE))
            if roster_section:
                parent = roster_section.find_parent()
                if parent:
                    links = parent.find_all("a")
                    for link in links:
                        text = link.get_text(strip=True)
                        if text and len(text) > 1 and not text.startswith(("[", "{")):
                            roster.append(text)

        info.roster = roster[:20]  # Cap at 20 to avoid noise

        # Extract org / parent organization
        org_el = soup.find(class_=re.compile(r'infobox-header|team-template-team', re.IGNORECASE))
        if org_el:
            info.org = org_el.get_text(strip=True)

        return info

    # ─── Serialization ──────────────────────────────────────────────────

    @staticmethod
    def _match_to_dict(m: MatchInfo) -> Dict:
        return {
            "team_a": m.team_a,
            "team_b": m.team_b,
            "scheduled_time": m.scheduled_time.isoformat() if m.scheduled_time else None,
            "tournament": m.tournament,
            "game": m.game,
            "stage": m.stage,
            "format": m.format,
            "is_completed": m.is_completed,
            "score_a": m.score_a,
            "score_b": m.score_b,
        }

    @staticmethod
    def _dict_to_match(d: Dict) -> MatchInfo:
        st = d.get("scheduled_time")
        if st and isinstance(st, str):
            try:
                st = datetime.fromisoformat(st)
            except ValueError:
                st = None
        return MatchInfo(
            team_a=d.get("team_a", ""),
            team_b=d.get("team_b", ""),
            scheduled_time=st,
            tournament=d.get("tournament", ""),
            game=d.get("game", ""),
            stage=d.get("stage", ""),
            format=d.get("format", ""),
            is_completed=d.get("is_completed", False),
            score_a=d.get("score_a", ""),
            score_b=d.get("score_b", ""),
        )

    @staticmethod
    def _team_to_dict(t: TeamInfo) -> Dict:
        return {
            "name": t.name,
            "game": t.game,
            "region": t.region,
            "status": t.status,
            "renamed_to": t.renamed_to,
            "org": t.org,
            "roster": t.roster,
            "coach": t.coach,
            "aliases": t.aliases,
            "page_exists": t.page_exists,
        }

    @staticmethod
    def _dict_to_team(d: Dict) -> TeamInfo:
        return TeamInfo(
            name=d.get("name", ""),
            game=d.get("game", ""),
            region=d.get("region", ""),
            status=d.get("status", "active"),
            renamed_to=d.get("renamed_to", ""),
            org=d.get("org", ""),
            roster=d.get("roster", []),
            coach=d.get("coach", ""),
            aliases=d.get("aliases", []),
            page_exists=d.get("page_exists", True),
        )
