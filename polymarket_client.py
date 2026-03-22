"""
Polymarket Gamma API client.

Fetches open esports markets and resolved market history.
"""

import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from curl_cffi import requests

import config
from logger import log


@dataclass
class MarketInfo:
    """Normalized representation of a Polymarket esports market."""
    market_id: str = ""
    condition_id: str = ""
    slug: str = ""
    question: str = ""
    description: str = ""
    team_a: str = ""
    team_b: str = ""
    game: str = ""
    tournament: str = ""
    match_time: Optional[datetime] = None
    outcomes: List[str] = field(default_factory=list)
    outcome_prices: List[float] = field(default_factory=list)
    volume: float = 0.0
    liquidity: float = 0.0
    url: str = ""
    event_slug: str = ""
    event_title: str = ""
    tags: List[str] = field(default_factory=list)
    active: bool = True
    closed: bool = False
    resolved: bool = False

    @property
    def cheap_side_price(self) -> float:
        """Get the cheapest outcome price."""
        if not self.outcome_prices:
            return 0.0
        return min(self.outcome_prices)

    @property
    def expensive_side_price(self) -> float:
        """Get the most expensive outcome price."""
        if not self.outcome_prices:
            return 0.0
        return max(self.outcome_prices)

    @property
    def multiplier(self) -> float:
        """Potential multiplier if market resolves 50/50 and you bought cheap side."""
        if self.cheap_side_price <= 0:
            return 0.0
        return round(0.5 / self.cheap_side_price, 2)

    @property
    def has_cheap_side(self) -> bool:
        """Check if cheap side is below threshold."""
        return 0 < self.cheap_side_price <= config.CHEAP_SIDE_THRESHOLD

    def to_dict(self) -> Dict:
        return {
            "market_id": self.market_id,
            "condition_id": self.condition_id,
            "slug": self.slug,
            "question": self.question,
            "team_a": self.team_a,
            "team_b": self.team_b,
            "game": self.game,
            "tournament": self.tournament,
            "match_time": self.match_time.isoformat() if self.match_time else None,
            "outcome_prices": self.outcome_prices,
            "volume": self.volume,
            "liquidity": self.liquidity,
            "url": self.url,
            "cheap_side_price": self.cheap_side_price,
            "multiplier": self.multiplier,
        }


class PolymarketClient:
    """Client for the Polymarket Gamma API."""

    def __init__(self):
        self.session = requests.Session(impersonate="chrome")
        self.session.headers.update({
            "Accept": "application/json",
        })

    def _request(self, url: str, params: Dict = None) -> Optional[Dict]:
        """Make a request with retry logic."""
        for attempt in range(config.MAX_RETRIES):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                try:
                    resp.raise_for_status()
                    return resp.json()
                finally:
                    resp.close()
            except Exception as e:
                wait = config.RETRY_BACKOFF_BASE ** (attempt + 1)
                log.warning(
                    f"Polymarket API request failed (attempt {attempt + 1}/{config.MAX_RETRIES}): "
                    f"{e} — retrying in {wait}s"
                )
                if attempt < config.MAX_RETRIES - 1:
                    time.sleep(wait)
                else:
                    log.error(f"Polymarket API request failed after {config.MAX_RETRIES} attempts: {url}")
                    return None

    def _paginate(self, url: str, params: Dict = None, limit: int = 100, max_items: int = 20000) -> List[Dict]:
        """Fetch all pages from a paginated endpoint safely."""
        params = params or {}
        params["limit"] = min(limit, max_items)
        offset = 0
        all_results = []
        seen_item_ids = set()

        while True:
            # Only set offset if we aren't using a cursor yet
            if "cursor" not in params:
                params["offset"] = offset
                
            data = self._request(url, params)
            if data is None:
                break

            # Handle both list and dict responses
            if isinstance(data, list):
                results = data
            elif isinstance(data, dict):
                results = data.get("data", data.get("results", data.get("events", data.get("markets", []))))
                if isinstance(results, dict):
                    results = [results]
            else:
                break

            if not results:
                break

            # Avoid infinite loops if API ignores offset/cursor and returns same page
            new_items_found = False
            for item in results:
                item_id = str(item.get("id", item.get("conditionId", str(item))))
                if item_id not in seen_item_ids:
                    seen_item_ids.add(item_id)
                    all_results.append(item)
                    new_items_found = True

            # If the API gave us items but we've seen them ALL before, break immediately
            if not new_items_found:
                log.debug("Pagination stopped: duplicate page detected (API likely ignores offset).")
                break

            if len(results) < limit:
                break
                
            # Setup next page parameters
            if isinstance(data, dict) and data.get("next_cursor"):
                params["cursor"] = data.get("next_cursor")
                params.pop("offset", None)
            else:
                offset += limit
                
            if len(all_results) >= max_items:
                log.warning(f"Pagination stopped early: reached hard limit of {max_items} items to preserve RAM.")
                break

        return all_results

    def fetch_open_esports_markets(self) -> List[MarketInfo]:
        """Fetch all currently open esports markets across all game tags."""
        log.info("Fetching open esports markets from Polymarket...")
        seen_ids = set()
        all_markets = []

        for tag in config.TARGET_TAGS:
            log.debug(f"Fetching markets with tag: {tag}")

            # Fetch events for this tag
            events = self._paginate(
                config.GAMMA_EVENTS_ENDPOINT,
                {"tag_slug": tag, "active": "true", "closed": "false"},
                limit=100,
                max_items=20000,
            )

            for event in events:
                event_slug = event.get("slug", "")
                event_title = event.get("title", "")
                event_tags = self._extract_tags(event)

                # Each event can have multiple markets
                markets = event.get("markets", [])
                if not markets:
                    # Try fetching markets separately
                    event_id = event.get("id")
                    if event_id:
                        market_data = self._request(
                            config.GAMMA_MARKETS_ENDPOINT,
                            {"event_id": event_id},
                        )
                        if market_data:
                            if isinstance(market_data, list):
                                markets = market_data
                            elif isinstance(market_data, dict):
                                markets = market_data.get("data", [market_data])

                for market in markets:
                    market_id = market.get("id", market.get("conditionId", ""))
                    if not market_id or market_id in seen_ids:
                        continue
                    seen_ids.add(market_id)

                    info = self._parse_market(market, event_slug, event_title, event_tags, tag)
                    if info:
                        all_markets.append(info)

        log.info(f"Found {len(all_markets)} open esports markets")
        return all_markets

    def fetch_resolved_markets_for_tournament(self, tournament_slug: str, game: str = "") -> List[MarketInfo]:
        """Fetch resolved markets for a specific tournament (repeat-offender detection)."""
        log.debug(f"Fetching resolved markets for tournament: {tournament_slug}")
        resolved = []

        # Search by tag and closed status
        params = {
            "active": "false",
            "closed": "true",
            "tag_slug": game if game else "esports",
        }
        events = self._paginate(config.GAMMA_EVENTS_ENDPOINT, params, limit=50, max_items=20000)

        for event in events:
            event_slug = event.get("slug", "")
            event_title = event.get("title", "")
            # Check if this event matches the tournament
            if not self._tournament_matches(event_title, event_slug, tournament_slug):
                continue

            markets = event.get("markets", [])
            for market in markets:
                info = self._parse_market(
                    market, event_slug, event_title,
                    self._extract_tags(event), game
                )
                if info:
                    info.resolved = True
                    # Check if it was 50/50 resolved
                    outcome_prices = info.outcome_prices
                    if outcome_prices and all(
                        abs(p - 0.5) < 0.05 for p in outcome_prices
                    ):
                        resolved.append(info)

        log.debug(f"Found {len(resolved)} resolved markets for {tournament_slug}")
        return resolved

    def _parse_market(
        self,
        market: Dict,
        event_slug: str,
        event_title: str,
        event_tags: List[str],
        search_tag: str,
    ) -> Optional[MarketInfo]:
        """Parse a raw market dict into a MarketInfo object."""
        try:
            question = market.get("question", market.get("title", ""))
            if not question:
                return None

            # Extract team names from question
            team_a, team_b = self._extract_teams(question)

            # Parse outcome prices
            outcome_prices = self._parse_prices(market)
            outcomes = market.get("outcomes", ["Yes", "No"])
            if isinstance(outcomes, str):
                try:
                    import json
                    outcomes = json.loads(outcomes)
                except (json.JSONDecodeError, TypeError):
                    outcomes = [outcomes]

            # Detect game from tags/title
            game = self._detect_game(event_title, event_tags, search_tag)

            # Extract tournament
            tournament = self._extract_tournament(event_title, event_slug)

            # Parse match time
            match_time = self._parse_time(market, event_title)

            # Build URL
            slug = market.get("slug", market.get("id", ""))
            condition_id = market.get("conditionId", market.get("condition_id", ""))
            url = f"{config.POLYMARKET_BASE_URL}/event/{event_slug}" if event_slug else ""

            info = MarketInfo(
                market_id=market.get("id", condition_id),
                condition_id=condition_id,
                slug=slug,
                question=question,
                description=market.get("description", ""),
                team_a=team_a,
                team_b=team_b,
                game=game,
                tournament=tournament,
                match_time=match_time,
                outcomes=outcomes,
                outcome_prices=outcome_prices,
                volume=float(market.get("volume", 0) or 0),
                liquidity=float(market.get("liquidity", 0) or 0),
                url=url,
                event_slug=event_slug,
                event_title=event_title,
                tags=event_tags,
                active=market.get("active", True),
                closed=market.get("closed", False),
            )
            return info

        except Exception as e:
            log.warning(f"Failed to parse market: {e}")
            return None

    @staticmethod
    def _extract_teams(question: str) -> Tuple[str, str]:
        """
        Extract team names from a market question.
        Common formats:
          - "Team A vs Team B"
          - "Team A vs. Team B"
          - "Will Team A beat Team B?"
          - "Team A v Team B - Map 1"
        """
        q = question.strip()
        # Remove trailing punctuation
        q = re.sub(r'[?!.]+$', '', q).strip()

        # Remove map/game/round suffixes
        q = re.sub(r'\s*[-–—]\s*(Map|Game|Round)\s*\d+.*$', '', q, flags=re.IGNORECASE)
        # Remove trailing parenthetical
        q = re.sub(r'\s*\(.*?\)\s*$', '', q)
        # Remove leading "Will"
        q = re.sub(r'^Will\s+', '', q, flags=re.IGNORECASE)

        # Try "vs" split patterns FIRST (before stripping beat/defeat)
        for pattern in [
            r'(.+?)\s+vs\.?\s+(.+)',
            r'(.+?)\s+v\s+(.+)',
            r'(.+?)\s+against\s+(.+)',
        ]:
            match = re.match(pattern, q, re.IGNORECASE)
            if match:
                return match.group(1).strip(), match.group(2).strip()

        # Try "beat/defeat/win against" patterns
        for pattern in [
            r'(.+?)\s+(?:beat|defeat)\s+(.+)',
            r'(.+?)\s+win\s+(?:against|vs\.?)\s+(.+)',
        ]:
            match = re.match(pattern, q, re.IGNORECASE)
            if match:
                return match.group(1).strip(), match.group(2).strip()

        # Fallback: strip trailing verbs and return single team
        q = re.sub(r'\s+(win|beat|defeat).*$', '', q, flags=re.IGNORECASE)
        return q, ""

    @staticmethod
    def _parse_prices(market: Dict) -> List[float]:
        """Parse outcome prices from market data."""
        prices = market.get("outcomePrices", market.get("outcome_prices", []))
        if isinstance(prices, str):
            try:
                import json
                prices = json.loads(prices)
            except (json.JSONDecodeError, TypeError):
                prices = []
        try:
            return [float(p) for p in prices] if prices else []
        except (ValueError, TypeError):
            return []

    @staticmethod
    def _extract_tags(event: Dict) -> List[str]:
        """Extract tag strings from an event object."""
        tags = event.get("tags", [])
        if isinstance(tags, list):
            result = []
            for t in tags:
                if isinstance(t, dict):
                    result.append(t.get("slug", t.get("label", str(t))))
                elif isinstance(t, str):
                    result.append(t)
            return result
        return []

    @staticmethod
    def _detect_game(event_title: str, tags: List[str], search_tag: str) -> str:
        """Detect which esports game a market is for."""
        title_lower = event_title.lower()
        tag_str = " ".join(tags).lower()
        combined = f"{title_lower} {tag_str} {search_tag}"

        game_patterns = {
            "cs2": ["cs2", "counter-strike", "counterstrike", "csgo"],
            "valorant": ["valorant", "vct"],
            "league-of-legends": ["league of legends", "lol", "lck", "lec", "lcs", "lpl", "worlds"],
            "dota2": ["dota", "dota2", "dota 2", "the international"],
            "call-of-duty": ["call of duty", "cod", "cdl", "warzone"],
            "overwatch": ["overwatch", "owl", "owcs"],
            "rocket-league": ["rocket league", "rlcs"],
            "apex-legends": ["apex", "algs"],
            "rainbow-six": ["rainbow six", "r6", "siege"],
        }

        for game, patterns in game_patterns.items():
            if any(p in combined for p in patterns):
                return game

        return search_tag or "esports"

    @staticmethod
    def _extract_tournament(event_title: str, event_slug: str) -> str:
        """Extract tournament name from event title/slug."""
        # Usually the event title IS the tournament context
        # Strip the vs portion if present
        title = re.sub(r'[-–—].*(vs\.?|v\s).*$', '', event_title, flags=re.IGNORECASE).strip()
        return title or event_slug

    @staticmethod
    def _parse_time(market: Dict, event_title: str) -> Optional[datetime]:
        """Parse match time from market data."""
        # Try various time fields
        for field_name in ["end_date_iso", "endDateIso", "game_start_time", "startDate", "end_date"]:
            val = market.get(field_name)
            if val:
                try:
                    if isinstance(val, (int, float)):
                        return datetime.fromtimestamp(val, tz=timezone.utc)
                    dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                    return dt
                except (ValueError, TypeError, OSError):
                    continue
        return None

    @staticmethod
    def _tournament_matches(event_title: str, event_slug: str, tournament_slug: str) -> bool:
        """Check if an event matches a tournament slug/name (fuzzy)."""
        slug_lower = tournament_slug.lower()
        if slug_lower in event_slug.lower():
            return True
        if slug_lower in event_title.lower():
            return True
        # Check individual words
        slug_words = set(re.findall(r'\w+', slug_lower))
        title_words = set(re.findall(r'\w+', event_title.lower()))
        if len(slug_words & title_words) >= len(slug_words) * 0.6:
            return True
        return False
