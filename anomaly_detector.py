"""
Anomaly Detector — implements all 14 anomaly filters.

Each filter returns a list of AnomalyFlag objects. Filters degrade
gracefully: if Liquipedia is unreachable, dependent filters log a
warning and skip instead of crashing.
"""

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set

from fuzzywuzzy import fuzz

import config
from database import Database
from liquipedia_client import LiquipediaClient, MatchInfo, TeamInfo
from logger import log
from polymarket_client import MarketInfo


@dataclass
class AnomalyFlag:
    """A single anomaly flag with severity and evidence."""
    flag_type: str          # One of the 14 filter names
    severity: int           # 1-10
    description: str        # Human-readable description
    evidence: str = ""      # Supporting evidence / data


class AnomalyDetector:
    """Runs all 14 anomaly detection filters against a market."""

    def __init__(self, db: Database, liquipedia: LiquipediaClient):
        self.db = db
        self.lp = liquipedia
        self._team_aliases: Dict = {}
        self._disbanded_teams: Dict = {}
        self._org_affiliates: Dict = {}
        self._load_data_files()

    def _load_data_files(self):
        """Load pre-populated team data files."""
        try:
            with open(config.TEAM_ALIASES_FILE, "r", encoding="utf-8") as f:
                self._team_aliases = json.load(f)
            log.info(f"Loaded team aliases from {config.TEAM_ALIASES_FILE}")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            log.warning(f"Could not load team aliases: {e}")
            self._team_aliases = {}

        try:
            with open(config.DISBANDED_TEAMS_FILE, "r", encoding="utf-8") as f:
                self._disbanded_teams = json.load(f)
            log.info(f"Loaded disbanded teams from {config.DISBANDED_TEAMS_FILE}")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            log.warning(f"Could not load disbanded teams: {e}")
            self._disbanded_teams = {}

        try:
            with open(config.ORG_AFFILIATES_FILE, "r", encoding="utf-8") as f:
                self._org_affiliates = json.load(f)
            log.info(f"Loaded org affiliates from {config.ORG_AFFILIATES_FILE}")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            log.warning(f"Could not load org affiliates: {e}")
            self._org_affiliates = {}

    def reload_data_files(self):
        """Hot-reload data files without restarting the bot."""
        self._load_data_files()

    def detect_all(self, market: MarketInfo, all_markets: List[MarketInfo] = None) -> List[AnomalyFlag]:
        """
        Run all 14 anomaly filters against a market.
        Returns a list of AnomalyFlag objects.
        Execution order: REPEAT_OFFENDER first (highest conviction), then by severity.
        """
        flags: List[AnomalyFlag] = []

        if not market.team_a:
            log.debug(f"Skipping market with no teams parsed: {market.question}")
            return flags

        # ── Filter 13: REPEAT_OFFENDER (runs first, highest conviction) ──
        flags.extend(self._check_repeat_offender(market))

        # ── Filter 14: LIQUIPEDIA_DRIFT ──
        flags.extend(self._check_liquipedia_drift(market))

        # ── Filter 1: RENAMED_TEAM ──
        flags.extend(self._check_renamed_team(market))

        # ── Filter 2: DISBANDED_TEAM ──
        flags.extend(self._check_disbanded_team(market))

        # ── Filter 11: AFFILIATE_CONFUSION ──
        flags.extend(self._check_affiliate_confusion(market))

        # ── Filter 4: WRONG_OPPONENT ──
        flags.extend(self._check_wrong_opponent(market))

        # ── Filter 3: IMPOSSIBLE_MATCH ──
        flags.extend(self._check_impossible_match(market))

        # ── Filter 12: CROSS_GAME_CONFLICT ──
        flags.extend(self._check_cross_game_conflict(market))

        # ── Filter 5: TIME_MISMATCH ──
        flags.extend(self._check_time_mismatch(market))

        # ── Filter 6: WRONG_TOURNAMENT ──
        flags.extend(self._check_wrong_tournament(market))

        # ── Filter 7: ROSTER_MISMATCH ──
        flags.extend(self._check_roster_mismatch(market))

        # ── Filter 9: ALREADY_PLAYED ──
        flags.extend(self._check_already_played(market))

        # ── Filter 10: DUPLICATE_MARKET ──
        if all_markets:
            flags.extend(self._check_duplicate_market(market, all_markets))

        # ── Filter 8: LIQUIDITY_ANOMALY ──
        flags.extend(self._check_liquidity_anomaly(market))

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Filter 13: REPEAT_OFFENDER
    # ═════════════════════════════════════════════════════════════════════

    def _check_repeat_offender(self, market: MarketInfo) -> List[AnomalyFlag]:
        """
        Check if the same wrong team name appeared in a prior resolved market
        in the same tournament that resolved 50/50.
        This is the highest conviction signal — weight is doubled.
        """
        flags = []
        if not market.tournament:
            return flags

        try:
            resolved = self.db.get_fifty_fifty_markets_for_tournament(market.tournament)
            if not resolved:
                return flags

            for team_name in [market.team_a, market.team_b]:
                if not team_name:
                    continue
                for rm in resolved:
                    # Check if the same team name appeared in a prior 50/50 market
                    prior_teams = [rm.get("team_a", ""), rm.get("team_b", "")]
                    for pt in prior_teams:
                        if not pt:
                            continue
                        similarity = fuzz.ratio(team_name.lower(), pt.lower())
                        if similarity >= 85:
                            flags.append(AnomalyFlag(
                                flag_type="REPEAT_OFFENDER",
                                severity=10,
                                description=(
                                    f'"{team_name}" appeared in a prior market in '
                                    f'{market.tournament} that resolved 50/50'
                                ),
                                evidence=(
                                    f'Prior market: "{rm.get("question", "")}" — '
                                    f'resolved 50/50 on {rm.get("resolved_at", "unknown")}'
                                ),
                            ))
                            break  # One flag per team name is enough
        except Exception as e:
            log.warning(f"REPEAT_OFFENDER check failed: {e}")

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Filter 14: LIQUIPEDIA_DRIFT
    # ═════════════════════════════════════════════════════════════════════

    def _check_liquipedia_drift(self, market: MarketInfo) -> List[AnomalyFlag]:
        """
        Liquipedia shows different teams than what Polymarket published
        for this timeslot. Critical signal since Polymarket sources from
        Liquipedia but enters manually.
        """
        flags = []
        try:
            lp_matches = self.lp.get_upcoming_matches(market.game, market.tournament)
            if not lp_matches:
                return flags

            # Find matches around the same timeslot
            for lp_match in lp_matches:
                if not self._times_overlap(market.match_time, lp_match.scheduled_time, hours=2):
                    continue

                # Check if either Polymarket team matches a Liquipedia team
                pm_teams = {market.team_a.lower(), market.team_b.lower()} - {""}
                lp_teams = {lp_match.team_a.lower(), lp_match.team_b.lower()} - {""}

                if not pm_teams or not lp_teams:
                    continue

                # At least one team overlaps (same match) but some differ
                overlap = self._fuzzy_set_overlap(pm_teams, lp_teams)

                if overlap == 0 and len(pm_teams) >= 2 and len(lp_teams) >= 2:
                    # Complete mismatch — could be different match slot
                    continue
                elif overlap > 0 and not self._fuzzy_sets_match(pm_teams, lp_teams):
                    # Partial match — one team matches, the other differs
                    pm_diff = self._fuzzy_set_difference(pm_teams, lp_teams)
                    lp_diff = self._fuzzy_set_difference(lp_teams, pm_teams)
                    flags.append(AnomalyFlag(
                        flag_type="LIQUIPEDIA_DRIFT",
                        severity=9,
                        description=(
                            f"Liquipedia shows different teams for this timeslot. "
                            f"Polymarket has {pm_diff}, Liquipedia has {lp_diff}"
                        ),
                        evidence=(
                            f"LP: {lp_match.team_a} vs {lp_match.team_b} | "
                            f"PM: {market.team_a} vs {market.team_b}"
                        ),
                    ))
                    break  # One drift flag is enough

        except Exception as e:
            log.warning(f"LIQUIPEDIA_DRIFT check failed: {e}")

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Filter 1: RENAMED_TEAM
    # ═════════════════════════════════════════════════════════════════════

    def _check_renamed_team(self, market: MarketInfo) -> List[AnomalyFlag]:
        """Check if either team is using an old/deprecated name."""
        flags = []
        game_aliases = self._team_aliases.get(market.game, {})
        # Also check a flattened "all games" view
        all_aliases = {}
        for game, aliases in self._team_aliases.items():
            if game.startswith("_"):
                continue
            if isinstance(aliases, dict):
                all_aliases.update(aliases)

        for team_name in [market.team_a, market.team_b]:
            if not team_name:
                continue

            # Direct match in game-specific aliases
            current_name = game_aliases.get(team_name)
            if not current_name:
                current_name = all_aliases.get(team_name)

            if current_name and current_name != team_name:
                # Verify it's an actual rename (not just a name → same name mapping)
                if "Disbanded" in current_name or "→" in current_name:
                    # This is really a disband, not a rename
                    continue
                flags.append(AnomalyFlag(
                    flag_type="RENAMED_TEAM",
                    severity=8,
                    description=f'"{team_name}" was renamed to "{current_name}"',
                    evidence=f"From team_aliases.json ({market.game})",
                ))
                continue

            # Fuzzy match against aliases
            for old_name, new_name in (game_aliases if isinstance(game_aliases, dict) else {}).items():
                if old_name.startswith("_"):
                    continue
                similarity = fuzz.ratio(team_name.lower(), old_name.lower())
                if similarity >= 85 and new_name != old_name:
                    flags.append(AnomalyFlag(
                        flag_type="RENAMED_TEAM",
                        severity=7,
                        description=(
                            f'"{team_name}" closely matches old name "{old_name}" '
                            f'(now "{new_name}")'
                        ),
                        evidence=f"Fuzzy match score: {similarity}/100",
                    ))
                    break

            # Also check Liquipedia for rename indicators
            try:
                lp_info = self.lp.get_team_info(market.game, team_name)
                if lp_info.status == "renamed" and lp_info.renamed_to:
                    flags.append(AnomalyFlag(
                        flag_type="RENAMED_TEAM",
                        severity=8,
                        description=(
                            f'"{team_name}" was renamed to "{lp_info.renamed_to}" '
                            f'according to Liquipedia'
                        ),
                        evidence="Liquipedia team page redirect/rename indicator",
                    ))
            except Exception as e:
                log.debug(f"Liquipedia rename check failed for {team_name}: {e}")

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Filter 2: DISBANDED_TEAM
    # ═════════════════════════════════════════════════════════════════════

    def _check_disbanded_team(self, market: MarketInfo) -> List[AnomalyFlag]:
        """Check if either team is in the disbanded list."""
        flags = []
        game_disbanded = self._disbanded_teams.get(market.game, [])
        all_disbanded = []
        for game, teams in self._disbanded_teams.items():
            if game.startswith("_"):
                continue
            if isinstance(teams, list):
                all_disbanded.extend(teams)

        for team_name in [market.team_a, market.team_b]:
            if not team_name:
                continue

            # Direct match
            disbanded = False
            for dt in game_disbanded:
                if fuzz.ratio(team_name.lower(), dt.lower()) >= 85:
                    disbanded = True
                    flags.append(AnomalyFlag(
                        flag_type="DISBANDED_TEAM",
                        severity=8,
                        description=f'"{team_name}" is listed as disbanded/inactive',
                        evidence=f"Match in disbanded_teams.json: {dt}",
                    ))
                    break

            if disbanded:
                continue

            # Check all games if not found in game-specific
            for dt in all_disbanded:
                if fuzz.ratio(team_name.lower(), dt.lower()) >= 85:
                    flags.append(AnomalyFlag(
                        flag_type="DISBANDED_TEAM",
                        severity=7,
                        description=f'"{team_name}" matches disbanded team "{dt}"',
                        evidence="Match in disbanded_teams.json (cross-game)",
                    ))
                    break

            # Also check Liquipedia
            try:
                lp_info = self.lp.get_team_info(market.game, team_name)
                if lp_info.status == "disbanded":
                    flags.append(AnomalyFlag(
                        flag_type="DISBANDED_TEAM",
                        severity=8,
                        description=f'"{team_name}" is marked as disbanded on Liquipedia',
                        evidence="Liquipedia team page status: disbanded",
                    ))
                elif not lp_info.page_exists:
                    flags.append(AnomalyFlag(
                        flag_type="DISBANDED_TEAM",
                        severity=5,
                        description=f'"{team_name}" has no Liquipedia page (may not exist)',
                        evidence="No page found on Liquipedia",
                    ))
            except Exception as e:
                log.debug(f"Liquipedia disband check failed for {team_name}: {e}")

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Filter 3: IMPOSSIBLE_MATCH
    # ═════════════════════════════════════════════════════════════════════

    def _check_impossible_match(self, market: MarketInfo) -> List[AnomalyFlag]:
        """Check if team is scheduled elsewhere at the same time."""
        flags = []
        if not market.match_time:
            return flags

        try:
            lp_matches = self.lp.get_upcoming_matches(market.game, market.tournament)
            for team_name in [market.team_a, market.team_b]:
                if not team_name:
                    continue
                for lp_match in lp_matches:
                    if not lp_match.scheduled_time:
                        continue
                    # Check if this team is in a DIFFERENT match at the same time
                    lp_teams = [lp_match.team_a.lower(), lp_match.team_b.lower()]
                    if team_name.lower() not in lp_teams and not any(
                        fuzz.ratio(team_name.lower(), t) >= 80 for t in lp_teams
                    ):
                        continue

                    # Same team found — check if it's the same match or a conflict
                    other_team = (lp_match.team_b if fuzz.ratio(
                        team_name.lower(), lp_match.team_a.lower()
                    ) >= 80 else lp_match.team_a)

                    # Is the opponent different?
                    pm_opponent = (market.team_b if team_name == market.team_a
                                   else market.team_a)
                    if fuzz.ratio(pm_opponent.lower(), other_team.lower()) >= 80:
                        continue  # Same match

                    # Time overlap?
                    if self._times_overlap(market.match_time, lp_match.scheduled_time, hours=2):
                        flags.append(AnomalyFlag(
                            flag_type="IMPOSSIBLE_MATCH",
                            severity=6,
                            description=(
                                f'"{team_name}" is scheduled to play "{other_team}" '
                                f'at the same time on Liquipedia'
                            ),
                            evidence=(
                                f"LP: {lp_match.team_a} vs {lp_match.team_b} at "
                                f"{lp_match.scheduled_time}"
                            ),
                        ))
                        break

        except Exception as e:
            log.warning(f"IMPOSSIBLE_MATCH check failed: {e}")

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Filter 4: WRONG_OPPONENT
    # ═════════════════════════════════════════════════════════════════════

    def _check_wrong_opponent(self, market: MarketInfo) -> List[AnomalyFlag]:
        """
        Check if team A is playing but against a different team B than listed.
        Cross-reference with Liquipedia schedule.
        """
        flags = []
        try:
            lp_matches = self.lp.get_upcoming_matches(market.game, market.tournament)
            for lp_match in lp_matches:
                # Find a match where one team matches but the opponent differs
                for pm_team, pm_opponent in [
                    (market.team_a, market.team_b),
                    (market.team_b, market.team_a),
                ]:
                    if not pm_team:
                        continue

                    pm_team_match = (
                        fuzz.ratio(pm_team.lower(), lp_match.team_a.lower()) >= 80 or
                        fuzz.ratio(pm_team.lower(), lp_match.team_b.lower()) >= 80
                    )
                    if not pm_team_match:
                        continue

                    # This team is playing — who is the real opponent?
                    real_opponent = (
                        lp_match.team_b if fuzz.ratio(
                            pm_team.lower(), lp_match.team_a.lower()
                        ) >= 80 else lp_match.team_a
                    )

                    # Does the Polymarket opponent match the real opponent?
                    if pm_opponent and fuzz.ratio(
                        pm_opponent.lower(), real_opponent.lower()
                    ) < 70:
                        # Time should also be close for this to be valid
                        if (not market.match_time or not lp_match.scheduled_time or
                                self._times_overlap(market.match_time, lp_match.scheduled_time, hours=3)):
                            flags.append(AnomalyFlag(
                                flag_type="WRONG_OPPONENT",
                                severity=7,
                                description=(
                                    f'Polymarket lists "{pm_team}" vs "{pm_opponent}", '
                                    f'but Liquipedia shows "{pm_team}" vs "{real_opponent}"'
                                ),
                                evidence=(
                                    f"LP: {lp_match.team_a} vs {lp_match.team_b}"
                                ),
                            ))
                            return flags  # One wrong opponent is enough

        except Exception as e:
            log.warning(f"WRONG_OPPONENT check failed: {e}")

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Filter 5: TIME_MISMATCH
    # ═════════════════════════════════════════════════════════════════════

    def _check_time_mismatch(self, market: MarketInfo) -> List[AnomalyFlag]:
        """Check if the match time differs by >1hr from Liquipedia."""
        flags = []
        if not market.match_time:
            return flags

        try:
            lp_matches = self.lp.get_upcoming_matches(market.game, market.tournament)
            for lp_match in lp_matches:
                if not lp_match.scheduled_time:
                    continue

                # Check if this is the same match
                if not self._same_match(market, lp_match):
                    continue

                # Calculate time difference
                diff = abs((market.match_time - lp_match.scheduled_time).total_seconds())
                diff_hours = diff / 3600.0

                if diff_hours > config.TIME_MISMATCH_HOURS:
                    flags.append(AnomalyFlag(
                        flag_type="TIME_MISMATCH",
                        severity=5,
                        description=(
                            f"Match time differs by {diff_hours:.1f} hours from Liquipedia"
                        ),
                        evidence=(
                            f"PM: {market.match_time.isoformat()} | "
                            f"LP: {lp_match.scheduled_time.isoformat()}"
                        ),
                    ))
                    break

        except Exception as e:
            log.warning(f"TIME_MISMATCH check failed: {e}")

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Filter 6: WRONG_TOURNAMENT
    # ═════════════════════════════════════════════════════════════════════

    def _check_wrong_tournament(self, market: MarketInfo) -> List[AnomalyFlag]:
        """Check if the match exists but is assigned to the wrong tournament."""
        flags = []
        try:
            # Search for this match across multiple tournaments
            for team_name in [market.team_a, market.team_b]:
                if not team_name:
                    continue

                lp_team = self.lp.fuzzy_match_team(market.game, team_name)
                if not lp_team:
                    continue

                # Search for the team's next matches
                lp_matches = self.lp.get_upcoming_matches(market.game)
                for lp_match in lp_matches:
                    if not self._team_in_match(team_name, lp_match):
                        continue

                    # Same match but different tournament?
                    pm_opponent = (market.team_b if team_name == market.team_a
                                   else market.team_a)
                    lp_opponent = (lp_match.team_b if fuzz.ratio(
                        team_name.lower(), lp_match.team_a.lower()
                    ) >= 80 else lp_match.team_a)

                    if fuzz.ratio(pm_opponent.lower(), lp_opponent.lower()) >= 70:
                        # Same match — check tournament
                        if (lp_match.tournament and market.tournament and
                                fuzz.ratio(
                                    lp_match.tournament.lower(),
                                    market.tournament.lower()
                                ) < 60):
                            flags.append(AnomalyFlag(
                                flag_type="WRONG_TOURNAMENT",
                                severity=5,
                                description=(
                                    f'Match assigned to "{market.tournament}" on Polymarket, '
                                    f'but belongs to "{lp_match.tournament}" on Liquipedia'
                                ),
                                evidence=(
                                    f"LP tournament: {lp_match.tournament}"
                                ),
                            ))
                            return flags

        except Exception as e:
            log.warning(f"WRONG_TOURNAMENT check failed: {e}")

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Filter 7: ROSTER_MISMATCH
    # ═════════════════════════════════════════════════════════════════════

    def _check_roster_mismatch(self, market: MarketInfo) -> List[AnomalyFlag]:
        """
        Compare roster between what Polymarket implies and Liquipedia.
        Catches academy vs main team confusion.
        """
        flags = []
        if not market.description:
            return flags

        try:
            for team_name in [market.team_a, market.team_b]:
                if not team_name:
                    continue

                lp_roster = self.lp.get_team_roster(market.game, team_name)
                if not lp_roster:
                    continue

                # Check if any player names from the description match the roster
                desc_lower = market.description.lower()
                roster_mentions = sum(
                    1 for player in lp_roster
                    if player.lower() in desc_lower
                )

                # If the description mentions players but they don't match the roster
                # Look for player-like names in description
                player_pattern = re.findall(r'\b([A-Z][a-z]*(?:[A-Z][a-z]*)*)\b', market.description)
                if player_pattern and roster_mentions == 0 and len(lp_roster) >= 3:
                    flags.append(AnomalyFlag(
                        flag_type="ROSTER_MISMATCH",
                        severity=4,
                        description=(
                            f'Market description players don\'t match Liquipedia roster '
                            f'for "{team_name}"'
                        ),
                        evidence=(
                            f"LP roster: {', '.join(lp_roster[:5])} | "
                            f"Description mentions different players"
                        ),
                    ))

        except Exception as e:
            log.warning(f"ROSTER_MISMATCH check failed: {e}")

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Filter 8: LIQUIDITY_ANOMALY
    # ═════════════════════════════════════════════════════════════════════

    def _check_liquidity_anomaly(self, market: MarketInfo) -> List[AnomalyFlag]:
        """
        Flag if volume is very low AND cheap side is under threshold.
        Calculate potential multiplier.
        """
        flags = []
        if not market.has_cheap_side:
            return flags

        if market.volume < 1000 and market.cheap_side_price <= config.CHEAP_SIDE_THRESHOLD:
            flags.append(AnomalyFlag(
                flag_type="LIQUIDITY_ANOMALY",
                severity=3,
                description=(
                    f"Low liquidity ({market.volume:.0f} volume) with cheap side at "
                    f"${market.cheap_side_price:.2f} — potential {market.multiplier:.1f}x multiplier"
                ),
                evidence=(
                    f"Volume: ${market.volume:.0f} | Liquidity: ${market.liquidity:.0f} | "
                    f"Cheap side: ${market.cheap_side_price:.2f} | "
                    f"Multiplier: {market.multiplier:.1f}x"
                ),
            ))

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Filter 9: ALREADY_PLAYED
    # ═════════════════════════════════════════════════════════════════════

    def _check_already_played(self, market: MarketInfo) -> List[AnomalyFlag]:
        """Flag if match time is in the past but no result found."""
        flags = []
        if not market.match_time:
            return flags

        now = datetime.now(timezone.utc)
        if market.match_time >= now:
            return flags

        # Match time is in the past — check how long ago
        time_since = now - market.match_time
        hours_ago = time_since.total_seconds() / 3600.0

        if hours_ago > 1:
            severity = 4 if hours_ago < 6 else 6  # Bigger flag if way past
            flags.append(AnomalyFlag(
                flag_type="ALREADY_PLAYED",
                severity=severity,
                description=(
                    f"Match time was {hours_ago:.1f} hours ago but market is still open"
                ),
                evidence=f"Match time: {market.match_time.isoformat()}",
            ))

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Filter 10: DUPLICATE_MARKET
    # ═════════════════════════════════════════════════════════════════════

    def _check_duplicate_market(
        self, market: MarketInfo, all_markets: List[MarketInfo]
    ) -> List[AnomalyFlag]:
        """Check if the same match is listed twice under different names/prices."""
        flags = []
        for other in all_markets:
            if other.market_id == market.market_id:
                continue

            # Check if same teams (possibly in different order)
            teams_match = (
                (self._fuzzy_match(market.team_a, other.team_a) and
                 self._fuzzy_match(market.team_b, other.team_b)) or
                (self._fuzzy_match(market.team_a, other.team_b) and
                 self._fuzzy_match(market.team_b, other.team_a))
            )

            if teams_match and market.game == other.game:
                # Check time overlap
                if (not market.match_time or not other.match_time or
                        self._times_overlap(market.match_time, other.match_time, hours=3)):
                    flags.append(AnomalyFlag(
                        flag_type="DUPLICATE_MARKET",
                        severity=3,
                        description=(
                            f'Possible duplicate: "{other.question}" '
                            f'(market {other.market_id})'
                        ),
                        evidence=(
                            f"Other market prices: {other.outcome_prices} | "
                            f"This market prices: {market.outcome_prices}"
                        ),
                    ))

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Filter 11: AFFILIATE_CONFUSION
    # ═════════════════════════════════════════════════════════════════════

    def _check_affiliate_confusion(self, market: MarketInfo) -> List[AnomalyFlag]:
        """
        Check if an org name is confused with a sub-team/academy team.
        e.g. "Falcons" when Liquipedia shows "Falcons Academy Green".
        """
        flags = []
        for team_name in [market.team_a, market.team_b]:
            if not team_name:
                continue

            for org_name, org_data in self._org_affiliates.items():
                if org_name.startswith("_"):
                    continue
                if not isinstance(org_data, dict):
                    continue

                main_team = org_data.get("main", "")
                affiliates = org_data.get("affiliates", [])
                org_games = org_data.get("games", [])

                # Check if the market's game matches the org's games
                if org_games and market.game not in org_games:
                    continue

                # Is the team name the org name or main team but not specific enough?
                name_lower = team_name.lower()
                org_lower = org_name.lower()
                main_lower = main_team.lower()

                # Check if using org name when should be specific affiliate
                is_org_match = (
                    fuzz.ratio(name_lower, org_lower) >= 80 or
                    fuzz.ratio(name_lower, main_lower) >= 80
                )

                if not is_org_match:
                    continue

                # Check Liquipedia to see if the actual match is with an affiliate
                try:
                    lp_matches = self.lp.get_upcoming_matches(market.game, market.tournament)
                    for lp_match in lp_matches:
                        lp_teams = [lp_match.team_a, lp_match.team_b]
                        for lp_team in lp_teams:
                            for affiliate in affiliates:
                                if fuzz.ratio(lp_team.lower(), affiliate.lower()) >= 80:
                                    flags.append(AnomalyFlag(
                                        flag_type="AFFILIATE_CONFUSION",
                                        severity=7,
                                        description=(
                                            f'Polymarket uses "{team_name}" but Liquipedia '
                                            f'shows "{lp_team}" (affiliate of {org_name})'
                                        ),
                                        evidence=(
                                            f"Main: {main_team} | "
                                            f"Affiliate: {affiliate} | "
                                            f"PM name: {team_name}"
                                        ),
                                    ))
                                    return flags  # One flag is enough
                except Exception as e:
                    log.debug(f"Affiliate LP check failed: {e}")

                # Even without Liquipedia, flag if team is in affiliate list
                for affiliate in affiliates:
                    if fuzz.ratio(name_lower, affiliate.lower()) >= 85:
                        # They're using affiliate name, that's fine
                        break
                else:
                    # Using org name which could be ambiguous
                    if len(affiliates) > 1:
                        flags.append(AnomalyFlag(
                            flag_type="AFFILIATE_CONFUSION",
                            severity=4,
                            description=(
                                f'"{team_name}" matches org "{org_name}" which has '
                                f'{len(affiliates)} sub-teams — could be ambiguous'
                            ),
                            evidence=f"Sub-teams: {', '.join(affiliates[:5])}",
                        ))

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Filter 12: CROSS_GAME_CONFLICT
    # ═════════════════════════════════════════════════════════════════════

    def _check_cross_game_conflict(self, market: MarketInfo) -> List[AnomalyFlag]:
        """Check if team is scheduled in a different game at the same time."""
        flags = []
        if not market.match_time:
            return flags

        try:
            # Check other games for this team
            other_games = [g for g in config.LIQUIPEDIA_WIKIS.keys()
                           if g != market.game and g not in [
                               "counter-strike", "lol", "dota2", "cod", "ow"  # dedup
                           ]]

            for team_name in [market.team_a, market.team_b]:
                if not team_name:
                    continue
                for other_game in other_games[:3]:  # Limit to avoid too many API calls
                    try:
                        lp_matches = self.lp.get_upcoming_matches(other_game)
                        for lp_match in lp_matches:
                            if not self._team_in_match(team_name, lp_match):
                                continue
                            if self._times_overlap(
                                market.match_time, lp_match.scheduled_time, hours=2
                            ):
                                flags.append(AnomalyFlag(
                                    flag_type="CROSS_GAME_CONFLICT",
                                    severity=6,
                                    description=(
                                        f'"{team_name}" is scheduled in {other_game} '
                                        f'at the same time'
                                    ),
                                    evidence=(
                                        f"Other match: {lp_match.team_a} vs {lp_match.team_b} "
                                        f"({other_game}) at {lp_match.scheduled_time}"
                                    ),
                                ))
                                break
                    except Exception:
                        continue

        except Exception as e:
            log.warning(f"CROSS_GAME_CONFLICT check failed: {e}")

        return flags

    # ═════════════════════════════════════════════════════════════════════
    # Utility Methods
    # ═════════════════════════════════════════════════════════════════════

    @staticmethod
    def _times_overlap(
        t1: Optional[datetime], t2: Optional[datetime], hours: float = 2
    ) -> bool:
        """Check if two times are within N hours of each other."""
        if not t1 or not t2:
            return False
        diff = abs((t1 - t2).total_seconds()) / 3600.0
        return diff <= hours

    @staticmethod
    def _fuzzy_match(name1: str, name2: str, threshold: int = None) -> bool:
        """Fuzzy match two team names."""
        threshold = threshold or config.FUZZY_MATCH_THRESHOLD
        if not name1 or not name2:
            return False
        n1, n2 = name1.lower(), name2.lower()
        score = max(
            fuzz.ratio(n1, n2),
            fuzz.token_sort_ratio(n1, n2),
            fuzz.partial_ratio(n1, n2),
            fuzz.token_set_ratio(n1, n2),
        )
        return score >= threshold

    @staticmethod
    def _fuzzy_set_overlap(set1: Set[str], set2: Set[str]) -> int:
        """Count how many items in set1 fuzzy-match items in set2."""
        count = 0
        for s1 in set1:
            for s2 in set2:
                if fuzz.ratio(s1, s2) >= 75:
                    count += 1
                    break
        return count

    @staticmethod
    def _fuzzy_sets_match(set1: Set[str], set2: Set[str]) -> bool:
        """Check if all items in set1 have a fuzzy match in set2."""
        for s1 in set1:
            found = False
            for s2 in set2:
                if fuzz.ratio(s1, s2) >= 75:
                    found = True
                    break
            if not found:
                return False
        return True

    @staticmethod
    def _fuzzy_set_difference(set1: Set[str], set2: Set[str]) -> Set[str]:
        """Items in set1 that don't fuzzy-match anything in set2."""
        diff = set()
        for s1 in set1:
            found = False
            for s2 in set2:
                if fuzz.ratio(s1, s2) >= 75:
                    found = True
                    break
            if not found:
                diff.add(s1)
        return diff

    def _same_match(self, market: MarketInfo, lp_match: MatchInfo) -> bool:
        """Check if a Polymarket market and Liquipedia match are the same match."""
        pm_teams = [market.team_a.lower(), market.team_b.lower()]
        lp_teams = [lp_match.team_a.lower(), lp_match.team_b.lower()]

        # Both teams should match (in any order)
        team_a_match = any(
            fuzz.ratio(pm_teams[0], lt) >= 75 for lt in lp_teams
        )
        team_b_match = any(
            fuzz.ratio(pm_teams[1], lt) >= 75 for lt in lp_teams
        )
        return team_a_match and team_b_match

    def _team_in_match(self, team_name: str, lp_match: MatchInfo) -> bool:
        """Check if a team is a participant in a Liquipedia match."""
        return (
            fuzz.ratio(team_name.lower(), lp_match.team_a.lower()) >= 80 or
            fuzz.ratio(team_name.lower(), lp_match.team_b.lower()) >= 80
        )
