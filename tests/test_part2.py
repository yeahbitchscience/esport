"""
Rigorous tests — Part 2: All 14 filters, error handling, edge cases.
Mocks Liquipedia client to avoid slow network calls.
"""
import os, sys, json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database import Database
from liquipedia_client import LiquipediaClient, MatchInfo, TeamInfo
from anomaly_detector import AnomalyDetector, AnomalyFlag
from polymarket_client import MarketInfo
from scoring import ScoringEngine, ScoringResult
from discord_notifier import DiscordNotifier

PASS = 0
FAIL = 0
now = datetime.now(timezone.utc)

def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1; print(f"  PASS: {name}")
    else:
        FAIL += 1; print(f"  FAIL: {name} -- {detail}")

# Setup with mocked Liquipedia
db = Database(":memory:")
lp = MagicMock(spec=LiquipediaClient)
# Default: no matches, no team info
lp.get_upcoming_matches.return_value = []
lp.get_team_info.return_value = TeamInfo(page_exists=False, status="unknown")
lp.fuzzy_match_team.return_value = None
lp.get_team_roster.return_value = []

det = AnomalyDetector(db, lp)
scorer = ScoringEngine()

# ====================================================================
print("\n=== FILTER 1: RENAMED_TEAM ===")
# ====================================================================
m = MarketInfo(market_id="r1", team_a="Las Vegas Falcons", team_b="OpTic",
               game="call-of-duty", tournament="CDL")
flags = det._check_renamed_team(m)
check("Detects Las Vegas Falcons rename", any(f.flag_type == "RENAMED_TEAM" for f in flags))

# Liquipedia says renamed
lp.get_team_info.return_value = TeamInfo(page_exists=True, status="renamed", renamed_to="New Name")
m2 = MarketInfo(market_id="r2", team_a="SomeOldTeam", team_b="X", game="cs2")
flags2 = det._check_renamed_team(m2)
check("LP rename detection works", any(f.flag_type == "RENAMED_TEAM" and "Liquipedia" in f.evidence for f in flags2))
lp.get_team_info.return_value = TeamInfo(page_exists=False, status="unknown")  # Reset

# Unknown team
m3 = MarketInfo(market_id="r3", team_a="TotallyNewTeam2026", team_b="X", game="cs2")
flags3 = det._check_renamed_team(m3)
check("Unknown team not falsely flagged as renamed", not any(
    "TotallyNewTeam2026" in f.description and f.flag_type == "RENAMED_TEAM" for f in flags3))

# Empty teams
m4 = MarketInfo(market_id="r4", team_a="", team_b="", game="cs2")
check("Empty teams no crash", det._check_renamed_team(m4) == [])

# ====================================================================
print("\n=== FILTER 2: DISBANDED_TEAM ===")
# ====================================================================
m = MarketInfo(market_id="d1", team_a="North", team_b="Astralis", game="cs2")
flags = det._check_disbanded_team(m)
check("North detected as disbanded", any(f.flag_type == "DISBANDED_TEAM" and "North" in f.description for f in flags))

# Note: mock LP returns page_exists=False by default, so unknown teams
# correctly get a low-severity "no Liquipedia page" flag. That's intended behavior.
lp.get_team_info.return_value = TeamInfo(page_exists=True, status="active")
m2 = MarketInfo(market_id="d2", team_a="ActiveTeam999", team_b="X", game="cs2")
flags_active = det._check_disbanded_team(m2)
check("Active LP team not flagged as disbanded", not any(
    f.flag_type == "DISBANDED_TEAM" and "ActiveTeam999" in f.description for f in flags_active))
lp.get_team_info.return_value = TeamInfo(page_exists=False, status="unknown")  # Reset

# LP says disbanded
lp.get_team_info.return_value = TeamInfo(page_exists=True, status="disbanded")
m3 = MarketInfo(market_id="d3", team_a="SomeTeam", team_b="X", game="valorant")
flags3 = det._check_disbanded_team(m3)
check("LP disbanded detection", any(f.flag_type == "DISBANDED_TEAM" and "Liquipedia" in f.description for f in flags3))

# LP says no page
lp.get_team_info.return_value = TeamInfo(page_exists=False, status="unknown")
m4 = MarketInfo(market_id="d4", team_a="GhostTeam", team_b="X", game="valorant")
flags4 = det._check_disbanded_team(m4)
check("No LP page flagged as possible non-existent", any(f.flag_type == "DISBANDED_TEAM" and "no Liquipedia page" in f.description for f in flags4))
lp.get_team_info.return_value = TeamInfo(page_exists=False, status="unknown")

# ====================================================================
print("\n=== FILTER 3: IMPOSSIBLE_MATCH ===")
# ====================================================================
# No match_time — skip
m = MarketInfo(market_id="i1", team_a="A", team_b="B", game="cs2", match_time=None)
check("No match_time skips filter", det._check_impossible_match(m) == [])

# LP shows team playing someone else at same time
lp_match = MatchInfo(team_a="A", team_b="DifferentTeam",
                     scheduled_time=now + timedelta(hours=1))
lp.get_upcoming_matches.return_value = [lp_match]
m2 = MarketInfo(market_id="i2", team_a="A", team_b="B", game="cs2",
                match_time=now + timedelta(hours=1))
flags2 = det._check_impossible_match(m2)
check("Impossible match detected", any(f.flag_type == "IMPOSSIBLE_MATCH" for f in flags2))
lp.get_upcoming_matches.return_value = []

# ====================================================================
print("\n=== FILTER 4: WRONG_OPPONENT ===")
# ====================================================================
# Use very distinct opponent names so fuzzy match doesn't confuse them
lp_match = MatchInfo(team_a="Cloud9", team_b="NAVI",
                     scheduled_time=now + timedelta(hours=1))
lp.get_upcoming_matches.return_value = [lp_match]
m = MarketInfo(market_id="wo1", team_a="Cloud9", team_b="FaZe Clan",
               game="cs2", match_time=now + timedelta(hours=1))
flags = det._check_wrong_opponent(m)
check("Wrong opponent detected", any(f.flag_type == "WRONG_OPPONENT" for f in flags))

# Correct opponent — no flag
m2 = MarketInfo(market_id="wo2", team_a="Cloud9", team_b="NAVI",
                game="cs2", match_time=now + timedelta(hours=1))
flags2 = det._check_wrong_opponent(m2)
check("Correct opponent not flagged", flags2 == [])
lp.get_upcoming_matches.return_value = []

# ====================================================================
print("\n=== FILTER 5: TIME_MISMATCH ===")
# ====================================================================
# No match time — skip
m = MarketInfo(market_id="t1", team_a="A", team_b="B", game="cs2", match_time=None)
check("No time skips TIME_MISMATCH", det._check_time_mismatch(m) == [])

# Same match, time off by 3 hours
lp_match = MatchInfo(team_a="A", team_b="B",
                     scheduled_time=now + timedelta(hours=5))
lp.get_upcoming_matches.return_value = [lp_match]
m2 = MarketInfo(market_id="t2", team_a="A", team_b="B", game="cs2",
                match_time=now + timedelta(hours=2))
flags2 = det._check_time_mismatch(m2)
check("3hr time mismatch detected", any(f.flag_type == "TIME_MISMATCH" for f in flags2))
lp.get_upcoming_matches.return_value = []

# ====================================================================
print("\n=== FILTER 6: WRONG_TOURNAMENT ===")
# ====================================================================
# Use realistic names so _team_in_match fuzzy logic finds them
lp_match = MatchInfo(team_a="Cloud9", team_b="Astralis", tournament="IEM Katowice 2026")
lp.get_upcoming_matches.return_value = [lp_match]
lp.fuzzy_match_team.return_value = "Cloud9"
m = MarketInfo(market_id="wt1", team_a="Cloud9", team_b="Astralis",
               game="cs2", tournament="PGL Major")
flags = det._check_wrong_tournament(m)
check("Wrong tournament detected", any(f.flag_type == "WRONG_TOURNAMENT" for f in flags))
lp.get_upcoming_matches.return_value = []
lp.fuzzy_match_team.return_value = None

# ====================================================================
print("\n=== FILTER 7: ROSTER_MISMATCH ===")
# ====================================================================
# No description — skip
m = MarketInfo(market_id="rm1", team_a="A", team_b="B", game="cs2", description="")
check("No description skips roster", det._check_roster_mismatch(m) == [])

# ====================================================================
print("\n=== FILTER 8: LIQUIDITY_ANOMALY ===")
# ====================================================================
m = MarketInfo(market_id="la1", outcome_prices=[0.03, 0.97], volume=100, liquidity=50,
               team_a="A", team_b="B", game="cs2")
flags = det._check_liquidity_anomaly(m)
check("Low liq flagged", any(f.flag_type == "LIQUIDITY_ANOMALY" for f in flags))
check("16.7x in desc", any("16.7x" in f.description for f in flags))

check("High vol not flagged", det._check_liquidity_anomaly(
    MarketInfo(outcome_prices=[0.05, 0.95], volume=50000, team_a="A", team_b="B")) == [])
check("Normal prices not flagged", det._check_liquidity_anomaly(
    MarketInfo(outcome_prices=[0.45, 0.55], volume=100, team_a="A", team_b="B")) == [])
check("Empty prices not flagged", det._check_liquidity_anomaly(
    MarketInfo(outcome_prices=[], volume=100, team_a="A", team_b="B")) == [])

# ====================================================================
print("\n=== FILTER 9: ALREADY_PLAYED ===")
# ====================================================================
m = MarketInfo(market_id="ap1", team_a="A", team_b="B", game="cs2",
               match_time=now - timedelta(hours=5))
flags = det._check_already_played(m)
check("5hr past flagged", any(f.flag_type == "ALREADY_PLAYED" for f in flags))
check("5hr sev=4", any(f.severity == 4 for f in flags))

m2 = MarketInfo(market_id="ap2", team_a="A", team_b="B",
                match_time=now - timedelta(hours=12))
check("12hr sev=6", any(f.severity == 6 for f in det._check_already_played(m2)))
check("Future not flagged", det._check_already_played(
    MarketInfo(match_time=now + timedelta(hours=2), team_a="A")) == [])
check("30min past not flagged", det._check_already_played(
    MarketInfo(match_time=now - timedelta(minutes=30), team_a="A")) == [])
check("No time not flagged", det._check_already_played(
    MarketInfo(match_time=None, team_a="A")) == [])

# ====================================================================
print("\n=== FILTER 10: DUPLICATE_MARKET ===")
# ====================================================================
m1 = MarketInfo(market_id="d1", team_a="Cloud9", team_b="FaZe Clan", game="cs2",
                question="Cloud9 vs FaZe Clan", outcome_prices=[0.30, 0.70])
m2 = MarketInfo(market_id="d2", team_a="Cloud9", team_b="FaZe", game="cs2",
                question="Cloud9 vs FaZe", outcome_prices=[0.35, 0.65])
check("Dup detected", any(f.flag_type == "DUPLICATE_MARKET" for f in det._check_duplicate_market(m1, [m1, m2])))
check("Different teams no dup", det._check_duplicate_market(m1, [m1, MarketInfo(
    market_id="d3", team_a="NAVI", team_b="G2", game="cs2")]) == [])
check("Different game no dup", det._check_duplicate_market(m1, [m1, MarketInfo(
    market_id="d4", team_a="Cloud9", team_b="FaZe", game="valorant")]) == [])
check("Self not dup", det._check_duplicate_market(m1, [m1]) == [])

# ====================================================================
print("\n=== FILTER 11: AFFILIATE_CONFUSION ===")
# ====================================================================
# Mock LP to return affiliate match
aff_match = MatchInfo(team_a="Falcons Academy Green", team_b="OpTic")
lp.get_upcoming_matches.return_value = [aff_match]
m = MarketInfo(market_id="ac1", team_a="Falcons", team_b="OpTic",
               game="call-of-duty", tournament="CDL")
flags = det._check_affiliate_confusion(m)
check("Falcons affiliate confusion detected", any(f.flag_type == "AFFILIATE_CONFUSION" for f in flags))
lp.get_upcoming_matches.return_value = []

# Non-org team
m2 = MarketInfo(market_id="ac2", team_a="RandomTeamXYZ", team_b="X", game="cs2")
check("Non-org not flagged", det._check_affiliate_confusion(m2) == [])

# ====================================================================
print("\n=== FILTER 12: CROSS_GAME_CONFLICT ===")
# ====================================================================
# No match time — skip
m = MarketInfo(market_id="cg1", team_a="A", team_b="B", game="cs2", match_time=None)
check("No time skips CROSS_GAME", det._check_cross_game_conflict(m) == [])

# ====================================================================
print("\n=== FILTER 13: REPEAT_OFFENDER ===")
# ====================================================================
db.add_resolved_market("res1", question="Las Vegas Falcons vs X",
                       team_a="Las Vegas Falcons", team_b="X",
                       tournament="CDL III", game="call-of-duty",
                       was_fifty_fifty=True, resolved_at="2026-01-15")
m = MarketInfo(market_id="ro1", team_a="Las Vegas Falcons", team_b="Boston",
               game="call-of-duty", tournament="CDL III")
flags = det._check_repeat_offender(m)
check("Repeat offender detected", any(f.flag_type == "REPEAT_OFFENDER" for f in flags))
check("Sev 10", any(f.severity == 10 for f in flags))
check("Different tournament not flagged", det._check_repeat_offender(
    MarketInfo(team_a="Las Vegas Falcons", team_b="X", tournament="CDL IV")) == [])
check("No tournament skips", det._check_repeat_offender(
    MarketInfo(team_a="Las Vegas Falcons", team_b="X", tournament="")) == [])

# ====================================================================
print("\n=== FILTER 14: LIQUIPEDIA_DRIFT ===")
# ====================================================================
lp_match = MatchInfo(team_a="TeamA", team_b="RealTeamB",
                     scheduled_time=now + timedelta(hours=1))
lp.get_upcoming_matches.return_value = [lp_match]
m = MarketInfo(market_id="ld1", team_a="TeamA", team_b="WrongTeamB",
               game="cs2", match_time=now + timedelta(hours=1))
flags = det._check_liquipedia_drift(m)
check("LP drift detected", any(f.flag_type == "LIQUIPEDIA_DRIFT" for f in flags))
check("LP drift sev 9", any(f.severity == 9 for f in flags))
lp.get_upcoming_matches.return_value = []

# ====================================================================
print("\n=== DETECT_ALL INTEGRATION ===")
# ====================================================================
m = MarketInfo(market_id="", team_a="", team_b="", question="no")
check("Empty teams skips detect_all", det.detect_all(m, []) == [])

m = MarketInfo(market_id="m1", team_a="Las Vegas Falcons", team_b="North",
               game="call-of-duty", tournament="CDL III",
               outcome_prices=[0.04, 0.96], volume=200, liquidity=50,
               match_time=now - timedelta(hours=8))
flags = det.detect_all(m, [m])
ft = {f.flag_type for f in flags}
check("Multi: REPEAT_OFFENDER", "REPEAT_OFFENDER" in ft)
check("Multi: RENAMED_TEAM", "RENAMED_TEAM" in ft)
check("Multi: LIQUIDITY_ANOMALY", "LIQUIDITY_ANOMALY" in ft)
check("Multi: ALREADY_PLAYED", "ALREADY_PLAYED" in ft)
result = scorer.score(m, flags)
check("Multi scores high", result.normalized_score >= 40)

# ====================================================================
print("\n=== UTILITY METHODS ===")
# ====================================================================
t1 = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)
t2 = datetime(2026, 3, 20, 13, 0, tzinfo=timezone.utc)
t3 = datetime(2026, 3, 20, 18, 0, tzinfo=timezone.utc)
check("1hr apart overlap", det._times_overlap(t1, t2, hours=2))
check("6hr apart no overlap", not det._times_overlap(t1, t3, hours=2))
check("None no overlap", not det._times_overlap(None, t1))
check("Both None", not det._times_overlap(None, None))

check("Exact fuzzy", det._fuzzy_match("Cloud9", "Cloud9"))
check("Case fuzzy", det._fuzzy_match("cloud9", "Cloud9"))
check("Partial: OpTic/OpTic Gaming", det._fuzzy_match("OpTic", "OpTic Gaming"))
check("No match: Cloud9/NAVI", not det._fuzzy_match("Cloud9", "NAVI"))
check("Empty no match", not det._fuzzy_match("", "Cloud9"))

# ====================================================================
print("\n=== ERROR HANDLING ===")
# ====================================================================
import config as cfg
orig = cfg.TEAM_ALIASES_FILE
cfg.TEAM_ALIASES_FILE = "/tmp/nonexistent.json"
det2 = AnomalyDetector(db, lp)
check("Missing alias file handled", det2._team_aliases == {})
cfg.TEAM_ALIASES_FILE = orig

# LP raises exception — filter degrades
lp_err = MagicMock(spec=LiquipediaClient)
lp_err.get_upcoming_matches.side_effect = Exception("API down")
lp_err.get_team_info.side_effect = Exception("API down")
lp_err.fuzzy_match_team.side_effect = Exception("API down")
lp_err.get_team_roster.side_effect = Exception("API down")
det_err = AnomalyDetector(db, lp_err)
m_err = MarketInfo(market_id="e1", team_a="A", team_b="B", game="cs2",
                   match_time=now + timedelta(hours=1), tournament="T")
try:
    flags_err = det_err.detect_all(m_err, [m_err])
    check("LP exception doesn't crash detect_all", True)
except Exception as e:
    check("LP exception doesn't crash detect_all", False, str(e))

# Score None market edge
try:
    r = scorer.score(MarketInfo(), [])
    check("Score empty market", r.normalized_score == 0)
except Exception as e:
    check("Score empty market no crash", False, str(e))

# ====================================================================
print("\n=== DISCORD NOTIFIER ===")
# ====================================================================
notifier = DiscordNotifier(db)
check("Fresh market not deduped", not db.is_alert_sent_recently("notif_test"))
db.record_alert("notif_test", score=80)
check("After record, dedup fires", db.is_alert_sent_recently("notif_test"))

# Build embed
sr = ScoringResult(
    market=MarketInfo(market_id="e1", question="A vs B", game="cs2",
                      tournament="Major", outcome_prices=[0.05, 0.95],
                      volume=500, liquidity=200, url="https://polymarket.com/test",
                      match_time=now),
    normalized_score=85.0, recommendation="BUY_CHEAP_SIDE",
    flags=[AnomalyFlag("REPEAT_OFFENDER", 10, "desc", "evidence"),
           AnomalyFlag("RENAMED_TEAM", 8, "old", "renamed")],
    booster_applied="REPEAT_OFFENDER (2.0x)", cheap_side_price=0.05, multiplier=10.0)
try:
    embed = notifier._build_anomaly_embed(sr)
    check("Embed builds OK", embed is not None)
except Exception as e:
    check("Embed builds OK", False, str(e))

# Minimal data embed
try:
    embed2 = notifier._build_anomaly_embed(ScoringResult(
        market=MarketInfo(market_id="min", question="?"),
        normalized_score=0, recommendation="INVESTIGATE", flags=[]))
    check("Minimal embed OK", embed2 is not None)
except Exception as e:
    check("Minimal embed OK", False, str(e))

# Long flags embed (truncation test)
try:
    embed3 = notifier._build_anomaly_embed(ScoringResult(
        market=MarketInfo(market_id="long", question="A vs B"),
        normalized_score=90, recommendation="BUY_CHEAP_SIDE",
        flags=[AnomalyFlag(f"F{i}", 5, "x " * 50) for i in range(20)],
        cheap_side_price=0.03, multiplier=16.7))
    check("20 flags embed truncation OK", embed3 is not None)
except Exception as e:
    check("20 flags embed OK", False, str(e))

# No webhook URL
notifier2 = DiscordNotifier(db)
notifier2.webhook_url = ""
check("No webhook returns False", notifier2._send_webhook("", content="test") is False)

# ====================================================================
print("\n=== MISC EDGE CASES ===")
# ====================================================================
check("Negative price", MarketInfo(outcome_prices=[-0.5, 1.5]).has_cheap_side is False)
check("Single price", MarketInfo(outcome_prices=[0.5]).cheap_side_price == 0.5)
check("Threshold price", MarketInfo(outcome_prices=[0.10, 0.90]).has_cheap_side is True)
check("to_dict with time", MarketInfo(match_time=now).to_dict()["match_time"] is not None)
check("to_dict no time", MarketInfo().to_dict()["match_time"] is None)

# ====================================================================
db.close()
print(f"\n{'=' * 50}")
print(f"Part 2 Results: {PASS} passed, {FAIL} failed")
print(f"{'=' * 50}")
sys.exit(1 if FAIL > 0 else 0)
