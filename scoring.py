"""
Scoring Engine — assigns severity scores and recommendations.

Takes the list of AnomalyFlags from the detector and produces a final
score (0-100) with a recommended action.
"""

from dataclasses import dataclass, field
from typing import Dict, List

import config
from anomaly_detector import AnomalyFlag
from logger import log
from polymarket_client import MarketInfo


@dataclass
class ScoringResult:
    """Final scoring output for a market."""
    market: MarketInfo = None
    total_score: float = 0.0
    normalized_score: float = 0.0  # 0-100
    recommendation: str = ""  # BUY_CHEAP_SIDE, MONITOR, INVESTIGATE
    flags: List[AnomalyFlag] = field(default_factory=list)
    booster_applied: str = ""
    cheap_side_price: float = 0.0
    multiplier: float = 0.0

    def to_dict(self) -> Dict:
        return {
            "total_score": round(self.total_score, 2),
            "normalized_score": round(self.normalized_score, 2),
            "recommendation": self.recommendation,
            "booster_applied": self.booster_applied,
            "cheap_side_price": self.cheap_side_price,
            "multiplier": self.multiplier,
            "flags": [
                {
                    "type": f.flag_type,
                    "severity": f.severity,
                    "description": f.description,
                    "evidence": f.evidence,
                }
                for f in self.flags
            ],
            "market": self.market.to_dict() if self.market else {},
        }


class ScoringEngine:
    """Calculate final anomaly scores with booster multipliers."""

    # Weight multipliers per flag type
    WEIGHTS = {
        "REPEAT_OFFENDER": 2.0,        # Highest conviction — double weight
        "LIQUIPEDIA_DRIFT": 1.8,       # Very high conviction
        "RENAMED_TEAM": 1.5,
        "DISBANDED_TEAM": 1.5,
        "AFFILIATE_CONFUSION": 1.3,
        "WRONG_OPPONENT": 1.3,
        "IMPOSSIBLE_MATCH": 1.2,
        "CROSS_GAME_CONFLICT": 1.2,
        "TIME_MISMATCH": 1.0,
        "WRONG_TOURNAMENT": 1.0,
        "ROSTER_MISMATCH": 1.0,
        "ALREADY_PLAYED": 1.0,
        "DUPLICATE_MARKET": 0.8,
        "LIQUIDITY_ANOMALY": 0.8,
    }

    # Maximum possible raw score for normalization
    # Theoretical max: all 14 flags at max severity with max weights
    THEORETICAL_MAX = sum(
        10 * weight for weight in WEIGHTS.values()
    )

    def score(self, market: MarketInfo, flags: List[AnomalyFlag]) -> ScoringResult:
        """
        Calculate the final score for a market based on its anomaly flags.
        
        Score = sum(severity * weight) for each flag, then apply boosters,
        then normalize to 0-100.
        """
        if not flags:
            return ScoringResult(
                market=market,
                total_score=0,
                normalized_score=0,
                recommendation=config.RECOMMENDATION_INVESTIGATE,
                flags=[],
                cheap_side_price=market.cheap_side_price,
                multiplier=market.multiplier,
            )

        # Calculate base score
        base_score = 0.0
        flag_types = set()
        for flag in flags:
            weight = self.WEIGHTS.get(flag.flag_type, 1.0)
            base_score += flag.severity * weight
            flag_types.add(flag.flag_type)

        # Apply boosters
        booster = 1.0
        booster_desc = ""
        has_repeat = "REPEAT_OFFENDER" in flag_types
        has_drift = "LIQUIPEDIA_DRIFT" in flag_types

        if has_repeat and has_drift:
            booster = config.SCORE_BOOSTER_BOTH
            booster_desc = "REPEAT_OFFENDER + LIQUIPEDIA_DRIFT (2.5x)"
        elif has_repeat:
            booster = config.SCORE_BOOSTER_REPEAT_OFFENDER
            booster_desc = "REPEAT_OFFENDER (2.0x)"
        elif has_drift:
            booster = config.SCORE_BOOSTER_LIQUIPEDIA_DRIFT
            booster_desc = "LIQUIPEDIA_DRIFT (1.5x)"

        total = base_score * booster

        # Normalize to 0-100
        normalized = min(100.0, (total / self.THEORETICAL_MAX) * 100.0 * 3)
        # Scale factor of 3 so that a few strong signals can reach high scores

        # Determine recommendation
        if normalized >= config.BUY_THRESHOLD:
            recommendation = config.RECOMMENDATION_BUY
        elif normalized >= config.MONITOR_THRESHOLD:
            recommendation = config.RECOMMENDATION_MONITOR
        else:
            recommendation = config.RECOMMENDATION_INVESTIGATE

        result = ScoringResult(
            market=market,
            total_score=total,
            normalized_score=round(normalized, 1),
            recommendation=recommendation,
            flags=flags,
            booster_applied=booster_desc,
            cheap_side_price=market.cheap_side_price,
            multiplier=market.multiplier,
        )

        log.debug(
            f"Scored market '{market.question}': {normalized:.1f}/100 → {recommendation} "
            f"({len(flags)} flags, booster={booster_desc or 'none'})"
        )

        return result
