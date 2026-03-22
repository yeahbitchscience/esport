"""
Main bot loop — orchestrates the full anomaly detection pipeline.

Polls Polymarket every 5 minutes, runs all filters, scores, and alerts.
"""

import signal
import sys
import time
import threading
from datetime import datetime, timezone
from typing import List

import config
from anomaly_detector import AnomalyDetector
from database import Database
from discord_notifier import DiscordNotifier
from liquipedia_client import LiquipediaClient
from logger import log
from polymarket_client import MarketInfo, PolymarketClient
from scoring import ScoringEngine, ScoringResult


class EsportsAnomalyBot:
    """Main bot class that runs the polling loop."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self._running = False
        self._consecutive_failures = 0
        self._cycle_count = 0
        self._last_cache_refresh = 0.0
        self._is_first_cycle = True

        # Initialize components
        log.info("Initializing Esports Anomaly Bot...")

        self.db = Database()
        self.polymarket = PolymarketClient()
        self.liquipedia = LiquipediaClient(self.db)
        self.detector = AnomalyDetector(self.db, self.liquipedia)
        self.scorer = ScoringEngine()
        self.notifier = DiscordNotifier(self.db)

        log.info("All components initialized")

    def _interruptible_sleep(self, seconds: int):
        """Sleep in 1-second increments so shutdown is near-instant."""
        for _ in range(seconds):
            if not self._running:
                return
            time.sleep(1)

    def start(self):
        """Start the bot's main loop."""
        self._running = True

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        log.info(
            f"Bot starting — polling every {config.POLL_INTERVAL_SECONDS}s, "
            f"alert threshold: {config.ALERT_SCORE_THRESHOLD}, "
            f"dry_run: {self.dry_run}"
        )

        # Send startup notification
        if not self.dry_run:
            self.notifier.send_startup_message()

        # Run first cycle immediately
        self._run_cycle()

        # Main loop
        while self._running:
            try:
                self._interruptible_sleep(config.POLL_INTERVAL_SECONDS)
                if self._running:
                    self._run_cycle()
            except KeyboardInterrupt:
                self._running = False
            except Exception as e:
                log.error(f"Unexpected error in main loop: {e}", exc_info=True)
                self._handle_failure(e)

        log.info("Bot stopped")
        self.db.close()

    def stop(self):
        """Stop the bot gracefully."""
        log.info("Stopping bot...")
        self._running = False

    def _handle_shutdown(self, signum, frame):
        """Handle SIGINT/SIGTERM for graceful shutdown."""
        log.info(f"Received signal {signum}, shutting down...")
        self._running = False

    def _run_cycle(self):
        """Run one complete polling and detection cycle."""
        self._cycle_count += 1
        cycle_start = time.time()
        log.info(f"═══ Starting cycle #{self._cycle_count} ═══")

        try:
            # Step 1: Fetch all open esports markets
            markets = self.polymarket.fetch_open_esports_markets()

            # Filter for new markets only
            new_markets = []
            for market in markets:
                if not self.db.is_market_processed(market.market_id):
                    new_markets.append(market)
                    self.db.mark_market_processed(market.market_id)

            if self._is_first_cycle:
                log.info(f"Initial boot baseline set: {len(new_markets)} pre-existing open markets identified and explicitly ignored. Only new markets will be evaluated hereafter.")
                self._is_first_cycle = False
                
                # Still do cache cleanup before retreating
                self.db.cleanup_old_alerts()
                self.db.cleanup_old_liquipedia_cache()
                return

            if not new_markets:
                log.info("No new markets detected. Idling.")
                self.db.cleanup_old_alerts()
                self.db.cleanup_old_liquipedia_cache()
                return

            log.info(f"Processing {len(new_markets)} newly listed market(s) for anomalies...")

            self._consecutive_failures = 0

            # Step 2: Check if we need to refresh caches
            self._maybe_refresh_caches()

            # Step 3: Run anomaly detection on each market
            results = self._analyze_markets(new_markets)

            # Step 4: Process results
            alerts_sent = 0
            for result in results:
                if result.normalized_score >= config.ALERT_SCORE_THRESHOLD:
                    if self.dry_run:
                        self._log_dry_run_result(result)
                    else:
                        if self.notifier.send_anomaly_alert(result):
                            alerts_sent += 1

            # Step 5: Cleanup
            self.db.cleanup_old_alerts()
            self.db.cleanup_old_liquipedia_cache()

            elapsed = time.time() - cycle_start
            log.info(
                f"═══ Cycle #{self._cycle_count} complete — "
                f"{len(markets)} markets, {len(results)} flagged, "
                f"{alerts_sent} alerts sent, {elapsed:.1f}s ═══"
            )

        except Exception as e:
            log.error(f"Cycle #{self._cycle_count} failed: {e}", exc_info=True)
            self._handle_failure(e)

    def _analyze_markets(self, markets: List[MarketInfo]) -> List[ScoringResult]:
        """Run anomaly detection and scoring on all markets."""
        results = []

        for i, market in enumerate(markets):
            try:
                flags = self.detector.detect_all(market, all_markets=markets)

                if not flags:
                    continue

                result = self.scorer.score(market, flags)
                results.append(result)

                log.info(
                    f"  [{result.recommendation}] {market.question} — "
                    f"score: {result.normalized_score}/100, flags: {len(flags)}"
                )

            except Exception as e:
                log.error(f"Failed to analyze market {market.market_id}: {e}")
                continue

        # Sort by score descending
        results.sort(key=lambda r: r.normalized_score, reverse=True)
        return results

    def _maybe_refresh_caches(self):
        """Refresh tournament fingerprints and Liquipedia cache if due."""
        now = time.time()
        if now - self._last_cache_refresh < config.TOURNAMENT_REFRESH_INTERVAL:
            return

        log.info("Refreshing tournament caches...")
        self._last_cache_refresh = now
        self.detector.reload_data_files()
        self.db.cleanup_old_tournament_fingerprints()

    def _handle_failure(self, error: Exception):
        """Handle consecutive failures with alerting."""
        self._consecutive_failures += 1
        if self._consecutive_failures >= config.CONSECUTIVE_FAILURE_ALERT_THRESHOLD:
            if not self.dry_run:
                self.notifier.send_health_warning(
                    self._consecutive_failures,
                    str(error)
                )

        # If too many failures, something is seriously wrong
        if self._consecutive_failures >= config.CONSECUTIVE_FAILURE_ALERT_THRESHOLD * 3:
            log.critical(
                f"Too many consecutive failures ({self._consecutive_failures}) — "
                "consider restarting the bot"
            )

    def _log_dry_run_result(self, result: ScoringResult):
        """Log a result in dry-run mode (no Discord alert sent)."""
        market = result.market
        print("\n" + "=" * 70)
        print(f"🚨 ANOMALY DETECTED (DRY RUN)")
        print(f"Market:     {market.question}")
        print(f"Game:       {market.game}")
        print(f"Tournament: {market.tournament}")
        if market.match_time:
            print(f"Match Time: {market.match_time.strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"\nFLAGS:")
        for flag in sorted(result.flags, key=lambda f: f.severity, reverse=True):
            emoji = "🔴" if flag.severity >= 8 else "🟡" if flag.severity >= 5 else "🔵"
            print(f"  {emoji} {flag.flag_type} ({flag.severity}/10) — {flag.description}")
            if flag.evidence:
                print(f"     Evidence: {flag.evidence}")
        print(f"\nCheap Side: ${result.cheap_side_price:.2f}")
        print(f"Multiplier: {result.multiplier:.1f}x")
        print(f"Volume:     ${market.volume:,.0f}")
        print(f"Liquidity:  ${market.liquidity:,.0f}")
        print(f"\nScore: {result.normalized_score:.0f}/100 → {result.recommendation}")
        if result.booster_applied:
            print(f"Booster: {result.booster_applied}")
        if market.url:
            print(f"Link: {market.url}")
        print("=" * 70)
