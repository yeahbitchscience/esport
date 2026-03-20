"""
Polymarket Esports Anomaly Bot — Entry Point

Usage:
    python main.py              # Run normally (sends Discord alerts)
    python main.py --dry-run    # Run without sending Discord alerts (logs to console)
"""

import sys
import traceback

from logger import log


def main():
    """Main entry point."""
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        log.info("Starting in DRY RUN mode — no Discord alerts will be sent")
    else:
        log.info("Starting in PRODUCTION mode")

    # Import here to avoid circular imports and to catch import errors early
    try:
        from bot import EsportsAnomalyBot
    except ImportError as e:
        log.critical(f"Failed to import bot: {e}")
        log.critical("Run: pip install -r requirements.txt")
        sys.exit(1)

    bot = None
    try:
        bot = EsportsAnomalyBot(dry_run=dry_run)
        bot.start()
    except KeyboardInterrupt:
        log.info("Shutting down gracefully...")
    except Exception as e:
        log.critical(f"Fatal error: {e}", exc_info=True)
        # Try to send crash alert
        if bot and not dry_run:
            try:
                from discord_notifier import DiscordNotifier
                from database import Database
                notifier = DiscordNotifier(bot.db)
                notifier.send_crash_alert(e)
            except Exception:
                pass
        sys.exit(1)
    finally:
        if bot:
            try:
                bot.db.close()
            except Exception:
                pass
        log.info("Bot exited")


if __name__ == "__main__":
    main()
