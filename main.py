"""
Polymarket Esports Anomaly Bot — Entry Point

Usage:
    python main.py              # Run normally (sends Discord alerts)
    python main.py --dry-run    # Run without sending Discord alerts (logs to console)
"""

import sys
import traceback

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from logger import log

class KeepaliveHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler to satisfy Render's port checker."""
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Bot is actively polling Polymarket.")

    def log_message(self, format, *args):
        pass  # Completely suppress standard HTTP logging to keep console clean

def start_keepalive_server():
    """Boots a background HTTP server on the PORT env variable."""
    import os
    port = int(os.getenv("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), KeepaliveHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info(f"Keepalive dummy server successfully bound to port {port}")


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
        # Start the dummy Web Service server to trick Render into keeping the bot alive
        start_keepalive_server()
        
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
