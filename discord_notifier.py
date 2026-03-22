"""
Discord webhook notifications with 24-hour deduplication.

Sends rich embeds when anomalies are detected, health warnings on
repeated failures, and crash alerts on fatal errors.
"""

import hashlib
import traceback
from datetime import datetime, timezone
from typing import List, Optional

from discord_webhook import DiscordEmbed, DiscordWebhook

import config
from database import Database
from logger import log
from polymarket_client import MarketInfo
from scoring import ScoringResult


# Severity emoji mapping
SEVERITY_EMOJI = {
    10: "🔴",
    9: "🔴",
    8: "🟠",
    7: "🟠",
    6: "🟡",
    5: "🟡",
    4: "🔵",
    3: "🔵",
    2: "⚪",
    1: "⚪",
}

# Recommendation colors (Discord embed color as integer)
RECOMMENDATION_COLORS = {
    "BUY_CHEAP_SIDE": 0xFF0000,   # Red — urgent
    "MONITOR": 0xFFA500,           # Orange — watch
    "INVESTIGATE": 0x3498DB,       # Blue — info
}


class DiscordNotifier:
    """Send Discord webhook alerts with deduplication."""

    def __init__(self, db: Database):
        self.db = db
        self.webhook_url = config.DISCORD_WEBHOOK_URL
        self.error_webhook_url = config.DISCORD_ERROR_WEBHOOK_URL or self.webhook_url

    def _send_webhook(self, webhook_url: str, embeds: List[DiscordEmbed] = None,
                      content: str = None) -> bool:
        """Send a webhook message robustly bypassing Cloudflare."""
        if not webhook_url:
            log.warning("Discord webhook URL not configured — skipping notification")
            return False

        try:
            from curl_cffi import requests as cffi_requests
            
            # Rewrite discord.com to canary.discord.com to bypass aggressive Render IP Cloudflare blocks
            if "discord.com" in webhook_url and "canary.discord.com" not in webhook_url:
                webhook_url = webhook_url.replace("discord.com", "canary.discord.com")
            
            payload = {}
            if content:
                payload["content"] = content
            if embeds:
                # DiscordEmbed stores its JSON payload natively in __dict__, but we must strip None values
                payload["embeds"] = [
                    {k: v for k, v in e.__dict__.items() if v is not None and v != []}
                    for e in embeds
                ]
                
            session = cffi_requests.Session(impersonate="chrome")
            response = session.post(webhook_url, json=payload, timeout=10)
            
            if response.status_code in (200, 204):
                log.info("Discord alert sent successfully via cffi bypass")
                return True
            else:
                log.error(f"Discord webhook failed with HTTP {response.status_code}: {response.text[:200]}")
                return False

        except Exception as e:
            log.error(f"Failed to send Discord webhook: {e}")
            return False

    def send_anomaly_alert(self, result: ScoringResult) -> bool:
        """
        Send an anomaly detection alert to Discord.
        Returns True if sent, False if skipped (dedup) or failed.
        """
        market = result.market
        if not market:
            return False

        # Check deduplication
        if self.db.is_alert_sent_recently(market.market_id):
            log.debug(f"Skipping duplicate alert for {market.market_id}")
            return False

        # Build the embed
        embed = self._build_anomaly_embed(result)

        # Send
        success = self._send_webhook(self.webhook_url, embeds=[embed])

        if success:
            # Record alert for dedup
            alert_hash = hashlib.md5(
                f"{market.market_id}:{result.normalized_score}".encode()
            ).hexdigest()
            self.db.record_alert(
                market_id=market.market_id,
                alert_hash=alert_hash,
                score=result.normalized_score,
                recommendation=result.recommendation,
                flags=[
                    {"type": f.flag_type, "severity": f.severity, "description": f.description}
                    for f in result.flags
                ],
            )
            log.info(
                f"Anomaly alert sent: {market.question} "
                f"(score: {result.normalized_score}/100, rec: {result.recommendation})"
            )

        return success

    def send_health_warning(self, consecutive_failures: int, last_error: str = ""):
        """Send a health warning when consecutive failures exceed threshold."""
        embed = DiscordEmbed(
            title="⚠️ ESPORTS BOT HEALTH WARNING",
            description=(
                f"The bot has encountered **{consecutive_failures}** consecutive "
                f"API failures. It is still running but may be missing data."
            ),
            color=0xFFA500,
        )
        embed.set_timestamp()
        if last_error:
            embed.add_embed_field(
                name="Last Error",
                value=f"```{last_error[:500]}```",
                inline=False,
            )
        embed.add_embed_field(
            name="Action Required",
            value="Check Polymarket/Liquipedia API status. Bot will continue retrying.",
            inline=False,
        )
        embed.set_footer(text="Polymarket Esports Anomaly Bot")

        self._send_webhook(self.error_webhook_url, embeds=[embed])

    def send_crash_alert(self, error: Exception):
        """Send an alert when the bot crashes fatally."""
        tb = traceback.format_exception(type(error), error, error.__traceback__)
        tb_str = "".join(tb)[-1000:]

        embed = DiscordEmbed(
            title="🚨 ESPORTS BOT CRASHED",
            description="The bot has encountered a fatal error and stopped.",
            color=0xFF0000,
        )
        embed.set_timestamp()
        embed.add_embed_field(
            name="Error",
            value=f"```{str(error)[:500]}```",
            inline=False,
        )
        embed.add_embed_field(
            name="Traceback",
            value=f"```{tb_str}```",
            inline=False,
        )
        embed.add_embed_field(
            name="Action Required",
            value="Restart the bot manually. Check logs for full traceback.",
            inline=False,
        )
        embed.set_footer(text="Polymarket Esports Anomaly Bot")

        self._send_webhook(self.error_webhook_url, embeds=[embed])

    def send_startup_message(self):
        """Send a message when the bot starts up."""
        embed = DiscordEmbed(
            title="✅ Esports Anomaly Bot Started",
            description=(
                f"Bot is now polling Polymarket every "
                f"{config.POLL_INTERVAL_SECONDS // 60} minutes.\n"
                f"Alert threshold: {config.ALERT_SCORE_THRESHOLD}/100\n"
                f"Dedup window: {config.DEDUP_HOURS}h"
            ),
            color=0x2ECC71,
        )
        embed.set_timestamp()
        embed.set_footer(text="Polymarket Esports Anomaly Bot")
        self._send_webhook(self.webhook_url, embeds=[embed])

    def _build_anomaly_embed(self, result: ScoringResult) -> DiscordEmbed:
        """Build a rich Discord embed for an anomaly alert."""
        market = result.market
        rec = result.recommendation

        # Title with urgency indicator
        urgency = {
            "BUY_CHEAP_SIDE": "🚨",
            "MONITOR": "⚠️",
            "INVESTIGATE": "🔍",
        }.get(rec, "🔍")

        embed = DiscordEmbed(
            title=f"{urgency} ESPORTS ANOMALY DETECTED",
            description=f"**{market.question}**",
            color=RECOMMENDATION_COLORS.get(rec, 0x3498DB),
            url=market.url if market.url else None,
        )
        embed.set_timestamp()

        # Market info
        info_parts = []
        if market.game:
            info_parts.append(f"🎮 **Game:** {market.game}")
        if market.tournament:
            info_parts.append(f"🏆 **Tournament:** {market.tournament}")
        if market.match_time:
            info_parts.append(f"🕐 **Match Time:** {market.match_time.strftime('%Y-%m-%d %H:%M UTC')}")
        if info_parts:
            embed.add_embed_field(
                name="📋 Market Info",
                value="\n".join(info_parts),
                inline=False,
            )

        # Flags
        flag_lines = []
        for flag in sorted(result.flags, key=lambda f: f.severity, reverse=True):
            emoji = SEVERITY_EMOJI.get(flag.severity, "⚪")
            flag_lines.append(
                f"{emoji} **{flag.flag_type}** ({flag.severity}/10) — {flag.description}"
            )
        if flag_lines:
            # Discord embeds have a 1024 char limit per field
            flags_text = "\n".join(flag_lines)
            while len(flags_text) > 1020:
                flag_lines = flag_lines[:-1]
                flags_text = "\n".join(flag_lines) + "\n..."
            embed.add_embed_field(
                name=f"🚩 Flags ({len(result.flags)})",
                value=flags_text,
                inline=False,
            )

        # Pricing & Multiplier
        pricing_parts = []
        if result.cheap_side_price > 0:
            pricing_parts.append(f"💰 **Cheap Side:** ${result.cheap_side_price:.2f}")
            pricing_parts.append(f"📈 **Multiplier:** {result.multiplier:.1f}x")
        if market.liquidity > 0:
            pricing_parts.append(f"💧 **Liquidity:** ${market.liquidity:,.0f}")
        if market.volume > 0:
            pricing_parts.append(f"📊 **Volume:** ${market.volume:,.0f}")
        if pricing_parts:
            embed.add_embed_field(
                name="💰 Pricing",
                value="\n".join(pricing_parts),
                inline=True,
            )

        # Score & Recommendation
        score_text = (
            f"**Score:** {result.normalized_score:.0f}/100\n"
            f"**Action:** {rec}"
        )
        if result.booster_applied:
            score_text += f"\n**Booster:** {result.booster_applied}"
        embed.add_embed_field(
            name="📊 Score",
            value=score_text,
            inline=True,
        )

        # Link
        if market.url:
            embed.add_embed_field(
                name="🔗 Link",
                value=f"[View on Polymarket]({market.url})",
                inline=False,
            )

        embed.set_footer(text="Polymarket Esports Anomaly Bot")

        return embed
