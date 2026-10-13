"""
notifier.py — minimal Discord webhook helper for the MACD bot.

Used by the engine to alert on critical events such as a position-state
divergence that the bot couldn't auto-recover from. Non-blocking:
notifications are sent on a daemon thread so a slow webhook never stalls
the engine.

Webhook URL is the same one used by discord_monitor.py.
"""
import os
import json
import threading
import logging
from datetime import datetime
from typing import Optional, List

try:
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    _HAVE_REQUESTS = True
except Exception:
    _HAVE_REQUESTS = False

logger = logging.getLogger("notifier")

DISCORD_WEBHOOK = __import__("os").environ.get("DISCORD_WEBHOOK_URL", "")
ACCOUNT_TAG = "DUXXXXXXX"


_send_lock = threading.Lock()


def _post_embed(title: str, message: str, color: int,
                fields: Optional[List[dict]] = None) -> None:
    """Synchronous POST to the Discord webhook. Caller wraps in a thread."""
    if not _HAVE_REQUESTS:
        logger.warning("requests not available — cannot send Discord embed")
        return
    try:
        embed = {
            "title"      : title,
            "description": message,
            "color"      : color,
            "timestamp"  : datetime.utcnow().isoformat(),
            "footer"     : {"text": f"MACD Bot  |  {ACCOUNT_TAG}"},
        }
        if fields:
            embed["fields"] = fields
        with _send_lock:
            r = requests.post(DISCORD_WEBHOOK, json={"embeds": [embed]}, timeout=10)
        if r.status_code not in (200, 204):
            logger.warning("Discord send returned %d: %s", r.status_code, r.text[:200])
    except Exception as exc:
        logger.warning("Discord send failed: %s", exc)


def notify(title: str, message: str,
           color: int = 0x2F3136,
           fields: Optional[List[dict]] = None) -> None:
    """Fire-and-forget — sends Discord notification on a daemon thread."""
    threading.Thread(
        target=_post_embed,
        args=(title, message, color, fields),
        daemon=True,
        name="discord-notify",
    ).start()


def notify_critical(title: str, message: str,
                    fields: Optional[List[dict]] = None) -> None:
    """Red embed for critical alerts (e.g. failed reconciliation)."""
    notify(title, message, color=0xFF0000, fields=fields)


def notify_warning(title: str, message: str,
                   fields: Optional[List[dict]] = None) -> None:
    """Orange embed for warnings."""
    notify(title, message, color=0xFFA500, fields=fields)


def notify_info(title: str, message: str,
                fields: Optional[List[dict]] = None) -> None:
    """Blue embed for informational alerts."""
    notify(title, message, color=0x3498DB, fields=fields)
