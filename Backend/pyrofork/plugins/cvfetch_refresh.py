"""Handle CVFetch refresh markers and start a normal incremental channel scan."""

import asyncio

from pyrogram import Client, filters
from pyrogram.types import Message

from Backend.helper.scan_manager import scan_manager
from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER

REFRESH_MARKER = "#cvfetch_refresh"
_pending_channels: set[str] = set()
_refresh_worker: asyncio.Task | None = None


def _is_auth_channel(chat_id: int) -> bool:
    configured = SettingsManager.current().auth_channels or []
    return str(chat_id) in {str(channel).strip() for channel in configured}


async def _drain_refresh_queue(client: Client) -> None:
    while _pending_channels:
        if scan_manager.get_status().get("is_running"):
            await asyncio.sleep(1)
            continue

        channels = sorted(_pending_channels)
        _pending_channels.difference_update(channels)
        result = await scan_manager.start(client, channels, mode="scan")
        if result.get("ok"):
            LOGGER.info("[CVFetch] Started normal scan for %s", ", ".join(channels))
            continue

        if "already running" in str(result.get("message", "")).lower():
            _pending_channels.update(channels)
            await asyncio.sleep(1)
            continue

        LOGGER.warning("[CVFetch] Could not start scan for %s: %s", channels, result.get("message"))
        break


@Client.on_message(filters.channel & filters.text & filters.regex(r"^#cvfetch_refresh$"), group=20)
async def handle_cvfetch_refresh(client: Client, message: Message) -> None:
    global _refresh_worker

    if not message.chat or not _is_auth_channel(message.chat.id):
        return

    try:
        await message.delete()
    except Exception as exc:
        LOGGER.warning("[CVFetch] Could not remove refresh marker from %s: %s", message.chat.id, exc)

    _pending_channels.add(str(message.chat.id))
    if _refresh_worker is None or _refresh_worker.done():
        _refresh_worker = asyncio.create_task(_drain_refresh_queue(client))
