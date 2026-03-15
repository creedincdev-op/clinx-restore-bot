import asyncio
import base64
import csv
import io
import json
import os
import re
import secrets
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands

TOKEN = os.getenv("BOT_TOKEN")
DEFAULT_BACKUP_GUILD_ID = os.getenv("DEFAULT_BACKUP_GUILD_ID")
SUPPORT_URL = "https://discord.gg/V6YEw2Wxcb"
DEVELOPER_USER_IDS = {1240237445841420302}
FREE_BACKUP_LIMIT = 5
PREMIUM_TERM_DAYS = 30
PREMIUM_GRACE_DAYS = 30
INTERVAL_PRESET_HOURS: tuple[int, ...] = (4, 8, 12, 24, 48, 72, 168, 336, 720)
INTERVAL_PRESET_CHOICES = [
    app_commands.Choice(name="4 hours", value=4),
    app_commands.Choice(name="8 hours", value=8),
    app_commands.Choice(name="12 hours", value=12),
    app_commands.Choice(name="24 hours", value=24),
    app_commands.Choice(name="2 days", value=48),
    app_commands.Choice(name="3 days", value=72),
    app_commands.Choice(name="7 days", value=168),
    app_commands.Choice(name="14 days", value=336),
    app_commands.Choice(name="30 days", value=720),
]
PLAN_BACKUP_LIMITS = {
    "free": FREE_BACKUP_LIMIT,
    "pro": 10,
    "pro_plus": 25,
    "pro_ultra": 40,
}
PREMIUM_PLAN_CATALOG: dict[str, dict[str, Any]] = {
    "pro": {
        "display_name": "Pro",
        "price_label": "INR 99/month",
        "badge_label": "Pro",
        "features": (
            "Backup channels, roles, settings",
            "Backup threads and forum posts",
            "Up to 10 backups",
            "2 auto backups/day",
        ),
    },
    "pro_plus": {
        "display_name": "Pro Plus",
        "price_label": "INR 199/month",
        "badge_label": "Pro Plus",
        "features": (
            "Everything in Pro",
            "Up to 25 backups",
            "4 auto backups/day",
            "Priority restore queue",
        ),
    },
    "pro_ultra": {
        "display_name": "Pro Ultra",
        "price_label": "INR 349/month",
        "badge_label": "MAX",
        "features": (
            "Everything in Pro Plus",
            "Up to 40 backups",
            "8 auto backups/day",
            "Advanced sync matrix",
        ),
    },
}
PREMIUM_CARD_THEMES: dict[str, dict[str, Any]] = {
    "free": {"title": "Base Access Card", "metal": "Core", "accent": 0x5B8CFF, "badge": "Core"},
    "pro": {"title": "Bronze Access Card", "metal": "Bronze", "accent": 0xB57A45, "badge": "Bronze"},
    "pro_plus": {"title": "Silver Access Card", "metal": "Silver", "accent": 0xAEB8C7, "badge": "Silver"},
    "pro_ultra": {"title": "Gold Access Card", "metal": "Gold", "accent": 0xD4AF37, "badge": "Gold"},
}

DATA_DIR = Path(__file__).parent / "data"
BACKUP_FILE = DATA_DIR / "backups.json"
SAFETY_FILE = DATA_DIR / "safety.json"
MESSAGE_ARCHIVE_DIR = DATA_DIR / "message_archives"
R2_BACKUP_BUCKET = os.getenv("R2_BACKUP_BUCKET")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL") or (
    f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else None
)
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
R2_REGION = os.getenv("R2_REGION", "auto")
R2_PREFIX = (os.getenv("R2_PREFIX", "clinx-backups") or "clinx-backups").strip("/ ")
DISPLAY_TIMEZONE = os.getenv("DISPLAY_TIMEZONE", "Asia/Calcutta")
DISPLAY_TIMEZONE_LABEL = os.getenv(
    "DISPLAY_TIMEZONE_LABEL",
    "IST" if DISPLAY_TIMEZONE == "Asia/Calcutta" else DISPLAY_TIMEZONE,
)
try:
    DISPLAY_TZ = ZoneInfo(DISPLAY_TIMEZONE)
except Exception:
    DISPLAY_TZ = timezone.utc

EMBED_OK = 0x22C55E
EMBED_WARN = 0xFACC15
EMBED_ERR = 0xFF5A76
EMBED_INFO = 0x5B8CFF

IMPORT_JOBS: dict[int, dict[str, Any]] = {}
BACKUP_LOAD_JOBS: dict[int, dict[str, Any]] = {}
PENDING_SAFETY_REQUESTS: dict[int, dict[str, Any]] = {}
_BACKUP_STORAGE_BACKEND: Any = None
_R2_SHARED_CLIENT: Any = None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_embed(
    title: str,
    description: str,
    color: int = EMBED_INFO,
    interaction: discord.Interaction | None = None,
) -> discord.Embed:
    embed = discord.Embed(
        title=title,
        description=description,
        color=color,
        timestamp=datetime.now(timezone.utc),
    )
    if interaction and interaction.client and interaction.client.user:
        embed.set_author(
            name=f"{interaction.client.user.name} APP",
            icon_url=interaction.client.user.display_avatar.url,
        )
    embed.set_footer(text="CLINX")
    return embed


def ensure_storage() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    MESSAGE_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    if not BACKUP_FILE.exists():
        BACKUP_FILE.write_text(json.dumps({"backups": {}, "users": {}}, indent=2), encoding="utf-8")
    if not SAFETY_FILE.exists():
        SAFETY_FILE.write_text(json.dumps({"guilds": {}}, indent=2), encoding="utf-8")


def slugify_archive_name(raw: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", raw).strip("-_.")
    return cleaned[:80] or "item"


def serialize_message_record(message: discord.Message) -> dict[str, Any]:
    return {
        "id": message.id,
        "type": str(message.type),
        "created_at": message.created_at.isoformat() if message.created_at else None,
        "edited_at": message.edited_at.isoformat() if message.edited_at else None,
        "author": {
            "id": message.author.id,
            "name": str(message.author),
            "display_name": getattr(message.author, "display_name", str(message.author)),
            "bot": message.author.bot,
        },
        "content": message.content,
        "clean_content": message.clean_content,
        "system_content": message.system_content,
        "pinned": message.pinned,
        "jump_url": message.jump_url,
        "mentions": [member.id for member in message.mentions],
        "role_mentions": [role.id for role in message.role_mentions],
        "channel_mentions": [channel.id for channel in message.channel_mentions],
        "attachments": [
            {
                "id": attachment.id,
                "filename": attachment.filename,
                "content_type": attachment.content_type,
                "size": attachment.size,
                "url": attachment.url,
                "proxy_url": attachment.proxy_url,
                "spoiler": attachment.is_spoiler(),
            }
            for attachment in message.attachments
        ],
        "embeds": [embed.to_dict() for embed in message.embeds],
        "stickers": [
            {
                "id": sticker.id,
                "name": sticker.name,
                "format": str(sticker.format),
            }
            for sticker in message.stickers
        ],
        "reactions": [
            {
                "emoji": str(reaction.emoji),
                "count": reaction.count,
            }
            for reaction in message.reactions
        ],
    }


def can_read_channel_history(channel: discord.abc.GuildChannel | discord.Thread, me: discord.Member | None) -> bool:
    if me is None:
        return False
    permissions = channel.permissions_for(me)
    return permissions.view_channel and permissions.read_message_history


async def channel_has_user_messages(channel: discord.TextChannel) -> bool:
    try:
        async for message in channel.history(limit=None, oldest_first=False):
            if not message.author.bot:
                return True
    except (discord.Forbidden, discord.HTTPException):
        return True
    return False


async def collect_archived_threads_for_channel(channel: Any) -> list[discord.Thread]:
    if not hasattr(channel, "archived_threads"):
        return []

    results: list[discord.Thread] = []
    seen_ids: set[int] = set()

    async def consume(**kwargs: Any) -> None:
        try:
            async for thread in channel.archived_threads(limit=None, **kwargs):
                if thread.id in seen_ids:
                    continue
                seen_ids.add(thread.id)
                results.append(thread)
        except TypeError:
            return
        except (discord.Forbidden, discord.HTTPException):
            return

    await consume(private=False)
    await consume(private=True, joined=True)
    return results


async def get_backup_message_targets(guild: discord.Guild) -> list[discord.abc.Messageable]:
    me = guild.me or guild.get_member(bot.user.id if bot.user else 0)
    targets: list[discord.abc.Messageable] = []
    seen_ids: set[int] = set()
    thread_parent_channels: list[Any] = []

    for channel in sorted(guild.text_channels, key=lambda item: (item.position, item.id)):
        if not can_read_channel_history(channel, me):
            continue
        targets.append(channel)
        seen_ids.add(channel.id)
        thread_parent_channels.append(channel)

    for forum_channel in sorted(getattr(guild, "forums", []), key=lambda item: (item.position, item.id)):
        thread_parent_channels.append(forum_channel)

    for media_channel in sorted(getattr(guild, "media_channels", []), key=lambda item: (item.position, item.id)):
        thread_parent_channels.append(media_channel)

    for thread in sorted(guild.threads, key=lambda item: (item.parent_id or 0, item.id)):
        if thread.id in seen_ids or not can_read_channel_history(thread, me):
            continue
        targets.append(thread)
        seen_ids.add(thread.id)

    for parent_channel in thread_parent_channels:
        for thread in await collect_archived_threads_for_channel(parent_channel):
            if thread.id in seen_ids or not can_read_channel_history(thread, me):
                continue
            targets.append(thread)
            seen_ids.add(thread.id)

    return targets


def build_message_archive_r2_key(guild: discord.Guild, archive_name: str) -> str:
    return f"{R2_PREFIX}/message-archives/{guild.id}/{archive_name}"


async def build_message_archive_for_guild(
    guild: discord.Guild,
    *,
    requested_by: discord.abc.User,
    progress_hook: Any = None,
) -> tuple[Path, dict[str, Any]]:
    ensure_storage()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    archive_name = f"{slugify_archive_name(guild.name)}-{guild.id}-{timestamp}.zip"
    archive_path = MESSAGE_ARCHIVE_DIR / archive_name

    targets = await get_backup_message_targets(guild)
    summary: dict[str, Any] = {
        "guild": {
            "id": guild.id,
            "name": guild.name,
            "icon_url": guild.icon.url if guild.icon else None,
        },
        "created_at": utc_now_iso(),
        "requested_by": {
            "id": requested_by.id,
            "name": str(requested_by),
        },
        "channel_count": 0,
        "thread_count": 0,
        "message_count": 0,
        "channels": [],
        "storage": {
            "local_path": str(archive_path),
            "r2_key": None,
        },
    }

    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for index, target in enumerate(targets, start=1):
            parent_name = getattr(getattr(target, "category", None), "name", None)
            if isinstance(target, discord.Thread):
                parent_name = target.parent.name if target.parent else parent_name
            if progress_hook is not None:
                await progress_hook(f"Scanning `{target.name}` ({index}/{len(targets)})")

            messages: list[dict[str, Any]] = []
            async for message in target.history(limit=None, oldest_first=True):
                messages.append(serialize_message_record(message))

            channel_payload = {
                "id": target.id,
                "name": target.name,
                "type": str(target.type),
                "parent": parent_name,
                "message_count": len(messages),
                "messages": messages,
            }
            summary["channels"].append(
                {
                    "id": target.id,
                    "name": target.name,
                    "type": str(target.type),
                    "parent": parent_name,
                    "message_count": len(messages),
                }
            )
            summary["channel_count"] += 1
            if isinstance(target, discord.Thread):
                summary["thread_count"] += 1
            summary["message_count"] += len(messages)

            safe_name = slugify_archive_name(f"{index:03d}-{target.name}-{target.id}")
            archive.writestr(
                f"channels/{safe_name}.json",
                json.dumps(channel_payload, indent=2, ensure_ascii=False),
            )

        archive.writestr("summary.json", json.dumps(summary, indent=2, ensure_ascii=False))

    if is_r2_backup_storage_enabled():
        archive_bytes = archive_path.read_bytes()
        r2_key = build_message_archive_r2_key(guild, archive_path.name)
        write_r2_bytes(r2_key, archive_bytes, content_type="application/zip")
        summary["storage"]["r2_key"] = r2_key

    return archive_path, summary


def normalize_backup_store(store: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    changed = False
    if not isinstance(store.get("backups"), dict):
        store["backups"] = {}
        changed = True

    rebuilt_users: dict[str, dict[str, Any]] = {}
    for backup_id, record in list(store["backups"].items()):
        if not isinstance(record, dict):
            store["backups"][backup_id] = {"id": backup_id}
            record = store["backups"][backup_id]
            changed = True

        if record.get("id") != backup_id:
            record["id"] = backup_id
            changed = True

        owner_id = record.get("created_by_user_id")
        if owner_id is not None:
            owner_id = str(owner_id)
            if record.get("created_by_user_id") != owner_id:
                record["created_by_user_id"] = owner_id
                changed = True

            user_bucket = rebuilt_users.setdefault(
                owner_id,
                {
                    "backup_ids": [],
                    "display_name": record.get("created_by_display_name", "Unknown User"),
                },
            )
            if backup_id not in user_bucket["backup_ids"]:
                user_bucket["backup_ids"].append(backup_id)
            if record.get("created_by_display_name"):
                user_bucket["display_name"] = record["created_by_display_name"]

    if store.get("users") != rebuilt_users:
        store["users"] = rebuilt_users
        changed = True

    return store, changed


def load_backup_store() -> dict[str, Any]:
    ensure_storage()
    store = json.loads(BACKUP_FILE.read_text(encoding="utf-8"))
    store, changed = normalize_backup_store(store)
    if changed:
        save_backup_store(store)
    return store


def save_backup_store(store: dict[str, Any]) -> None:
    ensure_storage()
    store, _ = normalize_backup_store(store)
    BACKUP_FILE.write_text(json.dumps(store, indent=2), encoding="utf-8")


def load_safety_store() -> dict[str, Any]:
    ensure_storage()
    local_store = json.loads(SAFETY_FILE.read_text(encoding="utf-8"))
    local_store, local_changed = normalize_safety_store(local_store)

    if is_r2_backup_storage_enabled():
        try:
            remote_store = read_r2_json(get_safety_store_r2_key(), None)
        except Exception:
            remote_store = None
        if isinstance(remote_store, dict):
            remote_store, remote_changed = normalize_safety_store(remote_store)
            serialized_remote = json.dumps(remote_store, indent=2)
            if remote_changed:
                write_r2_json(get_safety_store_r2_key(), remote_store)
            if not SAFETY_FILE.exists() or SAFETY_FILE.read_text(encoding="utf-8") != serialized_remote:
                SAFETY_FILE.write_text(serialized_remote, encoding="utf-8")
            return remote_store

        serialized_local = json.dumps(local_store, indent=2)
        if local_changed or not SAFETY_FILE.exists() or SAFETY_FILE.read_text(encoding="utf-8") != serialized_local:
            SAFETY_FILE.write_text(serialized_local, encoding="utf-8")
        try:
            write_r2_json(get_safety_store_r2_key(), local_store)
        except Exception:
            pass
        return local_store

    if local_changed:
        SAFETY_FILE.write_text(json.dumps(local_store, indent=2), encoding="utf-8")
    return local_store


def save_safety_store(store: dict[str, Any]) -> None:
    ensure_storage()
    store, _ = normalize_safety_store(store)
    serialized = json.dumps(store, indent=2)
    SAFETY_FILE.write_text(serialized, encoding="utf-8")
    if is_r2_backup_storage_enabled():
        try:
            write_r2_json(get_safety_store_r2_key(), store)
        except Exception:
            pass


def normalize_safety_store(store: dict[str, Any] | Any) -> tuple[dict[str, Any], bool]:
    changed = False
    if not isinstance(store, dict):
        return {"guilds": {}}, True
    if not isinstance(store.get("guilds"), dict):
        store["guilds"] = {}
        changed = True
    guilds = store["guilds"]
    for guild_key in list(guilds.keys()):
        if not isinstance(guilds[guild_key], dict):
            guilds[guild_key] = {}
            changed = True
        try:
            guild_id = int(guild_key)
        except ValueError:
            continue
        before = json.dumps(guilds[guild_key], sort_keys=True)
        get_guild_safety_bucket(store, guild_id)
        after = json.dumps(guilds[guild_key], sort_keys=True)
        if before != after:
            changed = True
    return store, changed


def get_guild_safety_bucket(store: dict[str, Any], guild_id: int) -> dict[str, Any]:
    guild_key = str(guild_id)
    guilds = store.setdefault("guilds", {})
    bucket = guilds.setdefault(
        guild_key,
        {
            "trusted_admin_ids": [],
            "full_access_user_ids": [],
            "full_access_records": {},
            "premium_entitlement": None,
            "backup_interval": None,
        },
    )
    if not isinstance(bucket.get("trusted_admin_ids"), list):
        bucket["trusted_admin_ids"] = []
    if not isinstance(bucket.get("full_access_user_ids"), list):
        bucket["full_access_user_ids"] = []
    if not isinstance(bucket.get("full_access_records"), dict):
        bucket["full_access_records"] = {}
    if bucket.get("premium_entitlement") is not None and not isinstance(bucket.get("premium_entitlement"), dict):
        bucket["premium_entitlement"] = None
    if bucket.get("backup_interval") is not None and not isinstance(bucket.get("backup_interval"), dict):
        bucket["backup_interval"] = None
    bucket["trusted_admin_ids"] = sorted({str(user_id) for user_id in bucket["trusted_admin_ids"]})
    bucket["full_access_user_ids"] = sorted({str(user_id) for user_id in bucket["full_access_user_ids"]})
    normalized_full_access_records: dict[str, dict[str, Any]] = {}
    for user_id in bucket["full_access_user_ids"]:
        raw_record = bucket["full_access_records"].get(user_id, {})
        if not isinstance(raw_record, dict):
            raw_record = {}
        normalized_full_access_records[user_id] = {
            "user_id": user_id,
            "user_display_name": str(raw_record.get("user_display_name") or f"User {user_id}"),
            "granted_at": raw_record.get("granted_at"),
            "granted_by_user_id": str(raw_record.get("granted_by_user_id") or ""),
            "granted_by_display_name": str(raw_record.get("granted_by_display_name") or "Unknown Operator"),
            "guild_name": str(raw_record.get("guild_name") or f"Guild {guild_id}"),
        }
    bucket["full_access_records"] = normalized_full_access_records
    if bucket.get("premium_entitlement") is not None:
        entitlement = dict(bucket["premium_entitlement"])
        if entitlement.get("gifted_to_user_id") is not None:
            entitlement["gifted_to_user_id"] = str(entitlement["gifted_to_user_id"])
        if entitlement.get("gifted_by_user_id") is not None:
            entitlement["gifted_by_user_id"] = str(entitlement["gifted_by_user_id"])
        if entitlement.get("cancelled_by_user_id") is not None:
            entitlement["cancelled_by_user_id"] = str(entitlement["cancelled_by_user_id"])
        entitlement["billing_cycle"] = str(entitlement.get("billing_cycle") or "monthly")
        entitlement["term_days"] = int(entitlement.get("term_days") or (365 if entitlement["billing_cycle"] == "yearly" else PREMIUM_TERM_DAYS))
        entitlement["expires_at"] = entitlement.get("expires_at") or compute_premium_expiry(entitlement.get("gifted_at"), term_days=entitlement["term_days"])
        end_anchor = entitlement.get("cancelled_at") or entitlement["expires_at"]
        entitlement["grace_ends_at"] = entitlement.get("grace_ends_at") or compute_premium_grace_end(end_anchor)
        entitlement["active"] = bool(entitlement.get("active", True)) and entitlement.get("cancelled_at") is None
        entitlement["gifted_to_display_name"] = str(
            entitlement.get("gifted_to_display_name") or f"User {entitlement.get('gifted_to_user_id', 'unknown')}"
        )
        entitlement["gifted_by_display_name"] = str(
            entitlement.get("gifted_by_display_name") or "Unknown Operator"
        )
        entitlement["guild_name"] = str(entitlement.get("guild_name") or f"Guild {guild_id}")
        if entitlement.get("cancelled_by_display_name") is not None:
            entitlement["cancelled_by_display_name"] = str(entitlement["cancelled_by_display_name"])
        bucket["premium_entitlement"] = entitlement
    return bucket


def format_backup_timestamp(created_at: str | None) -> str:
    if not created_at:
        return "Unknown time"
    try:
        dt = datetime.fromisoformat(created_at)
    except ValueError:
        return "Unknown time"
    return dt.astimezone(DISPLAY_TZ).strftime(f"%d %b %Y - %H:%M {DISPLAY_TIMEZONE_LABEL}")


def parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_premium_expiry(gifted_at: str | None, *, term_days: int) -> str:
    start = parse_iso_timestamp(gifted_at) or datetime.now(timezone.utc)
    return (start + timedelta(days=term_days)).isoformat()


def compute_premium_grace_end(end_at: str | None) -> str:
    end_dt = parse_iso_timestamp(end_at) or datetime.now(timezone.utc)
    return (end_dt + timedelta(days=PREMIUM_GRACE_DAYS)).isoformat()


def enrich_premium_entitlement(entitlement: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(entitlement, dict):
        return None

    plan_key = str(entitlement.get("plan_key") or "free")
    plan = PREMIUM_PLAN_CATALOG.get(plan_key)
    billing_cycle = str(entitlement.get("billing_cycle") or "monthly")
    term_days = int(entitlement.get("term_days") or (365 if billing_cycle == "yearly" else PREMIUM_TERM_DAYS))
    gifted_at = entitlement.get("gifted_at") or utc_now_iso()
    expires_at = entitlement.get("expires_at") or compute_premium_expiry(gifted_at, term_days=term_days)
    cancelled_at = entitlement.get("cancelled_at")
    active_flag = bool(entitlement.get("active", True)) and cancelled_at is None
    ended_at = cancelled_at or expires_at
    grace_ends_at = entitlement.get("grace_ends_at") or compute_premium_grace_end(ended_at)
    now = datetime.now(timezone.utc)
    expires_dt = parse_iso_timestamp(expires_at)
    grace_dt = parse_iso_timestamp(grace_ends_at)

    if active_flag and expires_dt and now < expires_dt:
        state = "active"
    elif grace_dt and now < grace_dt:
        state = "grace"
    else:
        state = "expired"

    return {
        **entitlement,
        "plan_key": plan_key,
        "plan_name": str(entitlement.get("plan_name") or (plan["display_name"] if plan else "Unknown Plan")),
        "billing_cycle": billing_cycle,
        "term_days": term_days,
        "gifted_at": gifted_at,
        "expires_at": expires_at,
        "grace_ends_at": grace_ends_at,
        "state": state,
        "active": state == "active",
        "limit": PLAN_BACKUP_LIMITS.get(plan_key, PLAN_BACKUP_LIMITS["free"]),
    }


def get_backup_vault_policy_for_guild(guild_id: int) -> dict[str, Any]:
    entitlement = enrich_premium_entitlement(get_guild_premium_entitlement(guild_id))
    policy: dict[str, Any] = {
        "state": "free",
        "creation_limit": PLAN_BACKUP_LIMITS["free"],
        "plan_limit": PLAN_BACKUP_LIMITS["free"],
        "plan_label": "Free",
        "badge_label": "Free",
        "billing_cycle": None,
        "expires_at": None,
        "grace_ends_at": None,
        "status_text": f"Free vault • up to {PLAN_BACKUP_LIMITS['free']} backups",
    }
    if entitlement is None:
        return policy

    plan_label = str(entitlement.get("plan_name") or "Premium")
    state = str(entitlement.get("state") or "free")
    plan_limit = int(entitlement.get("limit") or PLAN_BACKUP_LIMITS["free"])
    creation_limit = plan_limit if state == "active" else PLAN_BACKUP_LIMITS["free"]
    badge_label = (
        plan_label if state == "active" else
        f"{plan_label} Grace" if state == "grace" else
        f"{plan_label} Expired"
    )
    status_text = (
        f"{plan_label} monthly active • Renews {format_backup_timestamp(entitlement.get('expires_at'))}"
        if state == "active" else
        f"{plan_label} ended • Grace until {format_backup_timestamp(entitlement.get('grace_ends_at'))}"
        if state == "grace" else
        f"{plan_label} expired • Extra backups above free cap are now at risk"
    )
    return {
        "state": state,
        "creation_limit": creation_limit,
        "plan_limit": plan_limit,
        "plan_label": plan_label,
        "badge_label": badge_label,
        "billing_cycle": entitlement.get("billing_cycle"),
        "expires_at": entitlement.get("expires_at"),
        "grace_ends_at": entitlement.get("grace_ends_at"),
        "status_text": status_text,
        "entitlement": entitlement,
    }


def compute_at_risk_backup_ids(entries: list[dict[str, Any]], vault_policy: dict[str, Any]) -> set[str]:
    if vault_policy.get("state") != "expired":
        return set()
    limit = PLAN_BACKUP_LIMITS["free"]
    overflow = max(0, len(entries) - limit)
    if overflow <= 0:
        return set()
    oldest_entries = sorted(entries, key=lambda entry: entry.get("created_at", ""))[:overflow]
    return {str(entry.get("id")) for entry in oldest_entries if entry.get("id")}


def format_vault_storage_state(vault_policy: dict[str, Any], *, at_risk_count: int) -> str:
    state = str(vault_policy.get("state") or "free")
    if state == "active":
        return str(vault_policy.get("status_text") or "Vault active.")
    if state == "grace":
        return (
            str(vault_policy.get("status_text") or "Premium grace window active.")
            + "\nExtra backups are safe for now, but new backup creation is limited to the free cap until renewal."
        )
    if at_risk_count > 0:
        return (
            str(vault_policy.get("status_text") or "Premium expired.")
            + f"\n`{at_risk_count}` backup(s) are now at risk until the vault drops back to the free cap."
        )
    return str(vault_policy.get("status_text") or "Vault is running on the free cap.")


def format_backup_retention_label(
    backup_id: str | None,
    vault_policy: dict[str, Any],
    *,
    at_risk_ids: set[str],
) -> str:
    state = str(vault_policy.get("state") or "free")
    if state in {"free", "active"}:
        return "Stored Until: **Until deleted**"
    if state == "grace":
        return f"Stored Until: **Grace until {format_backup_timestamp(vault_policy.get('grace_ends_at'))}**"
    if backup_id and backup_id in at_risk_ids:
        return "Stored Until: **At risk now - renew premium or trim the vault to the free cap**"
    return "Stored Until: **Retained inside the free cap**"


def format_relative_timestamp(value: str | None) -> str:
    target = parse_iso_timestamp(value)
    if target is None:
        return "Not scheduled"
    now = datetime.now(timezone.utc)
    delta = target - now
    seconds = int(delta.total_seconds())
    suffix = "from now" if seconds >= 0 else "ago"
    seconds = abs(seconds)
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes or not parts:
        parts.append(f"{minutes}m")
    return f"{' '.join(parts)} {suffix}"


def format_interval_label(interval_hours: int | None) -> str:
    if not interval_hours:
        return "Off"
    if interval_hours % 24 == 0:
        days = interval_hours // 24
        return f"{days} day" + ("s" if days != 1 else "")
    return f"{interval_hours} hour" + ("s" if interval_hours != 1 else "")


def get_user_backup_entries(store: dict[str, Any], user_id: int) -> list[dict[str, Any]]:
    owner_id = str(user_id)
    backup_ids = store.get("users", {}).get(owner_id, {}).get("backup_ids", [])
    entries = [store["backups"][backup_id] for backup_id in backup_ids if backup_id in store.get("backups", {})]
    return sorted(entries, key=lambda entry: entry.get("created_at", ""), reverse=True)


def strip_backup_snapshot(record: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(record)
    metadata.pop("snapshot", None)
    return metadata


def build_backup_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    roles = sorted(snapshot.get("roles", []), key=lambda item: item.get("position", 0), reverse=True)
    categories = sorted(snapshot.get("categories", []), key=lambda item: item.get("position", 0))
    channels = sorted(
        snapshot.get("channels", []),
        key=lambda item: ((item.get("category") or "~"), item.get("position", 0), item.get("name", "")),
    )

    category_buckets: dict[str | None, list[dict[str, Any]]] = {}
    for channel in channels:
        category_buckets.setdefault(channel.get("category"), []).append(channel)

    structure_lines: list[str] = []
    for category in categories:
        category_name = str(category.get("name", "unnamed-category"))
        structure_lines.append(f"[ {category_name} ]")
        for channel in category_buckets.get(category_name, []):
            prefix = "#" if channel.get("type") == "text" else "<"
            structure_lines.append(f"  {prefix} {channel.get('name', 'unnamed-channel')}")

    uncategorized = category_buckets.get(None, [])
    if uncategorized:
        structure_lines.append("[ uncategorized ]")
        for channel in uncategorized:
            prefix = "#" if channel.get("type") == "text" else "<"
            structure_lines.append(f"  {prefix} {channel.get('name', 'unnamed-channel')}")

    role_lines = [str(role.get("name", "unnamed-role")) for role in roles]
    structure_preview = structure_lines[:36]
    role_preview = role_lines[:36]

    return {
        "roles_count": len(roles),
        "categories_count": len(categories),
        "channels_count": len(channels),
        "structure_preview": structure_preview,
        "structure_overflow": max(0, len(structure_lines) - len(structure_preview)),
        "role_preview": role_preview,
        "role_overflow": max(0, len(role_lines) - len(role_preview)),
    }


def ensure_backup_summary(record: dict[str, Any]) -> bool:
    if isinstance(record.get("summary"), dict):
        return False
    snapshot = record.get("snapshot")
    if not isinstance(snapshot, dict):
        return False
    record["summary"] = build_backup_summary(snapshot)
    return True


def is_r2_backup_storage_enabled() -> bool:
    return all([R2_BACKUP_BUCKET, R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY])


def get_r2_client() -> Any:
    global _R2_SHARED_CLIENT
    if _R2_SHARED_CLIENT is None:
        import boto3

        _R2_SHARED_CLIENT = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT_URL,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name=R2_REGION,
        )
    return _R2_SHARED_CLIENT


def get_safety_store_r2_key() -> str:
    return f"{R2_PREFIX}/system/safety.json"


def read_r2_json(key: str, default: Any) -> Any:
    from botocore.exceptions import ClientError

    client = get_r2_client()
    try:
        response = client.get_object(Bucket=R2_BACKUP_BUCKET, Key=key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"NoSuchKey", "404", "NotFound"}:
            return default
        raise
    body = response["Body"].read()
    return json.loads(body.decode("utf-8"))


def write_r2_json(key: str, payload: Any) -> None:
    client = get_r2_client()
    client.put_object(
        Bucket=R2_BACKUP_BUCKET,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def write_r2_bytes(key: str, payload: bytes, *, content_type: str = "application/octet-stream") -> None:
    client = get_r2_client()
    client.put_object(
        Bucket=R2_BACKUP_BUCKET,
        Key=key,
        Body=payload,
        ContentType=content_type,
    )


class LocalBackupStorageBackend:
    def list_user_backups(self, user_id: int) -> list[dict[str, Any]]:
        store = load_backup_store()
        changed = False
        entries: list[dict[str, Any]] = []
        for record in get_user_backup_entries(store, user_id):
            if ensure_backup_summary(record):
                changed = True
            entries.append(strip_backup_snapshot(record))
        if changed:
            save_backup_store(store)
        return entries

    def get_user_backup(self, user_id: int, backup_id: str) -> dict[str, Any] | None:
        store = load_backup_store()
        record = store.get("backups", {}).get(backup_id)
        if record is None or not can_access_backup(record, user_id):
            return None
        if ensure_backup_summary(record):
            save_backup_store(store)
        return dict(record)

    def save_backup(self, record: dict[str, Any]) -> None:
        ensure_backup_summary(record)
        store = load_backup_store()
        store.setdefault("backups", {})[record["id"]] = record
        save_backup_store(store)

    def delete_user_backup(self, user_id: int, backup_id: str) -> bool:
        store = load_backup_store()
        record = store.get("backups", {}).get(backup_id)
        if record is None or not can_access_backup(record, user_id):
            return False
        del store["backups"][backup_id]
        save_backup_store(store)
        return True


class R2BackupStorageBackend:
    def __init__(self) -> None:
        self._client: Any = None

    def _get_client(self) -> Any:
        if self._client is None:
            self._client = get_r2_client()
        return self._client

    def _manifest_key(self, user_id: int | str) -> str:
        return f"{R2_PREFIX}/users/{user_id}/manifest.json"

    def _backup_key(self, user_id: int | str, backup_id: str) -> str:
        return f"{R2_PREFIX}/users/{user_id}/backups/{backup_id}.json"

    def _read_json(self, key: str, default: Any) -> Any:
        return read_r2_json(key, default)

    def _write_json(self, key: str, payload: Any) -> None:
        write_r2_json(key, payload)

    def _delete_key(self, key: str) -> None:
        client = self._get_client()
        client.delete_object(Bucket=R2_BACKUP_BUCKET, Key=key)

    def _load_manifest(self, user_id: int) -> dict[str, Any]:
        manifest = self._read_json(self._manifest_key(user_id), {"user_id": str(user_id), "backups": []})
        if not isinstance(manifest, dict):
            return {"user_id": str(user_id), "backups": []}
        if not isinstance(manifest.get("backups"), list):
            manifest["backups"] = []
        manifest["user_id"] = str(user_id)
        return manifest

    def list_user_backups(self, user_id: int) -> list[dict[str, Any]]:
        manifest = self._load_manifest(user_id)
        entries = [entry for entry in manifest.get("backups", []) if isinstance(entry, dict)]
        return sorted(entries, key=lambda entry: entry.get("created_at", ""), reverse=True)

    def get_user_backup(self, user_id: int, backup_id: str) -> dict[str, Any] | None:
        manifest_entries = self.list_user_backups(user_id)
        metadata = next((entry for entry in manifest_entries if entry.get("id") == backup_id), None)
        if metadata is None:
            return None
        record = self._read_json(self._backup_key(user_id, backup_id), None)
        if not isinstance(record, dict):
            return None
        if "summary" not in record and isinstance(metadata.get("summary"), dict):
            record["summary"] = metadata["summary"]
        return record

    def save_backup(self, record: dict[str, Any]) -> None:
        user_id = int(record["created_by_user_id"])
        ensure_backup_summary(record)
        manifest = self._load_manifest(user_id)
        metadata = strip_backup_snapshot(record)
        manifest["backups"] = [entry for entry in manifest["backups"] if entry.get("id") != record["id"]]
        manifest["backups"].append(metadata)
        manifest["backups"] = sorted(manifest["backups"], key=lambda entry: entry.get("created_at", ""), reverse=True)
        self._write_json(self._backup_key(user_id, record["id"]), record)
        self._write_json(self._manifest_key(user_id), manifest)

    def delete_user_backup(self, user_id: int, backup_id: str) -> bool:
        manifest = self._load_manifest(user_id)
        before = len(manifest["backups"])
        manifest["backups"] = [entry for entry in manifest["backups"] if entry.get("id") != backup_id]
        if len(manifest["backups"]) == before:
            return False
        self._delete_key(self._backup_key(user_id, backup_id))
        self._write_json(self._manifest_key(user_id), manifest)
        return True


def get_backup_storage_backend() -> Any:
    global _BACKUP_STORAGE_BACKEND
    if _BACKUP_STORAGE_BACKEND is None:
        _BACKUP_STORAGE_BACKEND = R2BackupStorageBackend() if is_r2_backup_storage_enabled() else LocalBackupStorageBackend()
    return _BACKUP_STORAGE_BACKEND


async def list_user_backup_entries_async(user_id: int) -> list[dict[str, Any]]:
    return await asyncio.to_thread(get_backup_storage_backend().list_user_backups, user_id)


async def get_user_backup_record_async(user_id: int, backup_id: str) -> dict[str, Any] | None:
    return await asyncio.to_thread(get_backup_storage_backend().get_user_backup, user_id, backup_id)


async def save_backup_record_async(record: dict[str, Any]) -> None:
    await asyncio.to_thread(get_backup_storage_backend().save_backup, record)


async def delete_user_backup_async(user_id: int, backup_id: str) -> bool:
    return await asyncio.to_thread(get_backup_storage_backend().delete_user_backup, user_id, backup_id)


def can_access_backup(record: dict[str, Any], user_id: int) -> bool:
    return str(record.get("created_by_user_id", "")) == str(user_id)


def build_backup_choice_label(record: dict[str, Any]) -> str:
    source_name = record.get("source_guild_name", "Unknown Source")
    timestamp = format_backup_timestamp(record.get("created_at"))
    label = f"{source_name} | {timestamp} ({record.get('id', 'unknown')})"
    return label[:100]


async def trim_interval_backups_for_owner(
    owner_user_id: int,
    *,
    guild_id: int,
    keep_count: int,
    backup_limit: int,
) -> tuple[bool, int]:
    entries = await list_user_backup_entries_async(owner_user_id)
    interval_entries = [
        entry
        for entry in entries
        if (entry.get("auto_backup") or {}).get("type") == "interval"
        and str((entry.get("auto_backup") or {}).get("guild_id")) == str(guild_id)
    ]
    interval_entries = sorted(interval_entries, key=lambda entry: entry.get("created_at", ""))
    needed_slots = max(0, len(entries) + 1 - backup_limit)
    needed_keep_trim = max(0, len(interval_entries) + 1 - keep_count)
    delete_count = max(needed_slots, needed_keep_trim)
    if delete_count == 0:
        return True, 0
    if len(interval_entries) < delete_count:
        return False, 0
    deleted = 0
    for entry in interval_entries[:delete_count]:
        if await delete_user_backup_async(owner_user_id, entry["id"]):
            deleted += 1
    return deleted == delete_count, deleted


async def create_backup_record_for_owner(
    source: discord.Guild,
    *,
    owner_user_id: int,
    owner_display_name: str,
    include_assets: bool = True,
    auto_backup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = await build_guild_snapshot(source, include_assets=include_assets)
    backup_id = f"BKP-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{secrets.token_hex(3).upper()}"
    record = {
        "id": backup_id,
        "created_at": utc_now_iso(),
        "created_by_user_id": str(owner_user_id),
        "created_by_display_name": owner_display_name,
        "source_guild_id": source.id,
        "source_guild_name": source.name,
        "summary": build_backup_summary(snapshot),
        "snapshot": snapshot,
    }
    if auto_backup:
        record["auto_backup"] = auto_backup
    await save_backup_record_async(record)
    return record


def channel_signature(*, name: str, category: str | None, kind: str) -> tuple[str, str, str]:
    return kind, category or "", name


def snapshot_channel_signature(data: dict[str, Any]) -> tuple[str, str, str]:
    return channel_signature(
        name=str(data.get("name", "")),
        category=data.get("category"),
        kind=str(data.get("type", "text")),
    )


def live_channel_signature(channel: discord.abc.GuildChannel) -> tuple[str, str, str]:
    kind = "voice" if isinstance(channel, discord.VoiceChannel) else "text"
    category_name = getattr(getattr(channel, "category", None), "name", None)
    return channel_signature(name=channel.name, category=category_name, kind=kind)


def build_progress_bar(percent: int, *, width: int = 12) -> str:
    safe_percent = max(0, min(100, percent))
    filled = round((safe_percent / 100) * width)
    return f"[{'█' * filled}{'░' * (width - filled)}]"


def build_transit_meter(percent: int, *, width: int = 44) -> str:
    safe_percent = max(0, min(100, percent))
    filled = round((safe_percent / 100) * width)
    rail = ["━"] * width
    for idx in range(filled):
        rail[idx] = "█"
    head_index = min(width - 1, max(0, filled - 1 if filled else 0))
    rail[head_index] = "◉" if safe_percent < 100 else "◆"
    return f"╭{'─' * width}╮\n│{''.join(rail)}│\n╰{'─' * width}╯"


def get_backup_phase_sequence(selected_actions: set[str]) -> list[str]:
    steps = ["Queued"]
    if "delete_channels" in selected_actions:
        steps.append("Wiping Channels")
    if "load_channels" in selected_actions:
        steps.extend(["Scaffolding Categories", "Scaffolding Channels"])
    if "delete_roles" in selected_actions:
        steps.append("Wiping Roles")
    if "load_roles" in selected_actions:
        steps.append("Rebuilding Roles")
    if "load_channels" in selected_actions:
        steps.append("Finalizing Channels")
    if "load_settings" in selected_actions:
        steps.append("Syncing Server Settings")
    steps.append("Completed")
    return steps


def get_backup_progress_state(job: dict[str, Any]) -> tuple[int, str, int, int]:
    selected_actions = set(job.get("selected_actions", []))
    phases = get_backup_phase_sequence(selected_actions)
    status = job.get("status")
    current_phase = job.get("phase", "Queued")
    if status in {"failed", "cancelled"}:
        current_phase = job.get("last_progress_phase") or job.get("phase") or "Queued"
    if status == "completed":
        index = len(phases) - 1
    else:
        try:
            index = phases.index(current_phase)
        except ValueError:
            index = 0
    total_stages = max(1, len(phases) - 1)
    percent = int(round((index / total_stages) * 100))
    if status == "completed":
        percent = 100
    return percent, build_transit_meter(percent), min(index + 1, len(phases)), len(phases)


async def run_limited(tasks: list[Any], *, limit: int = 6) -> list[Any]:
    semaphore = asyncio.Semaphore(limit)

    async def runner(coro: Any) -> Any:
        async with semaphore:
            return await coro

    return await asyncio.gather(*(runner(task) for task in tasks))


def chunk_items(items: list[Any], size: int) -> list[list[Any]]:
    return [items[index:index + size] for index in range(0, len(items), size)]


async def backup_id_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    entries = await list_user_backup_entries_async(interaction.user.id)
    query = current.casefold().strip()
    choices: list[app_commands.Choice[str]] = []
    for record in entries:
        haystack = " ".join(
            [
                str(record.get("id", "")),
                str(record.get("source_guild_name", "")),
                format_backup_timestamp(record.get("created_at")),
            ]
        ).casefold()
        if query and query not in haystack:
            continue
        choices.append(app_commands.Choice(name=build_backup_choice_label(record), value=record["id"]))
        if len(choices) >= 25:
            break
    return choices


def build_backup_load_status_description(job: dict[str, Any]) -> str:
    desc = (
        f"Status: `{job.get('status', 'unknown')}`\n"
        f"Backup ID: `{job.get('backup_id', 'n/a')}`\n"
        f"Started: `{job.get('started_at', 'n/a')}`"
    )
    if job.get("source_name") and job.get("target_name"):
        desc += f"\nRoute: `{job['source_name']}` -> `{job['target_name']}`"
    if job.get("finished_at"):
        desc += f"\nFinished: `{job['finished_at']}`"
    if job.get("phase"):
        desc += f"\nPhase: `{job['phase']}`"
    if job.get("stats"):
        stats = job["stats"]
        desc += (
            f"\n\nDeleted roles: `{stats.get('deleted_roles', 0)}`"
            f"\nDeleted channels: `{stats.get('deleted_channels', 0)}`"
            f"\nCreated roles: `{stats.get('created_roles', 0)}`"
            f"\nUpdated roles: `{stats.get('updated_roles', 0)}`"
            f"\nCreated categories: `{stats.get('created_categories', 0)}`"
            f"\nCreated channels: `{stats.get('created_channels', 0)}`"
            f"\nUpdated channels: `{stats.get('updated_channels', 0)}`"
            f"\nUpdated settings: `{stats.get('updated_settings', 0)}`"
        )
        if stats.get("blocked_roles"):
            desc += f"\nBlocked roles: `{stats.get('blocked_roles', 0)}`"
    if job.get("error"):
        desc += f"\nError: `{job['error']}`"
    return desc


def is_guild_owner(user: discord.abc.User, guild: discord.Guild | None) -> bool:
    return guild is not None and guild.owner_id == user.id


def is_trusted_admin(user_id: int, guild_id: int) -> bool:
    store = load_safety_store()
    bucket = get_guild_safety_bucket(store, guild_id)
    return str(user_id) in bucket.get("trusted_admin_ids", [])


def has_full_command_access(user_id: int, guild_id: int) -> bool:
    store = load_safety_store()
    bucket = get_guild_safety_bucket(store, guild_id)
    return str(user_id) in bucket.get("full_access_user_ids", [])


def get_guild_premium_entitlement(guild_id: int) -> dict[str, Any] | None:
    store = load_safety_store()
    bucket = get_guild_safety_bucket(store, guild_id)
    entitlement = bucket.get("premium_entitlement")
    return enrich_premium_entitlement(entitlement if isinstance(entitlement, dict) else None)


def set_guild_premium_entitlement(
    guild: discord.Guild,
    gifted_to_user_id: int,
    gifted_by_user_id: int,
    plan_key: str,
    *,
    gifted_to_display_name: str | None = None,
    gifted_by_display_name: str | None = None,
) -> dict[str, Any]:
    store = load_safety_store()
    bucket = get_guild_safety_bucket(store, guild.id)
    plan = PREMIUM_PLAN_CATALOG[plan_key]
    gifted_at = utc_now_iso()
    billing_cycle = "monthly"
    term_days = PREMIUM_TERM_DAYS
    expires_at = compute_premium_expiry(gifted_at, term_days=term_days)
    entitlement = {
        "plan_key": plan_key,
        "plan_name": plan["display_name"],
        "gifted_to_user_id": str(gifted_to_user_id),
        "gifted_to_display_name": gifted_to_display_name or f"User {gifted_to_user_id}",
        "gifted_by_user_id": str(gifted_by_user_id),
        "gifted_by_display_name": gifted_by_display_name or "Unknown Operator",
        "gifted_at": gifted_at,
        "billing_cycle": billing_cycle,
        "term_days": term_days,
        "expires_at": expires_at,
        "grace_ends_at": compute_premium_grace_end(expires_at),
        "guild_name": guild.name,
        "active": True,
        "cancelled_at": None,
        "cancelled_by_user_id": None,
        "cancelled_by_display_name": None,
    }
    bucket["premium_entitlement"] = entitlement
    save_safety_store(store)
    return enrich_premium_entitlement(entitlement) or entitlement


def cancel_guild_premium_entitlement(
    guild_id: int,
    *,
    cancelled_by_user_id: int,
    cancelled_by_display_name: str,
) -> dict[str, Any] | None:
    store = load_safety_store()
    bucket = get_guild_safety_bucket(store, guild_id)
    entitlement = bucket.get("premium_entitlement")
    if not isinstance(entitlement, dict):
        return None
    entitlement = dict(entitlement)
    entitlement["active"] = False
    entitlement["cancelled_at"] = utc_now_iso()
    entitlement["grace_ends_at"] = compute_premium_grace_end(entitlement["cancelled_at"])
    entitlement["cancelled_by_user_id"] = str(cancelled_by_user_id)
    entitlement["cancelled_by_display_name"] = cancelled_by_display_name
    bucket["premium_entitlement"] = entitlement
    save_safety_store(store)
    return enrich_premium_entitlement(entitlement)


def get_backup_interval_config(guild_id: int) -> dict[str, Any] | None:
    store = load_safety_store()
    bucket = get_guild_safety_bucket(store, guild_id)
    config = bucket.get("backup_interval")
    return dict(config) if isinstance(config, dict) else None


def set_backup_interval_config(
    guild: discord.Guild,
    *,
    owner_user_id: int,
    owner_display_name: str,
    interval_hours: int,
    keep_count: int,
) -> dict[str, Any]:
    now = utc_now_iso()
    next_run = datetime.now(timezone.utc).timestamp() + (interval_hours * 3600)
    config = {
        "enabled": True,
        "owner_user_id": str(owner_user_id),
        "owner_display_name": owner_display_name,
        "interval_hours": int(interval_hours),
        "keep_count": int(keep_count),
        "created_at": now,
        "updated_at": now,
        "next_run_at": datetime.fromtimestamp(next_run, timezone.utc).isoformat(),
        "last_run_at": None,
        "last_success_at": None,
        "last_backup_id": None,
        "last_error": None,
    }
    store = load_safety_store()
    bucket = get_guild_safety_bucket(store, guild.id)
    previous = bucket.get("backup_interval")
    if isinstance(previous, dict):
        config["created_at"] = previous.get("created_at") or now
        config["last_run_at"] = previous.get("last_run_at")
        config["last_success_at"] = previous.get("last_success_at")
        config["last_backup_id"] = previous.get("last_backup_id")
    bucket["backup_interval"] = config
    save_safety_store(store)
    return config


def disable_backup_interval_config(guild: discord.Guild) -> dict[str, Any] | None:
    store = load_safety_store()
    bucket = get_guild_safety_bucket(store, guild.id)
    config = bucket.get("backup_interval")
    if not isinstance(config, dict):
        return None
    config = dict(config)
    config["enabled"] = False
    config["updated_at"] = utc_now_iso()
    config["next_run_at"] = None
    bucket["backup_interval"] = config
    save_safety_store(store)
    return config


def update_backup_interval_runtime(guild_id: int, **updates: Any) -> dict[str, Any] | None:
    store = load_safety_store()
    bucket = get_guild_safety_bucket(store, guild_id)
    config = bucket.get("backup_interval")
    if not isinstance(config, dict):
        return None
    config = dict(config)
    config.update(updates)
    config["updated_at"] = utc_now_iso()
    bucket["backup_interval"] = config
    save_safety_store(store)
    return config


def get_backup_limit_for_guild(guild_id: int) -> tuple[int, str]:
    policy = get_backup_vault_policy_for_guild(guild_id)
    return int(policy["creation_limit"]), str(policy["badge_label"])


def normalize_premium_plan(raw_plan: str | None) -> str | None:
    token = (raw_plan or "").casefold().strip()
    token = token.replace(" ", "").replace("-", "").replace("_", "")
    token = token.replace("premiuim", "premium")
    token = token.replace("premium(", "").replace(")", "")
    aliases = {
        "pro": "pro",
        "premiumpro": "pro",
        "plus": "pro_plus",
        "proplus": "pro_plus",
        "premiumplus": "pro_plus",
        "ultra": "pro_ultra",
        "proultra": "pro_ultra",
        "premiumultra": "pro_ultra",
        "max": "pro_ultra",
        "premiummax": "pro_ultra",
    }
    return aliases.get(token)


def format_actor_label(display_name: str | None, user_id: str | int | None) -> str:
    clean_name = (display_name or "Unknown User").strip()
    if user_id in (None, ""):
        return clean_name
    return f"{clean_name} (`{user_id}`)"


def build_developer_dashboard_entries(
    bot_instance: commands.Bot,
    *,
    developer_id: int,
    mode: str,
) -> list[dict[str, Any]]:
    store = load_safety_store()
    entries: list[dict[str, Any]] = []
    for guild_key in list(store.get("guilds", {}).keys()):
        try:
            guild_id = int(guild_key)
        except ValueError:
            continue
        bucket = get_guild_safety_bucket(store, guild_id)
        guild = bot_instance.get_guild(guild_id)
        fallback_guild_name = f"Guild {guild_id}"
        premium_bucket = bucket.get("premium_entitlement")
        guild_name = guild.name if guild else (
            str(premium_bucket.get("guild_name")) if isinstance(premium_bucket, dict) and premium_bucket.get("guild_name") else fallback_guild_name
        )

        if mode == "obypass":
            records = bucket.get("full_access_records", {})
            for user_id in bucket.get("full_access_user_ids", []):
                record = records.get(user_id, {})
                entries.append(
                    {
                        "key": f"obypass:{guild_id}:{user_id}",
                        "kind": "obypass",
                        "guild_id": guild_id,
                        "guild_name": str(record.get("guild_name") or guild_name),
                        "user_id": str(user_id),
                        "user_display_name": str(record.get("user_display_name") or f"User {user_id}"),
                        "granted_at": record.get("granted_at"),
                        "granted_by_user_id": str(record.get("granted_by_user_id") or ""),
                        "granted_by_display_name": str(record.get("granted_by_display_name") or "Unknown Operator"),
                    }
                )
            continue

        entitlement = enrich_premium_entitlement(bucket.get("premium_entitlement") if isinstance(bucket.get("premium_entitlement"), dict) else None)
        if not isinstance(entitlement, dict):
            continue
        if str(entitlement.get("gifted_by_user_id") or "") != str(developer_id):
            continue
        entries.append(
            {
                "key": f"premium:{guild_id}",
                "kind": "premium",
                "guild_id": guild_id,
                "guild_name": str(entitlement.get("guild_name") or guild_name),
                "plan_key": str(entitlement.get("plan_key") or "free"),
                "plan_name": str(entitlement.get("plan_name") or "Unknown Plan"),
                "gifted_to_user_id": str(entitlement.get("gifted_to_user_id") or ""),
                "gifted_to_display_name": str(entitlement.get("gifted_to_display_name") or "Unknown User"),
                "gifted_by_user_id": str(entitlement.get("gifted_by_user_id") or ""),
                "gifted_by_display_name": str(entitlement.get("gifted_by_display_name") or "Unknown Operator"),
                "gifted_at": entitlement.get("gifted_at"),
                "active": bool(entitlement.get("active", True)),
                "state": str(entitlement.get("state") or "expired"),
                "billing_cycle": str(entitlement.get("billing_cycle") or "monthly"),
                "expires_at": entitlement.get("expires_at"),
                "grace_ends_at": entitlement.get("grace_ends_at"),
                "cancelled_at": entitlement.get("cancelled_at"),
                "cancelled_by_user_id": str(entitlement.get("cancelled_by_user_id") or ""),
                "cancelled_by_display_name": str(entitlement.get("cancelled_by_display_name") or ""),
            }
        )

    def sort_key(entry: dict[str, Any]) -> tuple[float, str]:
        timestamp_key = "granted_at" if entry["kind"] == "obypass" else "gifted_at"
        dt = parse_iso_timestamp(entry.get(timestamp_key))
        return (dt.timestamp() if dt else 0.0, entry.get("guild_name", ""))

    return sorted(entries, key=sort_key, reverse=True)


def has_administrator(user: discord.abc.User) -> bool:
    return isinstance(user, discord.Member) and user.guild_permissions.administrator


def is_developer_user(user: discord.abc.User) -> bool:
    return user.id in DEVELOPER_USER_IDS


async def send_developer_interaction_denied(interaction: discord.Interaction) -> None:
    embed = make_embed("Developer Only", "This CLINX developer command is locked to the CLINX developer account.", EMBED_ERR)
    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


DEV_VISIBILITY_CHOICES = [
    app_commands.Choice(name="Private", value="private"),
    app_commands.Choice(name="Public", value="public"),
]


def resolve_dev_visibility(visibility: app_commands.Choice[str] | None, *, default_private: bool = True) -> bool:
    if visibility is None:
        return default_private
    return visibility.value != "public"


async def send_temp_interaction_notice_nowait(
    interaction: discord.Interaction,
    title: str,
    description: str,
    color: int,
    *,
    delay_seconds: float = 3.0,
) -> None:
    embed = make_embed(title, description, color)
    if interaction.response.is_done():
        message = await interaction.followup.send(embed=embed, wait=True)
    else:
        await interaction.response.send_message(embed=embed)
        message = await interaction.original_response()

    async def _delete_later() -> None:
        await asyncio.sleep(delay_seconds)
        try:
            await message.delete()
        except (discord.Forbidden, discord.HTTPException):
            return

    asyncio.create_task(_delete_later(), name="clinx-temp-interaction-notice-delete")


def get_command_safety_tier(command_name: str, *, selected_actions: set[str] | None = None, destructive: bool = False) -> int:
    if command_name in {"help", "invite"}:
        return 0
    if command_name in {
        "backup_create",
        "backup_list",
        "backup_delete",
        "export_guild",
        "export_channels",
        "export_roles",
        "export_channel",
        "export_role",
        "export_message",
        "export_reactions",
    }:
        return 1
    if command_name in {"restore_missing", "import_guild"}:
        return 2
    if command_name == "backup_load":
        if selected_actions and {"delete_roles", "delete_channels"} & selected_actions:
            return 3
        return 2
    if command_name in {"leave", "safety_grant", "safety_revoke", "safety_list", "cleantoday", "cleanempty"}:
        return 4
    return 1


def require_clinx_access(
    interaction: discord.Interaction,
    command_name: str,
    *,
    selected_actions: set[str] | None = None,
    destructive: bool = False,
) -> tuple[str, int, str | None]:
    guild = interaction.guild
    tier = get_command_safety_tier(command_name, selected_actions=selected_actions, destructive=destructive)
    if tier == 0:
        return "direct", tier, None
    if guild is None:
        return "deny", tier, "This command must be used inside a server."
    if is_developer_user(interaction.user):
        return "direct", tier, None
    if is_guild_owner(interaction.user, guild):
        return "direct", tier, None
    if has_full_command_access(interaction.user.id, guild.id) and tier < 4:
        return "direct", tier, None
    if tier == 4:
        return "deny", tier, "Only the actual server owner can use this command."
    if is_trusted_admin(interaction.user.id, guild.id):
        return "direct", tier, None
    if tier == 1:
        if has_administrator(interaction.user):
            return "direct", tier, None
        return "deny", tier, "CLINX allows this command only for server admins, trusted admins, or the server owner."
    if has_administrator(interaction.user):
        return "approval", tier, None
    return "deny", tier, "CLINX requires administrator permissions for this command. Untrusted admins must go through owner approval."


def build_preview_lines(preview: dict[str, int]) -> tuple[list[str], list[str]]:
    delete_lines: list[str] = []
    build_lines: list[str] = []
    if preview.get("deleted_roles"):
        delete_lines.append(f"- `{preview['deleted_roles']}` roles")
    if preview.get("deleted_channels"):
        delete_lines.append(f"- `{preview['deleted_channels']}` channels")
    if preview.get("created_roles"):
        build_lines.append(f"- `{preview['created_roles']}` roles")
    if preview.get("updated_roles"):
        build_lines.append(f"- `{preview['updated_roles']}` role updates")
    if preview.get("created_categories"):
        build_lines.append(f"- `{preview['created_categories']}` categories")
    if preview.get("created_channels"):
        build_lines.append(f"- `{preview['created_channels']}` channels")
    if preview.get("updated_channels"):
        build_lines.append(f"- `{preview['updated_channels']}` channel updates")
    if preview.get("updated_settings"):
        build_lines.append("- server settings sync")
    return delete_lines or ["- none"], build_lines or ["- none"]


@dataclass
class ParsedChannel:
    kind: str
    name: str
    topic: str | None
    category: str | None


@dataclass(frozen=True)
class CommandLibraryEntry:
    path: str
    summary: str
    detail: str
    visibility: str = "Public"
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class CommandLibraryLane:
    key: str
    label: str
    emoji: str
    accent: int
    blurb: str
    entries: tuple[CommandLibraryEntry, ...]


def clean_channel_name(raw: str) -> str:
    name = raw.strip().lower()
    name = re.sub(r"^[\s\-*\d.)]+", "", name)
    name = name.replace("#", "")
    name = re.sub(r"\s+", "-", name)
    name = re.sub(r"[^a-z0-9_\-]", "", name)
    name = re.sub(r"-+", "-", name).strip("-")
    return name[:100]


def parse_layout(layout: str) -> list[ParsedChannel]:
    parsed: list[ParsedChannel] = []
    active_category: str | None = None

    for raw_line in layout.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        line = re.sub(r"^[^\w#:\[\]-]+", "", line).strip()
        if not line:
            continue

        cat_match = re.match(r"^\[(.+?)\]$", line)
        if cat_match:
            active_category = cat_match.group(1).strip()[:100]
            continue

        if line.lower().startswith("category:"):
            active_category = line.split(":", 1)[1].strip()[:100]
            continue

        if line.startswith("# "):
            active_category = line[2:].strip()[:100]
            continue

        kind = "text"
        if line.lower().startswith("voice:"):
            kind = "voice"
            line = line.split(":", 1)[1].strip()
        elif line.lower().startswith("text:"):
            line = line.split(":", 1)[1].strip()

        topic = None
        if "|" in line:
            left, right = line.split("|", 1)
            line = left.strip()
            topic = right.strip()[:1024] if right.strip() else None

        name = clean_channel_name(line)
        if name:
            parsed.append(ParsedChannel(kind=kind, name=name, topic=topic, category=active_category))

    return parsed


def serialize_overwrites(overwrites: dict[Any, discord.PermissionOverwrite]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for target, overwrite in overwrites.items():
        allow, deny = overwrite.pair()
        if isinstance(target, discord.Role):
            out.append(
                {
                    "target_type": "role",
                    "target_id": target.id,
                    "target_name": target.name,
                    "allow": allow.value,
                    "deny": deny.value,
                }
            )
        elif isinstance(target, discord.Member):
            out.append(
                {
                    "target_type": "member",
                    "target_id": target.id,
                    "target_name": str(target),
                    "allow": allow.value,
                    "deny": deny.value,
                }
            )
    return out


def deserialize_overwrites(
    entries: list[dict[str, Any]],
    guild: discord.Guild,
    *,
    role_id_index: dict[int, discord.Role] | None = None,
    role_name_index: dict[str, discord.Role] | None = None,
    member_id_index: dict[int, discord.Member] | None = None,
) -> dict[Any, discord.PermissionOverwrite]:
    built: dict[Any, discord.PermissionOverwrite] = {}
    for entry in entries:
        allow = discord.Permissions(entry.get("allow", 0))
        deny = discord.Permissions(entry.get("deny", 0))
        overwrite = discord.PermissionOverwrite.from_pair(allow, deny)

        target = None
        if entry.get("target_type") == "role":
            target_id = entry.get("target_id")
            target_name = entry.get("target_name")
            if role_id_index is not None and isinstance(target_id, int):
                target = role_id_index.get(target_id)
            if target is None and role_name_index is not None and target_name:
                target = role_name_index.get(str(target_name))
            if target is None:
                target = guild.get_role(target_id) or discord.utils.get(guild.roles, name=target_name)
        elif entry.get("target_type") == "member":
            target_id = entry.get("target_id")
            if member_id_index is not None and isinstance(target_id, int):
                target = member_id_index.get(target_id)
            if target is None:
                target = guild.get_member(target_id)

        if target is not None:
            built[target] = overwrite

    return built


def serialize_roles(guild: discord.Guild) -> list[dict[str, Any]]:
    data: list[dict[str, Any]] = []
    for role in sorted(guild.roles, key=lambda r: r.position):
        if role.is_default() or role.managed:
            continue
        data.append(
            {
                "name": role.name,
                "permissions": role.permissions.value,
                "color": role.color.value,
                "hoist": role.hoist,
                "mentionable": role.mentionable,
                "position": role.position,
            }
        )
    return data


def build_channel_reference(channel: discord.abc.GuildChannel | None) -> dict[str, Any] | None:
    if channel is None:
        return None
    category_name = getattr(getattr(channel, "category", None), "name", None)
    channel_type = "voice" if isinstance(channel, discord.VoiceChannel) else "text"
    return {
        "name": channel.name,
        "category": category_name,
        "type": channel_type,
    }


async def encode_asset(asset: discord.Asset | None) -> str | None:
    if asset is None:
        return None
    try:
        return base64.b64encode(await asset.read()).decode("ascii")
    except (discord.HTTPException, discord.Forbidden, discord.NotFound):
        return None


def decode_asset(encoded: str | None) -> bytes | None:
    if encoded is None:
        return None
    try:
        return base64.b64decode(encoded)
    except (ValueError, TypeError):
        return None


def resolve_channel_reference(guild: discord.Guild, reference: dict[str, Any] | None) -> discord.abc.GuildChannel | None:
    if not reference:
        return None

    category_name = reference.get("category")
    channel_name = reference.get("name")
    channel_type = reference.get("type")

    for channel in guild.channels:
        if channel.name != channel_name:
            continue
        if category_name and getattr(getattr(channel, "category", None), "name", None) != category_name:
            continue
        if channel_type == "voice" and not isinstance(channel, discord.VoiceChannel):
            continue
        if channel_type == "text" and not isinstance(channel, discord.TextChannel):
            continue
        return channel
    return None


def serialize_settings(guild: discord.Guild) -> dict[str, Any]:
    return {
        "name": guild.name,
        "description": guild.description,
        "verification_level": int(guild.verification_level.value),
        "default_notifications": int(guild.default_notifications.value),
        "explicit_content_filter": int(guild.explicit_content_filter.value),
        "afk_timeout": guild.afk_timeout,
        "preferred_locale": str(guild.preferred_locale) if getattr(guild, "preferred_locale", None) else None,
        "premium_progress_bar_enabled": getattr(guild, "premium_progress_bar_enabled", False),
        "system_channel_flags": int(guild.system_channel_flags.value) if getattr(guild, "system_channel_flags", None) else 0,
        "afk_channel": build_channel_reference(guild.afk_channel),
        "system_channel": build_channel_reference(guild.system_channel),
        "rules_channel": build_channel_reference(guild.rules_channel),
        "public_updates_channel": build_channel_reference(guild.public_updates_channel),
        "safety_alerts_channel": build_channel_reference(getattr(guild, "safety_alerts_channel", None)),
        "widget_enabled": getattr(guild, "widget_enabled", False),
        "widget_channel": build_channel_reference(getattr(guild, "widget_channel", None)),
    }


async def build_guild_snapshot(guild: discord.Guild, *, include_assets: bool = True) -> dict[str, Any]:
    snapshot = serialize_guild_snapshot(guild)
    if include_assets:
        snapshot["settings"].update(
            {
                "icon_image": await encode_asset(guild.icon),
                "banner_image": await encode_asset(getattr(guild, "banner", None)),
                "splash_image": await encode_asset(getattr(guild, "splash", None)),
                "discovery_splash_image": await encode_asset(getattr(guild, "discovery_splash", None)),
            }
        )
    return snapshot


def serialize_guild_snapshot(guild: discord.Guild) -> dict[str, Any]:
    categories: list[dict[str, Any]] = []
    channels: list[dict[str, Any]] = []

    for category in sorted(guild.categories, key=lambda c: c.position):
        categories.append(
            {
                "name": category.name,
                "position": category.position,
                "overwrites": serialize_overwrites(category.overwrites),
            }
        )

    for channel in sorted(guild.channels, key=lambda c: c.position):
        if isinstance(channel, discord.TextChannel):
            channels.append(
                {
                    "type": "text",
                    "name": channel.name,
                    "position": channel.position,
                    "category": channel.category.name if channel.category else None,
                    "topic": channel.topic,
                    "slowmode_delay": channel.slowmode_delay,
                    "nsfw": channel.nsfw,
                    "overwrites": serialize_overwrites(channel.overwrites),
                }
            )
        elif isinstance(channel, discord.VoiceChannel):
            channels.append(
                {
                    "type": "voice",
                    "name": channel.name,
                    "position": channel.position,
                    "category": channel.category.name if channel.category else None,
                    "bitrate": channel.bitrate,
                    "user_limit": channel.user_limit,
                    "overwrites": serialize_overwrites(channel.overwrites),
                }
            )

    return {
        "version": 1,
        "created_at": utc_now_iso(),
        "roles": serialize_roles(guild),
        "settings": serialize_settings(guild),
        "categories": categories,
        "channels": channels,
    }


def resolve_default_backup_guild_id() -> int | None:
    if not DEFAULT_BACKUP_GUILD_ID:
        return None
    try:
        return int(DEFAULT_BACKUP_GUILD_ID)
    except ValueError:
        return None


def resolve_notice_channel(guild: discord.Guild) -> discord.abc.Messageable | None:
    me = guild.me
    if me is None:
        return None
    if guild.system_channel and guild.system_channel.permissions_for(me).send_messages:
        return guild.system_channel
    for channel in guild.text_channels:
        permissions = channel.permissions_for(me)
        if permissions.send_messages and permissions.view_channel:
            return channel
    return None


async def apply_snapshot_to_guild(
    snapshot: dict[str, Any],
    target: discord.Guild,
    *,
    delete_roles: bool,
    delete_channels: bool,
    load_roles: bool,
    load_channels: bool,
    load_settings: bool,
    create_only_missing: bool = False,
    preserve_channel_id: int | None = None,
    progress_callback: Any = None,
) -> dict[str, int]:
    result = {
        "deleted_roles": 0,
        "deleted_channels": 0,
        "created_roles": 0,
        "updated_roles": 0,
        "created_categories": 0,
        "created_channels": 0,
        "updated_channels": 0,
        "updated_settings": 0,
        "blocked_roles": 0,
    }
    precreated_categories: dict[str, discord.CategoryChannel] = {}
    precreated_channels: dict[tuple[str, str, str], discord.abc.GuildChannel] = {}

    async def report(phase: str, detail: str) -> None:
        if progress_callback is not None:
            try:
                await progress_callback(phase, detail, dict(result))
            except Exception:
                pass

    preserved_category_id: int | None = None
    if preserve_channel_id:
        preserved_channel = target.get_channel(preserve_channel_id)
        if isinstance(preserved_channel, discord.TextChannel) and preserved_channel.category is not None:
            preserved_category_id = preserved_channel.category.id
            try:
                await preserved_channel.edit(category=None, reason="CLINX backup load: preserve status channel")
                preserved_category_id = None
            except (discord.Forbidden, discord.HTTPException):
                pass

    if delete_channels and not create_only_missing:
        await report("Wiping Channels", "Removing the live channel tree before rebuild.")
        for channel in list(target.channels):
            if preserve_channel_id and channel.id == preserve_channel_id:
                continue
            if preserved_category_id and channel.id == preserved_category_id:
                continue
            try:
                await channel.delete(reason="CLINX backup load: delete channels")
                result["deleted_channels"] += 1
            except (discord.Forbidden, discord.HTTPException):
                pass

    if load_channels and not create_only_missing:
        structure_category_map: dict[str, discord.CategoryChannel] = (
            {} if delete_channels else {category.name: category for category in target.categories}
        )

        await report("Scaffolding Categories", "Creating the category frame before channels start streaming in.")
        for cat_data in snapshot.get("categories", []):
            if cat_data["name"] in structure_category_map:
                continue
            try:
                created_category = await target.create_category(name=cat_data["name"])
                precreated_categories[cat_data["name"]] = created_category
                structure_category_map[cat_data["name"]] = created_category
                result["created_categories"] += 1
            except (discord.Forbidden, discord.HTTPException, AttributeError):
                continue

        await report("Scaffolding Channels", "Creating text and voice channels in the rebuilt category tree.")
        for ch_data in snapshot.get("channels", []):
            channel_key = snapshot_channel_signature(ch_data)
            existing_structure_channel = next(
                (channel for channel in target.channels if live_channel_signature(channel) == channel_key),
                None,
            )
            if existing_structure_channel is not None:
                continue

            category = structure_category_map.get(ch_data.get("category")) if ch_data.get("category") else None
            try:
                if ch_data["type"] == "text":
                    created_channel = await target.create_text_channel(
                        name=ch_data["name"],
                        category=category,
                        topic=ch_data.get("topic"),
                        slowmode_delay=ch_data.get("slowmode_delay", 0),
                        nsfw=ch_data.get("nsfw", False),
                    )
                elif ch_data["type"] == "voice":
                    created_channel = await target.create_voice_channel(
                        name=ch_data["name"],
                        category=category,
                        bitrate=ch_data.get("bitrate"),
                        user_limit=ch_data.get("user_limit", 0),
                    )
                else:
                    continue
                precreated_channels[channel_key] = created_channel
                result["created_channels"] += 1
            except (discord.Forbidden, discord.HTTPException, AttributeError):
                continue

    if delete_roles and not create_only_missing:
        await report("Wiping Roles", "Clearing live roles so the backup stack can be rebuilt cleanly.")
        roles_to_delete = [
            role for role in sorted(target.roles, key=lambda r: r.position, reverse=True)
            if not role.managed and not role.is_default()
        ]

        async def delete_role(role: discord.Role) -> tuple[str, discord.Role]:
            try:
                await role.delete(reason="CLINX backup load: delete roles")
                return ("deleted", role)
            except (discord.Forbidden, discord.HTTPException):
                return ("blocked", role)

        deleted_total = 0
        for batch in chunk_items(roles_to_delete, 10):
            role_delete_results = await run_limited([delete_role(role) for role in batch], limit=10)
            for status_name, _ in role_delete_results:
                if status_name == "deleted":
                    result["deleted_roles"] += 1
                    deleted_total += 1
                else:
                    result["blocked_roles"] += 1
            await report(
                "Wiping Roles",
                f"Clearing live roles so the backup stack can be rebuilt cleanly. `{deleted_total}` of `{len(roles_to_delete)}` roles processed.",
            )

    if load_roles and not create_only_missing:
        await report("Rebuilding Roles", "Creating and updating the role stack from the backup snapshot.")
        existing_roles: dict[str, list[discord.Role]] = {}
        if not delete_roles:
            for live_role in sorted(target.roles, key=lambda item: item.position):
                if live_role.is_default() or live_role.managed:
                    continue
                existing_roles.setdefault(live_role.name, []).append(live_role)
        assigned_roles: list[tuple[dict[str, Any], discord.Role | None]] = []
        for role_data in sorted(snapshot.get("roles", []), key=lambda r: r.get("position", 0)):
            assigned_role: discord.Role | None = None
            if not delete_roles:
                bucket = existing_roles.get(role_data["name"], [])
                if bucket:
                    assigned_role = bucket.pop(0)
            assigned_roles.append((role_data, assigned_role))

        async def sync_role(role_data: dict[str, Any], role: discord.Role | None) -> tuple[str, discord.Role | None, int]:
            permissions = discord.Permissions(role_data.get("permissions", 0))
            color = discord.Colour(role_data.get("color", 0))
            desired_position = int(role_data.get("position", role.position if role is not None else 1))
            if role is None:
                try:
                    created_role = await target.create_role(
                        name=role_data["name"],
                        permissions=permissions,
                        colour=color,
                        hoist=role_data.get("hoist", False),
                        mentionable=role_data.get("mentionable", False),
                        reason="CLINX backup load: create role",
                    )
                    return ("created", created_role, desired_position)
                except (discord.Forbidden, discord.HTTPException, AttributeError):
                    return ("blocked", None, desired_position)
            try:
                await role.edit(
                    permissions=permissions,
                    colour=color,
                    hoist=role_data.get("hoist", False),
                    mentionable=role_data.get("mentionable", False),
                    reason="CLINX backup load: update role",
                )
                return ("updated", role, desired_position)
            except (discord.Forbidden, discord.HTTPException, AttributeError):
                return ("blocked", role, desired_position)

        role_order: list[tuple[discord.Role, int]] = []
        processed_roles = 0
        for batch in chunk_items(assigned_roles, 10):
            role_results = await run_limited([sync_role(role_data, role) for role_data, role in batch], limit=10)
            for status_name, role, desired_position in role_results:
                processed_roles += 1
                if status_name == "created" and role is not None:
                    result["created_roles"] += 1
                    role_order.append((role, desired_position))
                elif status_name == "updated" and role is not None:
                    result["updated_roles"] += 1
                    role_order.append((role, desired_position))
                else:
                    result["blocked_roles"] += 1
            await report(
                "Rebuilding Roles",
                f"Creating and updating the role stack from the backup snapshot. `{processed_roles}` of `{len(assigned_roles)}` roles processed.",
            )

        bot_member = target.me
        if role_order and bot_member is not None:
            max_position = max(1, bot_member.top_role.position - 1)
            manageable_roles = [item for item in role_order if item[0] < bot_member.top_role]
            manageable_roles.sort(key=lambda item: item[1], reverse=True)
            position_map: dict[discord.Role, int] = {}
            for offset, (role, _desired_position) in enumerate(manageable_roles):
                position_map[role] = max(1, max_position - offset)
            if position_map:
                try:
                    await target.edit_role_positions(position_map)
                except (discord.Forbidden, discord.HTTPException, AttributeError):
                    result["blocked_roles"] += len(position_map)

    if load_channels:
        await report("Finalizing Channels", "Binding channel permissions, topics, and structure against the rebuilt role stack.")
        category_map: dict[str, discord.CategoryChannel] = dict(precreated_categories)
        existing_categories = {} if delete_channels else {category.name: category for category in target.categories}
        role_id_index = {role.id: role for role in target.roles}
        role_name_index = {role.name: role for role in target.roles}
        member_id_index = {member.id: member for member in target.members}

        for cat_data in snapshot.get("categories", []):
            existing = category_map.get(cat_data["name"]) or existing_categories.get(cat_data["name"])
            overwrites = deserialize_overwrites(
                cat_data.get("overwrites", []),
                target,
                role_id_index=role_id_index,
                role_name_index=role_name_index,
                member_id_index=member_id_index,
            )

            if existing is None:
                try:
                    existing = await target.create_category(name=cat_data["name"], overwrites=overwrites)
                    result["created_categories"] += 1
                except (discord.Forbidden, discord.HTTPException, AttributeError):
                    continue
            elif not create_only_missing:
                try:
                    await existing.edit(overwrites=overwrites, position=cat_data.get("position", existing.position))
                except (discord.Forbidden, discord.HTTPException, AttributeError):
                    pass

            category_map[cat_data["name"]] = existing

        category_edit_tasks: list[Any] = []
        for cat_data in snapshot.get("categories", []):
            existing = category_map.get(cat_data["name"])
            if existing is None or create_only_missing:
                continue
            overwrites = deserialize_overwrites(
                cat_data.get("overwrites", []),
                target,
                role_id_index=role_id_index,
                role_name_index=role_name_index,
                member_id_index=member_id_index,
            )

            async def edit_category(category: discord.CategoryChannel, payload: dict[str, Any], category_overwrites: dict[Any, discord.PermissionOverwrite]) -> bool:
                try:
                    await category.edit(overwrites=category_overwrites, position=payload.get("position", category.position))
                    return True
                except (discord.Forbidden, discord.HTTPException, AttributeError):
                    return False

            category_edit_tasks.append(edit_category(existing, cat_data, overwrites))

        if category_edit_tasks:
            await run_limited(category_edit_tasks, limit=12)

        existing_channels = {} if delete_channels else {live_channel_signature(channel): channel for channel in target.channels}
        channel_edit_tasks: list[Any] = []
        for ch_data in snapshot.get("channels", []):
            channel_key = snapshot_channel_signature(ch_data)
            existing = precreated_channels.get(channel_key) or existing_channels.get(channel_key)
            category = category_map.get(ch_data.get("category")) if ch_data.get("category") else None
            overwrites = deserialize_overwrites(
                ch_data.get("overwrites", []),
                target,
                role_id_index=role_id_index,
                role_name_index=role_name_index,
                member_id_index=member_id_index,
            )

            if existing is None:
                try:
                    if ch_data["type"] == "text":
                        await target.create_text_channel(
                            name=ch_data["name"],
                            category=category,
                            topic=ch_data.get("topic"),
                            slowmode_delay=ch_data.get("slowmode_delay", 0),
                            nsfw=ch_data.get("nsfw", False),
                            overwrites=overwrites,
                        )
                    elif ch_data["type"] == "voice":
                        await target.create_voice_channel(
                            name=ch_data["name"],
                            category=category,
                            bitrate=ch_data.get("bitrate"),
                            user_limit=ch_data.get("user_limit", 0),
                            overwrites=overwrites,
                        )
                    result["created_channels"] += 1
                except (discord.Forbidden, discord.HTTPException, AttributeError):
                    pass
                continue

            if create_only_missing:
                continue

            async def edit_existing_channel(
                live_channel: discord.abc.GuildChannel,
                payload: dict[str, Any],
                target_category: discord.CategoryChannel | None,
                channel_overwrites: dict[Any, discord.PermissionOverwrite],
                precreated: bool,
            ) -> bool:
                try:
                    if isinstance(live_channel, discord.TextChannel) and payload["type"] == "text":
                        await live_channel.edit(
                            category=target_category,
                            topic=payload.get("topic"),
                            slowmode_delay=payload.get("slowmode_delay", 0),
                            nsfw=payload.get("nsfw", False),
                            overwrites=channel_overwrites,
                        )
                        return not precreated
                    if isinstance(live_channel, discord.VoiceChannel) and payload["type"] == "voice":
                        await live_channel.edit(
                            category=target_category,
                            bitrate=payload.get("bitrate", live_channel.bitrate),
                            user_limit=payload.get("user_limit", 0),
                            overwrites=channel_overwrites,
                        )
                        return not precreated
                except (discord.Forbidden, discord.HTTPException, AttributeError):
                    return False
                return False

            channel_edit_tasks.append(
                edit_existing_channel(existing, ch_data, category, overwrites, channel_key in precreated_channels)
            )

        if channel_edit_tasks:
            channel_edit_results = await run_limited(channel_edit_tasks, limit=16)
            result["updated_channels"] += sum(1 for updated in channel_edit_results if updated)

    if load_settings and not create_only_missing:
        await report("Syncing Server Settings", "Applying the saved profile, visuals, and moderation defaults.")
        settings = snapshot.get("settings", {})
        profile_kwargs: dict[str, Any] = {}
        if "name" in settings and settings.get("name"):
            profile_kwargs["name"] = settings["name"]
        if "description" in settings:
            profile_kwargs["description"] = settings.get("description")
        if "verification_level" in settings:
            profile_kwargs["verification_level"] = discord.VerificationLevel(settings.get("verification_level", target.verification_level.value))
        if "default_notifications" in settings:
            profile_kwargs["default_notifications"] = discord.NotificationLevel(settings.get("default_notifications", target.default_notifications.value))
        if "explicit_content_filter" in settings:
            profile_kwargs["explicit_content_filter"] = discord.ContentFilter(settings.get("explicit_content_filter", target.explicit_content_filter.value))
        if "afk_timeout" in settings:
            profile_kwargs["afk_timeout"] = settings.get("afk_timeout", target.afk_timeout)
        if settings.get("preferred_locale"):
            try:
                profile_kwargs["preferred_locale"] = discord.Locale(settings["preferred_locale"])
            except ValueError:
                pass
        if "premium_progress_bar_enabled" in settings:
            profile_kwargs["premium_progress_bar_enabled"] = bool(settings.get("premium_progress_bar_enabled"))
        if "widget_enabled" in settings:
            profile_kwargs["widget_enabled"] = bool(settings.get("widget_enabled"))
        if "system_channel_flags" in settings:
            profile_kwargs["system_channel_flags"] = discord.SystemChannelFlags._from_value(int(settings.get("system_channel_flags", 0)))

        channel_setting_map = {
            "afk_channel": settings.get("afk_channel"),
            "system_channel": settings.get("system_channel"),
            "rules_channel": settings.get("rules_channel"),
            "public_updates_channel": settings.get("public_updates_channel"),
            "safety_alerts_channel": settings.get("safety_alerts_channel"),
            "widget_channel": settings.get("widget_channel"),
        }
        for key, reference in channel_setting_map.items():
            if key in settings:
                profile_kwargs[key] = resolve_channel_reference(target, reference)

        async def apply_setting_payload(payload: dict[str, Any], *, reason_suffix: str) -> bool:
            if not payload:
                return False
            try:
                await target.edit(reason=f"CLINX backup load: {reason_suffix}", **payload)
                result["updated_settings"] += 1
                return True
            except (discord.Forbidden, discord.HTTPException, AttributeError):
                return False

        await apply_setting_payload(
            {key: profile_kwargs[key] for key in ("name",) if key in profile_kwargs},
            reason_suffix="update server name",
        )
        await apply_setting_payload(
            {key: profile_kwargs[key] for key in ("description",) if key in profile_kwargs},
            reason_suffix="update server description",
        )
        await apply_setting_payload(
            {
                key: value
                for key, value in profile_kwargs.items()
                if key
                in {
                    "verification_level",
                    "default_notifications",
                    "explicit_content_filter",
                    "afk_timeout",
                    "preferred_locale",
                    "premium_progress_bar_enabled",
                    "widget_enabled",
                }
            },
            reason_suffix="update moderation defaults",
        )
        await apply_setting_payload(
            {key: profile_kwargs[key] for key in ("system_channel_flags",) if key in profile_kwargs},
            reason_suffix="update system channel flags",
        )
        for key in (
            "afk_channel",
            "system_channel",
            "rules_channel",
            "public_updates_channel",
            "safety_alerts_channel",
            "widget_channel",
        ):
            if key not in profile_kwargs:
                continue
            await apply_setting_payload({key: profile_kwargs[key]}, reason_suffix=f"update {key}")

        asset_keys = (
            ("icon", "icon_image"),
            ("banner", "banner_image"),
            ("splash", "splash_image"),
            ("discovery_splash", "discovery_splash_image"),
        )
        for edit_key, data_key in asset_keys:
            if data_key not in settings:
                continue
            try:
                await target.edit(reason=f"CLINX backup load: update {edit_key}", **{edit_key: decode_asset(settings.get(data_key))})
                result["updated_settings"] += 1
            except (discord.Forbidden, discord.HTTPException, AttributeError):
                continue

    return result


async def run_backup_load_job(
    guild_id: int,
    snapshot: dict[str, Any],
    target: discord.Guild,
    *,
    backup_id: str,
    source_name: str,
    selected_actions: set[str],
) -> None:
    async def report_phase(phase: str, detail: str, stats: dict[str, int]) -> None:
        job = BACKUP_LOAD_JOBS.get(guild_id)
        if not job:
            return
        job["last_progress_phase"] = phase
        job["phase"] = phase
        job["phase_detail"] = detail
        job["stats"] = stats
        await sync_backup_status_message(guild_id)

    try:
        stats = await apply_snapshot_to_guild(
            snapshot,
            target,
            delete_roles="delete_roles" in selected_actions,
            delete_channels="delete_channels" in selected_actions,
            load_roles="load_roles" in selected_actions,
            load_channels="load_channels" in selected_actions,
            load_settings="load_settings" in selected_actions,
            create_only_missing=False,
            preserve_channel_id=BACKUP_LOAD_JOBS.get(guild_id, {}).get("preserve_channel_id"),
            progress_callback=report_phase,
        )
        BACKUP_LOAD_JOBS[guild_id]["status"] = "completed"
        BACKUP_LOAD_JOBS[guild_id]["finished_at"] = utc_now_iso()
        BACKUP_LOAD_JOBS[guild_id]["stats"] = stats
        BACKUP_LOAD_JOBS[guild_id]["phase"] = "Completed"
        BACKUP_LOAD_JOBS[guild_id]["phase_detail"] = "The reviewed restore plan finished applying."
        await sync_backup_status_message(guild_id)
        approval_request_id = BACKUP_LOAD_JOBS[guild_id].get("approval_request_id")
        request = PENDING_SAFETY_REQUESTS.get(guild_id)
        if request and request.get("id") == approval_request_id:
            request["status"] = "completed"
            request["status_text"] = "Owner-approved restore finished."
            request["result_text"] = build_backup_load_status_description(BACKUP_LOAD_JOBS[guild_id])
            await sync_safety_request_message(guild_id)
    except asyncio.CancelledError:
        BACKUP_LOAD_JOBS[guild_id]["status"] = "cancelled"
        BACKUP_LOAD_JOBS[guild_id]["finished_at"] = utc_now_iso()
        BACKUP_LOAD_JOBS[guild_id]["phase"] = "Cancelled"
        BACKUP_LOAD_JOBS[guild_id]["phase_detail"] = "The restore task was cancelled before completion."
        await sync_backup_status_message(guild_id)
        approval_request_id = BACKUP_LOAD_JOBS[guild_id].get("approval_request_id")
        request = PENDING_SAFETY_REQUESTS.get(guild_id)
        if request and request.get("id") == approval_request_id:
            request["status"] = "failed"
            request["status_text"] = "Owner-approved restore was cancelled."
            request["result_text"] = "The restore task stopped before completion."
            await sync_safety_request_message(guild_id)
        raise
    except Exception as exc:
        BACKUP_LOAD_JOBS[guild_id]["status"] = "failed"
        BACKUP_LOAD_JOBS[guild_id]["finished_at"] = utc_now_iso()
        BACKUP_LOAD_JOBS[guild_id]["error"] = str(exc)
        BACKUP_LOAD_JOBS[guild_id]["phase"] = "Failed"
        BACKUP_LOAD_JOBS[guild_id]["phase_detail"] = "CLINX hit an exception while applying the restore plan."
        await sync_backup_status_message(guild_id)
        approval_request_id = BACKUP_LOAD_JOBS[guild_id].get("approval_request_id")
        request = PENDING_SAFETY_REQUESTS.get(guild_id)
        if request and request.get("id") == approval_request_id:
            request["status"] = "failed"
            request["status_text"] = "Owner-approved restore failed."
            request["result_text"] = str(exc)
            await sync_safety_request_message(guild_id)


class BackupLoadStatusButton(discord.ui.Button["BackupLoadStatusView"]):
    def __init__(self, guild_id: int) -> None:
        super().__init__(label="View Status", style=discord.ButtonStyle.primary)
        self.guild_id = guild_id

    async def callback(self, interaction: discord.Interaction) -> None:
        job = BACKUP_LOAD_JOBS.get(self.guild_id)
        if not job:
            await interaction.response.send_message(
                embed=make_embed("Backup Status", "No backup load job found for this server.", EMBED_INFO, interaction),
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            embed=make_embed("Backup Status", build_backup_load_status_description(job), EMBED_INFO, interaction),
            ephemeral=True,
        )


class BackupLoadStatusView(discord.ui.View):
    def __init__(self, guild_id: int) -> None:
        super().__init__(timeout=1800)
        self.add_item(BackupLoadStatusButton(guild_id))


LOAD_ACTION_ORDER = (
    "load_roles",
    "load_channels",
    "load_settings",
    "delete_roles",
    "delete_channels",
)

LOAD_ACTION_LABELS = {
    "load_roles": "Load Roles",
    "load_channels": "Load Channels",
    "load_settings": "Load Settings",
    "delete_roles": "Delete Roles",
    "delete_channels": "Delete Channels",
}


def summarize_selected_actions(selected_actions: set[str]) -> str:
    ordered = [LOAD_ACTION_LABELS[action] for action in LOAD_ACTION_ORDER if action in selected_actions]
    return ", ".join(ordered) if ordered else "No active lanes"


def build_snapshot_detail_lines(snapshot: dict[str, Any], target: discord.Guild) -> str:
    settings = snapshot.get("settings", {})
    settings_flags = []
    if settings.get("name"):
        settings_flags.append("name")
    if settings.get("icon_image") is not None:
        settings_flags.append("icon")
    if settings.get("banner_image") is not None:
        settings_flags.append("banner")
    if settings.get("splash_image") is not None:
        settings_flags.append("invite splash")
    if settings.get("discovery_splash_image") is not None:
        settings_flags.append("discovery splash")
    if settings.get("description") is not None:
        settings_flags.append("description")
    if settings.get("preferred_locale"):
        settings_flags.append("locale")

    settings_text = ", ".join(settings_flags) if settings_flags else "core moderation defaults"
    return (
        f"**Snapshot Inventory**\n"
        f"- `{len(snapshot.get('roles', []))}` roles captured\n"
        f"- `{len(snapshot.get('categories', []))}` categories captured\n"
        f"- `{len(snapshot.get('channels', []))}` channels captured\n"
        f"- Settings payload includes {settings_text}\n\n"
        f"**Target Inventory**\n"
        f"- `{len([role for role in target.roles if not role.managed and not role.is_default()])}` live roles\n"
        f"- `{len(target.categories)}` live categories\n"
        f"- `{len(target.channels)}` live channels"
    )


def build_backup_plan_preview(
    snapshot: dict[str, Any],
    target: discord.Guild,
    selected_actions: set[str],
) -> dict[str, int]:
    target_roles = [role for role in target.roles if not role.managed and not role.is_default()]
    snapshot_roles = snapshot.get("roles", [])
    snapshot_categories = snapshot.get("categories", [])
    snapshot_channels = snapshot.get("channels", [])
    target_categories = list(target.categories)
    target_channels = list(target.channels)
    target_role_names = {role.name for role in target_roles}
    target_category_names = {category.name for category in target_categories}
    target_channel_signatures = {live_channel_signature(channel) for channel in target_channels}

    preview = {
        "deleted_roles": len(target_roles) if "delete_roles" in selected_actions else 0,
        "deleted_channels": len(target_channels) if "delete_channels" in selected_actions else 0,
        "created_roles": 0,
        "updated_roles": 0,
        "created_categories": 0,
        "created_channels": 0,
        "updated_channels": 0,
        "updated_settings": 1 if "load_settings" in selected_actions else 0,
    }

    if "load_roles" in selected_actions:
        if "delete_roles" in selected_actions:
            preview["created_roles"] = len(snapshot_roles)
        else:
            preview["created_roles"] = sum(1 for role in snapshot_roles if role.get("name") not in target_role_names)
            preview["updated_roles"] = sum(1 for role in snapshot_roles if role.get("name") in target_role_names)

    if "load_channels" in selected_actions:
        if "delete_channels" in selected_actions:
            preview["created_categories"] = len(snapshot_categories)
            preview["created_channels"] = len(snapshot_channels)
        else:
            preview["created_categories"] = sum(1 for category in snapshot_categories if category.get("name") not in target_category_names)
            preview["created_channels"] = sum(
                1 for channel in snapshot_channels if snapshot_channel_signature(channel) not in target_channel_signatures
            )
            preview["updated_channels"] = sum(
                1 for channel in snapshot_channels if snapshot_channel_signature(channel) in target_channel_signatures
            )

    return preview


def build_backup_lane_lines(selected_actions: set[str]) -> list[str]:
    return [
        f"- **Load Roles**: {'ON' if 'load_roles' in selected_actions else 'OFF'} - restore role stack from backup",
        f"- **Load Channels**: {'ON' if 'load_channels' in selected_actions else 'OFF'} - restore categories and channels",
        f"- **Load Settings**: {'ON' if 'load_settings' in selected_actions else 'OFF'} - sync server profile and config",
        f"- **Delete Roles**: {'ON' if 'delete_roles' in selected_actions else 'OFF'} - wipe live roles before rebuild",
        f"- **Delete Channels**: {'ON' if 'delete_channels' in selected_actions else 'OFF'} - wipe live channels before rebuild",
    ]


def build_backup_hierarchy_warnings(
    snapshot: dict[str, Any],
    target: discord.Guild,
    selected_actions: set[str],
) -> list[str]:
    warnings: list[str] = []
    bot_member = target.me
    if bot_member is None:
        warnings.append("CLINX could not resolve its member object in the target server.")
        return warnings

    if "load_roles" in selected_actions:
        if not bot_member.guild_permissions.manage_roles:
            warnings.append("CLINX is missing **Manage Roles** in the target server.")
        higher_live_roles = [
            role
            for role in target.roles
            if not role.is_default()
            and not role.managed
            and role.id != bot_member.top_role.id
            and role.position >= bot_member.top_role.position
        ]
        if higher_live_roles:
            warnings.append(
                f"`{len(higher_live_roles)}` live roles still sit at or above the CLINX role in this server. "
                "Move CLINX higher or those roles cannot be managed correctly."
            )

    if "load_channels" in selected_actions and not bot_member.guild_permissions.manage_channels:
        warnings.append("CLINX is missing **Manage Channels** in the target server.")

    if "load_settings" in selected_actions and not bot_member.guild_permissions.manage_guild:
        warnings.append("CLINX is missing **Manage Server** for full settings sync.")

    return warnings


def build_source_hierarchy_warnings(guild: discord.Guild) -> list[str]:
    warnings: list[str] = []
    bot_member = guild.me
    if bot_member is None:
        warnings.append("CLINX could not resolve its member object in the source server.")
        return warnings

    if not bot_member.guild_permissions.manage_roles:
        warnings.append("CLINX is missing **Manage Roles** in the source server.")

    higher_roles = [
        role
        for role in guild.roles
        if not role.is_default()
        and not role.managed
        and role.id != bot_member.top_role.id
        and role.position >= bot_member.top_role.position
    ]
    if higher_roles:
        warnings.append(
            f"`{len(higher_roles)}` source roles sit at or above the CLINX role. "
            "Move CLINX higher before relying on role backups from this server."
        )
    return warnings


class BackupCreatedCardView(discord.ui.LayoutView):
    def __init__(
        self,
        bot_user: discord.ClientUser | None,
        backup_id: str,
        source: discord.Guild,
        snapshot: dict[str, Any],
    ) -> None:
        super().__init__(timeout=None)
        self.bot_user = bot_user
        self.backup_id = backup_id
        self.source = source
        self.snapshot = snapshot
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()
        hero = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        snapshot_roles = len(self.snapshot.get("roles", []))
        snapshot_categories = len(self.snapshot.get("categories", []))
        snapshot_channels = len(self.snapshot.get("channels", []))
        source_name = self.source.name.strip() if getattr(self.source, "name", "") else "Unknown Server"
        source_label = f"{source_name} • {self.source.id}"
        header_strip = "🗂 Snapshot locked • 🔐 Private restore lane • ⚡ Ready to load"
        children: list[discord.ui.Item[Any]] = [
            discord.ui.Section(
                discord.ui.TextDisplay("## <> Backup Vault Sealed"),
                discord.ui.TextDisplay("Snapshot locked. This recovery ID is ready for private restore use."),
                discord.ui.TextDisplay(header_strip),
                accessory=hero,
            ),
            discord.ui.Separator(),
            discord.ui.Section(
                discord.ui.TextDisplay("### Load ID"),
                discord.ui.TextDisplay(f"**{self.backup_id}**"),
                accessory=discord.ui.Button(label="Ready", style=discord.ButtonStyle.success, disabled=True),
            ),
            discord.ui.Separator(),
            discord.ui.Section(
                discord.ui.TextDisplay("### Source"),
                discord.ui.TextDisplay(f"**{source_label}**"),
                accessory=discord.ui.Button(label="Private", style=discord.ButtonStyle.secondary, disabled=True),
            ),
            discord.ui.Separator(),
            discord.ui.Section(
                discord.ui.TextDisplay("### Snapshot Payload"),
                discord.ui.TextDisplay(
                    f"**{snapshot_roles}** roles\n"
                    f"**{snapshot_categories}** categories\n"
                    f"**{snapshot_channels}** channels"
                ),
                accessory=discord.ui.Button(label="Vault", style=discord.ButtonStyle.primary, disabled=True),
            ),
            discord.ui.Separator(),
            discord.ui.TextDisplay(
                "### Restore Profile\n"
                "- Roles, channels, categories, overwrites, and server settings were captured.\n"
                "- Use **`/backup load`** to open the planner and apply this snapshot."
            ),
        ]
        self.add_item(discord.ui.Container(*children, accent_color=EMBED_INFO))


class RoleSafetyWarningCardView(discord.ui.LayoutView):
    def __init__(
        self,
        bot_user: discord.ClientUser | None,
        title: str,
        subtitle: str,
        warnings: list[str],
    ) -> None:
        super().__init__(timeout=None)
        self.bot_user = bot_user
        self.title = title
        self.subtitle = subtitle
        self.warnings = warnings
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()
        hero = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        warning_text = "\n".join(f"- {line}" for line in self.warnings)
        self.add_item(
            discord.ui.Container(
                discord.ui.Section(
                    discord.ui.TextDisplay(f"## ⚠ {self.title}"),
                    discord.ui.TextDisplay(self.subtitle),
                    accessory=hero,
                ),
                discord.ui.Separator(),
                discord.ui.TextDisplay(f"### What Needs Attention\n{warning_text}"),
                discord.ui.TextDisplay(
                    "### Fix\n"
                    "- Move the CLINX role above the roles it must manage.\n"
                    "- Keep **Manage Roles** enabled if you expect full role backup and load.\n"
                    "- Discord does not allow CLINX to move its own role automatically."
                ),
                accent_color=EMBED_WARN,
            )
        )


def get_backup_storage_label() -> str:
    return "Cloudflare R2" if is_r2_backup_storage_enabled() else "Local JSON"


def build_backup_interval_health_text(config: dict[str, Any] | None) -> str:
    if not isinstance(config, dict):
        return "- Auto backup lane is offline.\n- Turn it on to let CLINX seal backups on a fixed interval."
    if not config.get("enabled"):
        return "- Auto backup lane is disabled.\n- Existing settings are preserved until you turn it on again."
    lines = [
        f"- Interval: `{format_interval_label(int(config.get('interval_hours') or 0))}`",
        f"- Keep latest: `{int(config.get('keep_count') or 1)}` backup(s)",
        f"- Next seal: `{format_relative_timestamp(config.get('next_run_at'))}`",
    ]
    if config.get("last_success_at"):
        lines.append(f"- Last success: `{format_relative_timestamp(config.get('last_success_at'))}`")
    if config.get("last_error"):
        lines.append(f"- Last issue: `{str(config['last_error'])[:120]}`")
    return "\n".join(lines)


class BackupIntervalCardView(discord.ui.LayoutView):
    def __init__(
        self,
        bot_user: discord.ClientUser | None,
        *,
        title: str,
        subtitle: str,
        guild: discord.Guild,
        config: dict[str, Any] | None,
        backup_count: int,
        backup_limit: int,
        plan_label: str,
        color: int = EMBED_INFO,
    ) -> None:
        super().__init__(timeout=None)
        self.bot_user = bot_user
        self.title = title
        self.subtitle = subtitle
        self.guild = guild
        self.config = config
        self.backup_count = backup_count
        self.backup_limit = backup_limit
        self.plan_label = plan_label
        self.color = color
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()
        hero = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        enabled = bool(self.config and self.config.get("enabled"))
        state_badge = discord.ui.Button(
            label="Enabled" if enabled else "Disabled",
            style=discord.ButtonStyle.success if enabled else discord.ButtonStyle.secondary,
            disabled=True,
        )
        interval_badge = discord.ui.Button(
            label=format_interval_label(int(self.config.get("interval_hours") or 0)) if self.config else "Off",
            style=discord.ButtonStyle.primary if enabled else discord.ButtonStyle.secondary,
            disabled=True,
        )
        owner_name = "Not assigned"
        if isinstance(self.config, dict) and self.config.get("owner_display_name"):
            owner_name = str(self.config["owner_display_name"])
        runtime_text = (
            f"Storage: `{get_backup_storage_label()}`\n"
            f"Vault tier: `{self.plan_label}`\n"
            f"Slots used: `{self.backup_count}/{self.backup_limit}`\n"
            f"Vault owner: `{owner_name}`"
        )
        schedule_text = build_backup_interval_health_text(self.config)
        last_backup_id = (self.config or {}).get("last_backup_id") if isinstance(self.config, dict) else None
        last_backup_text = (
            f"`{last_backup_id}`\n"
            f"`{format_backup_timestamp((self.config or {}).get('last_success_at'))}`"
            if last_backup_id
            else "No automatic backup sealed yet."
        )

        self.add_item(
            discord.ui.Container(
                discord.ui.Section(
                    discord.ui.TextDisplay(f"## <> {self.title}"),
                    discord.ui.TextDisplay(self.subtitle),
                    accessory=hero,
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Schedule"),
                    discord.ui.TextDisplay(schedule_text),
                    accessory=interval_badge,
                ),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Runtime"),
                    discord.ui.TextDisplay(runtime_text),
                    accessory=state_badge,
                ),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Last Auto Backup"),
                    discord.ui.TextDisplay(last_backup_text),
                    accessory=discord.ui.Button(label="Vault", style=discord.ButtonStyle.primary, disabled=True),
                ),
                discord.ui.TextDisplay(
                    "### Commands\n"
                    "- Use **`/backup interval on`** to arm automatic backups.\n"
                    "- Use **`/backup interval show`** to inspect the next seal window.\n"
                    "- Use **`/backup list`** to browse the backups stored in your vault."
                ),
                accent_color=self.color,
            )
        )


BACKUP_VAULT_PAGE_SIZE = 10
BACKUP_STRUCTURE_PREVIEW_LIMIT = 900
BACKUP_ROLE_PREVIEW_LIMIT = 900
DEVELOPER_DASHBOARD_PAGE_SIZE = 8


def format_backup_structure_preview(summary: dict[str, Any] | None) -> str:
    if not isinstance(summary, dict):
        return "```text\nNo structure preview stored.\n```"
    lines = list(summary.get("structure_preview", []) or ["No structure preview stored."])
    overflow = int(summary.get("structure_overflow", 0) or 0)
    if overflow > 0:
        lines.append(f"... +{overflow} more")
    return "```text\n" + "\n".join(lines)[:BACKUP_STRUCTURE_PREVIEW_LIMIT] + "\n```"


def format_backup_role_preview(summary: dict[str, Any] | None) -> str:
    if not isinstance(summary, dict):
        return "```text\nNo role preview stored.\n```"
    lines = list(summary.get("role_preview", []) or ["No role preview stored."])
    overflow = int(summary.get("role_overflow", 0) or 0)
    if overflow > 0:
        lines.append(f"... +{overflow} more")
    return "```text\n" + "\n".join(lines)[:BACKUP_ROLE_PREVIEW_LIMIT] + "\n```"


class BackupVaultSelect(discord.ui.Select["BackupListCardView"]):
    def __init__(self, page_entries: list[dict[str, Any]], selected_backup_id: str | None) -> None:
        options = [
            discord.SelectOption(
                label=entry.get("id", "unknown"),
                description=f"{entry.get('source_guild_name', 'Unknown Source')} ({format_backup_timestamp(entry.get('created_at'))})"[:100],
                value=entry.get("id", ""),
                default=entry.get("id") == selected_backup_id,
            )
            for entry in page_entries
        ]
        super().__init__(
            placeholder="Select a backup",
            options=options,
            disabled=not options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        selected_backup_id = self.values[0]
        new_view = BackupListCardView(
            self.view.bot_user,
            author_id=self.view.author_id,
            entries=self.view.entries,
            guild_id=self.view.guild_id,
            backup_limit=self.view.backup_limit,
            plan_label=self.view.plan_label,
            page=self.view.page,
            selected_backup_id=selected_backup_id,
        )
        await interaction.response.edit_message(view=new_view)


class BackupVaultPageButton(discord.ui.Button["BackupListCardView"]):
    def __init__(self, *, label: str, delta: int, disabled: bool) -> None:
        super().__init__(label=label, style=discord.ButtonStyle.secondary, disabled=disabled)
        self.delta = delta

    async def callback(self, interaction: discord.Interaction) -> None:
        next_page = max(0, min(self.view.page + self.delta, self.view.max_page_index))
        page_entries = self.view.entries[next_page * BACKUP_VAULT_PAGE_SIZE:(next_page + 1) * BACKUP_VAULT_PAGE_SIZE]
        selected_backup_id = self.view.selected_backup_id
        if page_entries and selected_backup_id not in {entry.get("id") for entry in page_entries}:
            selected_backup_id = None
        new_view = BackupListCardView(
            self.view.bot_user,
            author_id=self.view.author_id,
            entries=self.view.entries,
            guild_id=self.view.guild_id,
            backup_limit=self.view.backup_limit,
            plan_label=self.view.plan_label,
            page=next_page,
            selected_backup_id=selected_backup_id,
        )
        await interaction.response.edit_message(view=new_view)


class BackupVaultLoadButton(discord.ui.Button["BackupListCardView"]):
    def __init__(self, disabled: bool) -> None:
        super().__init__(label="Load this backup", style=discord.ButtonStyle.primary, disabled=disabled)

    async def callback(self, interaction: discord.Interaction) -> None:
        selected_entry = self.view.selected_entry
        if selected_entry is None:
            await interaction.response.send_message("Select a backup first.", ephemeral=True)
            return
        if interaction.guild is None:
            await interaction.response.send_message("Run this inside the target server.", ephemeral=True)
            return

        record = await get_user_backup_record_async(self.view.author_id, selected_entry["id"])
        if record is None:
            entries = await list_user_backup_entries_async(self.view.author_id)
            new_view = BackupListCardView(
                self.view.bot_user,
                author_id=self.view.author_id,
                entries=entries,
                guild_id=self.view.guild_id,
                backup_limit=self.view.backup_limit,
                plan_label=self.view.plan_label,
                page=min(self.view.page, max(0, (len(entries) - 1) // BACKUP_VAULT_PAGE_SIZE) if entries else 0),
                selected_backup_id=None,
            )
            await interaction.response.edit_message(view=new_view)
            return

        planner = BackupLoadPlannerView(
            self.view.author_id,
            record["id"],
            record.get("source_guild_name", "Unknown Source"),
            record["snapshot"],
            interaction.guild,
            self.view.bot_user,
        )
        await interaction.response.edit_message(view=planner)
        target_warnings = build_backup_hierarchy_warnings(record["snapshot"], interaction.guild, set(planner.selected_actions))
        if target_warnings:
            await interaction.followup.send(
                view=RoleSafetyWarningCardView(
                    self.view.bot_user,
                    "Target Role Safety Audit",
                    "CLINX detected a hierarchy or permission issue in the target server before the restore started.",
                    target_warnings,
                ),
                ephemeral=True,
            )


class BackupVaultDeleteButton(discord.ui.Button["BackupListCardView"]):
    def __init__(self, disabled: bool) -> None:
        super().__init__(label="Delete this backup", style=discord.ButtonStyle.danger, disabled=disabled)

    async def callback(self, interaction: discord.Interaction) -> None:
        selected_entry = self.view.selected_entry
        if selected_entry is None:
            await interaction.response.send_message("Select a backup first.", ephemeral=True)
            return

        deleted = await delete_user_backup_async(self.view.author_id, selected_entry["id"])
        entries = await list_user_backup_entries_async(self.view.author_id)
        new_view = BackupListCardView(
            self.view.bot_user,
            author_id=self.view.author_id,
            entries=entries,
            guild_id=self.view.guild_id,
            backup_limit=self.view.backup_limit,
            plan_label=self.view.plan_label,
            page=min(self.view.page, max(0, (len(entries) - 1) // BACKUP_VAULT_PAGE_SIZE) if entries else 0),
            selected_backup_id=None,
        )
        if not deleted:
            await interaction.response.edit_message(view=new_view)
            await interaction.followup.send("That backup no longer exists in your vault.", ephemeral=True)
            return
        await interaction.response.edit_message(view=new_view)


class BackupListCardView(discord.ui.LayoutView):
    def __init__(
        self,
        bot_user: discord.ClientUser | None,
        *,
        author_id: int,
        entries: list[dict[str, Any]],
        guild_id: int | None,
        backup_limit: int,
        plan_label: str,
        page: int = 0,
        selected_backup_id: str | None = None,
    ) -> None:
        super().__init__(timeout=900)
        self.bot_user = bot_user
        self.author_id = author_id
        self.entries = entries
        self.guild_id = guild_id
        self.vault_policy = (
            get_backup_vault_policy_for_guild(guild_id)
            if guild_id is not None
            else {
                "state": "free",
                "creation_limit": backup_limit,
                "plan_limit": backup_limit,
                "plan_label": plan_label,
                "badge_label": plan_label,
                "status_text": f"Free vault • up to `{backup_limit}` backups",
            }
        )
        self.backup_limit = int(self.vault_policy.get("creation_limit") or backup_limit)
        self.plan_limit = int(self.vault_policy.get("plan_limit") or self.backup_limit)
        self.plan_label = str(self.vault_policy.get("badge_label") or plan_label)
        self.at_risk_ids = compute_at_risk_backup_ids(entries, self.vault_policy)
        self.page = page
        self.selected_backup_id = selected_backup_id
        self.rebuild()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the backup owner can use this vault card.", ephemeral=True)
            return False
        return True

    @property
    def max_page_index(self) -> int:
        return max(0, (len(self.entries) - 1) // BACKUP_VAULT_PAGE_SIZE) if self.entries else 0

    @property
    def current_page_entries(self) -> list[dict[str, Any]]:
        start = self.page * BACKUP_VAULT_PAGE_SIZE
        return self.entries[start : start + BACKUP_VAULT_PAGE_SIZE]

    @property
    def selected_entry(self) -> dict[str, Any] | None:
        if not self.entries or not self.selected_backup_id:
            return None
        for entry in self.entries:
            if entry.get("id") == self.selected_backup_id:
                return entry
        return None

    def rebuild(self) -> None:
        self.clear_items()
        hero = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        is_developer_vault = self.author_id in DEVELOPER_USER_IDS
        slot_cap_label = "∞" if is_developer_vault else str(self.backup_limit)
        page_entries = self.current_page_entries
        selected_entry = self.selected_entry
        vault_badge = discord.ui.Button(label=self.plan_label, style=discord.ButtonStyle.primary, disabled=True)
        count_badge = discord.ui.Button(
            label=f"{len(self.entries)}/{slot_cap_label} slots",
            style=discord.ButtonStyle.secondary,
            disabled=True,
        )
        vault_feed_text = format_vault_storage_state(self.vault_policy, at_risk_count=len(self.at_risk_ids))
        header_strip = (
            "🗂 Private vault lane • 🔐 Owner-locked • ⚡ Restore-ready"
            if not is_developer_vault else
            "🗂 Developer vault lane • ♾ Unlimited creator slots • ⚡ Restore-ready"
        )

        if not self.entries:
            container = discord.ui.Container(
                discord.ui.Section(
                    discord.ui.TextDisplay("## <> Backup Vault"),
                    discord.ui.TextDisplay("Your private CLINX vault is empty right now. Create a new backup to populate it."),
                    discord.ui.TextDisplay(header_strip),
                    accessory=hero,
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Vault State"),
                    discord.ui.TextDisplay(
                        f"**0/{slot_cap_label}** slots used\n"
                        f"{vault_feed_text}"
                    ),
                    accessory=vault_badge,
                ),
                discord.ui.TextDisplay(
                    "### Next Step\n"
                    "- Run **`/backup create`** in the server you want to snapshot.\n"
                    "- CLINX will store that backup under your account only."
                ),
                accent_color=EMBED_INFO,
            )
            self.add_item(container)
            return

        if selected_entry is None:
            page_feed_blocks: list[str] = []
            for index, entry in enumerate(page_entries, start=(self.page * BACKUP_VAULT_PAGE_SIZE) + 1):
                page_feed_blocks.append(
                    "\n".join(
                        [
                            f"**{index}.** **{entry.get('id', 'unknown')}**",
                            f"↳ Source: **{entry.get('source_guild_name', 'Unknown Source')}**",
                            f"↳ Created: **{format_backup_timestamp(entry.get('created_at'))}**",
                            "────────────────",
                        ]
                    )
                )
            container = discord.ui.Container(
                discord.ui.Section(
                    discord.ui.TextDisplay("## <> Backup Vault"),
                    discord.ui.TextDisplay(
                        "Private recovery IDs owned by your account are listed here."
                    ),
                    discord.ui.TextDisplay(header_strip),
                    accessory=hero,
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Vault Feed"),
                    discord.ui.TextDisplay(
                        f"**{len(self.entries)}/{slot_cap_label}** private backups stored\n"
                        f"{vault_feed_text}"
                    ),
                    accessory=discord.ui.Button(label="Private", style=discord.ButtonStyle.secondary, disabled=True),
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Your Backups"),
                    discord.ui.TextDisplay(
                        f"Current page: **{self.page + 1} / {self.max_page_index + 1}**\n"
                        "────────────\n\n"
                        f"{chr(10).join(page_feed_blocks[:4]) if page_feed_blocks else '- No backups on this page.'}"
                    ),
                    accessory=count_badge,
                ),
                accent_color=EMBED_INFO,
            )
            self.add_item(container)
        else:
            summary = selected_entry.get("summary")
            created_at = format_backup_timestamp(selected_entry.get("created_at"))
            counts_text = (
                f"Categories: **{(summary or {}).get('categories_count', 0)}**\n"
                f"Channels: **{(summary or {}).get('channels_count', 0)}**\n"
                f"Roles: **{(summary or {}).get('roles_count', 0)}**\n"
                f"{format_backup_retention_label(selected_entry.get('id'), self.vault_policy, at_risk_ids=self.at_risk_ids)}"
            )
            container = discord.ui.Container(
                discord.ui.Section(
                    discord.ui.TextDisplay("## <> Backup Vault"),
                    discord.ui.TextDisplay("Select one of your backups, inspect its saved structure, then load or delete it from the same card."),
                    discord.ui.TextDisplay(header_strip),
                    accessory=hero,
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Vault Feed"),
                    discord.ui.TextDisplay(
                        f"**{len(self.entries)}/{slot_cap_label}** private backups stored\n"
                        f"Current page: **{self.page + 1} / {self.max_page_index + 1}**\n"
                        f"{vault_feed_text}"
                    ),
                    accessory=count_badge,
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay(f"### Backup Info - {selected_entry.get('source_guild_name', 'Unknown Source')}"),
                    discord.ui.TextDisplay(
                        f"ID: **{selected_entry.get('id', 'unknown')}**\n"
                        f"Created At: **{created_at}**\n"
                        f"{counts_text}"
                    ),
                    accessory=vault_badge,
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Structure Preview"),
                    discord.ui.TextDisplay(format_backup_structure_preview(summary)),
                    accessory=discord.ui.Button(label="Layout", style=discord.ButtonStyle.secondary, disabled=True),
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Role Stack"),
                    discord.ui.TextDisplay(format_backup_role_preview(summary)),
                    accessory=discord.ui.Button(label="Roles", style=discord.ButtonStyle.secondary, disabled=True),
                ),
                accent_color=EMBED_INFO,
            )
            self.add_item(container)
        self.add_item(discord.ui.ActionRow(BackupVaultSelect(page_entries, self.selected_backup_id)))
        self.add_item(
            discord.ui.ActionRow(
                BackupVaultPageButton(label="Previous Page", delta=-1, disabled=self.page <= 0),
                BackupVaultPageButton(label="Next Page", delta=1, disabled=self.page >= self.max_page_index),
                BackupVaultLoadButton(disabled=selected_entry is None),
                BackupVaultDeleteButton(disabled=selected_entry is None),
            )
        )


class PingCardView(discord.ui.LayoutView):
    def __init__(self, bot_user: discord.ClientUser | None, *, latency_ms: int) -> None:
        super().__init__(timeout=None)
        hero = (
            discord.ui.Thumbnail(bot_user.display_avatar.url)
            if bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        band_label = (
            "Ultra Low" if latency_ms <= 80 else
            "Stable" if latency_ms <= 160 else
            "Elevated"
        )
        band_style = (
            discord.ButtonStyle.success if latency_ms <= 80 else
            discord.ButtonStyle.primary if latency_ms <= 160 else
            discord.ButtonStyle.secondary
        )
        self.add_item(
            discord.ui.Container(
                discord.ui.Section(
                    discord.ui.TextDisplay("## <> CLINX Ping"),
                    discord.ui.TextDisplay("Live gateway latency from the current bot session."),
                    accessory=hero,
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Gateway"),
                    discord.ui.TextDisplay(f"`{latency_ms} ms`"),
                    accessory=discord.ui.Button(label=band_label, style=band_style, disabled=True),
                ),
                discord.ui.TextDisplay(
                    "### Readout\n"
                    f"- Gateway heartbeat: `{latency_ms} ms`\n"
                    f"- Session state: `online`\n"
                    "- Use this as the quick check after deploys and restarts."
                ),
                accent_color=EMBED_INFO,
            )
        )


class PremiumStatusCardView(discord.ui.LayoutView):
    def __init__(
        self,
        bot_user: discord.ClientUser | None,
        guild: discord.Guild,
        *,
        entitlement: dict[str, Any] | None,
    ) -> None:
        super().__init__(timeout=None)
        self.bot_user = bot_user
        self.guild = guild
        self.entitlement = entitlement
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()
        hero = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        entitlement = enrich_premium_entitlement(self.entitlement) or {"plan_key": "free", "state": "free"}
        plan_key = str(entitlement.get("plan_key") or "free")
        theme = PREMIUM_CARD_THEMES.get(plan_key, PREMIUM_CARD_THEMES["free"])
        plan = PREMIUM_PLAN_CATALOG.get(plan_key)
        state = str(entitlement.get("state") or "free")
        server_name = self.guild.name.strip() if getattr(self.guild, "name", "") else "This Server"
        plan_name = str(entitlement.get("plan_name") or (plan["display_name"] if plan else "Free"))
        billing_line = (
            f"Renews {format_backup_timestamp(entitlement.get('expires_at'))}"
            if state == "active" else
            f"Grace until {format_backup_timestamp(entitlement.get('grace_ends_at'))}"
            if state == "grace" else
            "Free lane active"
        )
        backup_cap = PLAN_BACKUP_LIMITS.get(plan_key, PLAN_BACKUP_LIMITS["free"])
        access_label = (
            "Premium Live" if state == "active" else
            "Grace Window" if state == "grace" else
            "Free Lane"
        )
        detail_lines = [
            f"**Tier**  {plan_name}",
            f"**Server**  {server_name}",
            f"**Vault**  {backup_cap} slots",
            f"**State**  {access_label}",
            f"**Billing**  {billing_line}",
        ]
        if state == "expired":
            detail_lines.append("**Retention**  Backups above the free cap are now at risk")
        elif state == "active":
            detail_lines.append("**Access**  Premium tooling is live for CLINX-permitted members")
        else:
            detail_lines.append("**Access**  Renew to restore premium backup creation limits")

        self.add_item(
            discord.ui.Container(
                discord.ui.Section(
                    discord.ui.TextDisplay(f"## {theme['title']}"),
                    discord.ui.TextDisplay(f"{theme['metal']} tier issued for **{server_name}**"),
                    discord.ui.TextDisplay("────────────"),
                    accessory=hero,
                ),
                discord.ui.Section(
                    discord.ui.TextDisplay("\n".join(detail_lines)),
                    accessory=discord.ui.Button(
                        label=str(theme["badge"]),
                        style=discord.ButtonStyle.primary if state == "active" else discord.ButtonStyle.secondary,
                        disabled=True,
                    ),
                ),
                accent_color=int(theme["accent"]),
            )
        )


class SafetyRosterCardView(discord.ui.LayoutView):
    def __init__(
        self,
        bot_user: discord.ClientUser | None,
        guild: discord.Guild,
        *,
        title: str,
        subtitle: str,
        trusted_ids: list[str],
        badge_label: str,
        badge_style: discord.ButtonStyle,
        accent_color: int,
    ) -> None:
        super().__init__(timeout=None)
        self.bot_user = bot_user
        self.guild = guild
        self.title = title
        self.subtitle = subtitle
        self.trusted_ids = trusted_ids
        self.badge_label = badge_label
        self.badge_style = badge_style
        self.accent_color = accent_color
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()
        hero = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        roster_lines: list[str] = []
        for index, user_id in enumerate(self.trusted_ids, start=1):
            member = self.guild.get_member(int(user_id))
            roster_lines.append(f"**{index}.** {member.mention if member else f'`{user_id}`'}")
        if not roster_lines:
            roster_lines.append("- No trusted CLINX admins are configured in this server.")

        self.add_item(
            discord.ui.Container(
                discord.ui.Section(
                    discord.ui.TextDisplay(f"## <> {self.title}"),
                    discord.ui.TextDisplay(self.subtitle),
                    accessory=hero,
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Trust State"),
                    discord.ui.TextDisplay(f"`{len(self.trusted_ids)}` trusted admin slot(s) configured"),
                    accessory=discord.ui.Button(label=self.badge_label, style=self.badge_style, disabled=True),
                ),
                discord.ui.TextDisplay("### Trusted Admins\n" + "\n".join(roster_lines)),
                discord.ui.TextDisplay(
                    "### Scope\n"
                    "- Trusted admins bypass owner approval for Tier 2 and Tier 3 CLINX actions.\n"
                    "- `/leave` and `/safety` commands stay locked to the actual server owner."
                ),
                accent_color=self.accent_color,
            )
        )


class FullAccessRosterCardView(discord.ui.LayoutView):
    def __init__(
        self,
        bot_user: discord.ClientUser | None,
        guild: discord.Guild,
        *,
        title: str,
        subtitle: str,
        user_ids: list[str],
        badge_label: str,
        badge_style: discord.ButtonStyle,
        accent_color: int,
    ) -> None:
        super().__init__(timeout=None)
        self.bot_user = bot_user
        self.guild = guild
        self.title = title
        self.subtitle = subtitle
        self.user_ids = user_ids
        self.badge_label = badge_label
        self.badge_style = badge_style
        self.accent_color = accent_color
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()
        hero = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        roster_lines: list[str] = []
        for index, user_id in enumerate(self.user_ids, start=1):
            member = self.guild.get_member(int(user_id))
            roster_lines.append(f"**{index}.** {member.mention if member else f'`{user_id}`'}")
        if not roster_lines:
            roster_lines.append("- No full-access overrides are configured in this server.")

        self.add_item(
            discord.ui.Container(
                discord.ui.Section(
                    discord.ui.TextDisplay(f"## <> {self.title}"),
                    discord.ui.TextDisplay(self.subtitle),
                    accessory=hero,
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Override State"),
                    discord.ui.TextDisplay(f"`{len(self.user_ids)}` full-access override(s) configured"),
                    accessory=discord.ui.Button(label=self.badge_label, style=self.badge_style, disabled=True),
                ),
                discord.ui.TextDisplay("### Full Access Users\n" + "\n".join(roster_lines)),
                discord.ui.TextDisplay(
                    "### Scope\n"
                    "- These users bypass CLINX runtime command gates in this server.\n"
                    "- This override is developer-managed and not shown in the normal safety owner flow."
                ),
                accent_color=self.accent_color,
            )
        )


class PremiumGiftCardView(discord.ui.LayoutView):
    def __init__(
        self,
        bot_user: discord.ClientUser | None,
        guild: discord.Guild,
        *,
        gifted_member: discord.Member,
        gifted_by_id: int,
        entitlement: dict[str, Any],
    ) -> None:
        super().__init__(timeout=None)
        self.bot_user = bot_user
        self.guild = guild
        self.gifted_member = gifted_member
        self.gifted_by_id = gifted_by_id
        self.entitlement = entitlement
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()
        hero = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        plan = PREMIUM_PLAN_CATALOG[self.entitlement["plan_key"]]
        server_name = self.guild.name.strip() if getattr(self.guild, "name", "") else "this server"
        feature_lines = "\n".join(f"- {feature}" for feature in plan["features"])
        gifted_at = format_backup_timestamp(self.entitlement.get("gifted_at"))
        expires_at = format_backup_timestamp(self.entitlement.get("expires_at"))
        subtitle = (
            f"{self.gifted_member.mention} now has **{plan['display_name']}** active for this server."
        )
        premium_strip = (
            f"Plan: **{plan['display_name']}**  •  Server: **{server_name}**\n"
            f"Billing: **Monthly**  •  Renews: **{expires_at}**"
        )
        payload_text = (
            f"Recipient: {self.gifted_member.mention}\n"
            f"Plan: **{plan['display_name']}**\n"
            f"Value: **{plan['price_label']}**\n"
            f"Tier Badge: **{plan['badge_label']}**"
        )
        activation_text = (
            "### Activation Status\n"
            f"- Gifted by: <@{self.gifted_by_id}>\n"
            f"- Activated: **{gifted_at}**"
        )
        access_text = (
            "### Server Access\n"
            f"- Premium is now active for CLINX-permitted members in **{server_name}**.\n"
            "- Safety gates and owner approval rules still apply where required."
        )
        self.add_item(
            discord.ui.Container(
                discord.ui.Section(
                    discord.ui.TextDisplay("## 🎁 Premium Gift Delivered"),
                    discord.ui.TextDisplay(subtitle),
                    discord.ui.TextDisplay(premium_strip),
                    accessory=hero,
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Gift Payload"),
                    discord.ui.TextDisplay(payload_text),
                    accessory=discord.ui.Button(
                        label=plan["badge_label"],
                        style=discord.ButtonStyle.primary,
                        disabled=True,
                    ),
                ),
                discord.ui.Separator(),
                discord.ui.TextDisplay(f"### Included\n{feature_lines}"),
                discord.ui.Separator(),
                discord.ui.TextDisplay(activation_text),
                discord.ui.Separator(),
                discord.ui.TextDisplay(access_text),
                accent_color=0x4F8CFF,
            )
        )


class DeveloperDashboardSelect(discord.ui.Select["DeveloperDashboardView"]):
    def __init__(self, page_entries: list[dict[str, Any]], selected_key: str | None) -> None:
        options: list[discord.SelectOption] = []
        for entry in page_entries:
            if entry["kind"] == "obypass":
                label = entry["user_display_name"][:100]
                description = f"{entry['guild_name']} • {format_backup_timestamp(entry.get('granted_at'))}"[:100]
            else:
                status = "Active" if entry.get("active") else "Cancelled"
                label = f"{entry['plan_name']} • {entry['gifted_to_display_name']}"[:100]
                description = f"{entry['guild_name']} • {status}"[:100]
            options.append(
                discord.SelectOption(
                    label=label,
                    description=description,
                    value=entry["key"],
                    default=entry["key"] == selected_key,
                )
            )
        super().__init__(placeholder="Select a record", options=options, disabled=not options)

    async def callback(self, interaction: discord.Interaction) -> None:
        new_view = self.view.spawn(selected_key=self.values[0], action_note=None)
        await interaction.response.edit_message(view=new_view)


class DeveloperDashboardPageButton(discord.ui.Button["DeveloperDashboardView"]):
    def __init__(self, *, label: str, delta: int, disabled: bool) -> None:
        super().__init__(label=label, style=discord.ButtonStyle.secondary, disabled=disabled)
        self.delta = delta

    async def callback(self, interaction: discord.Interaction) -> None:
        next_page = max(0, min(self.view.page + self.delta, self.view.max_page_index))
        page_entries = self.view.entries[next_page * DEVELOPER_DASHBOARD_PAGE_SIZE:(next_page + 1) * DEVELOPER_DASHBOARD_PAGE_SIZE]
        selected_key = self.view.selected_key
        if page_entries and selected_key not in {entry["key"] for entry in page_entries}:
            selected_key = None
        new_view = self.view.spawn(page=next_page, selected_key=selected_key, action_note=None)
        await interaction.response.edit_message(view=new_view)


class DeveloperDashboardActionButton(discord.ui.Button["DeveloperDashboardView"]):
    def __init__(self, mode: str, selected_entry: dict[str, Any] | None) -> None:
        if mode == "obypass":
            super().__init__(label="Revoke OBypass", style=discord.ButtonStyle.danger, disabled=selected_entry is None)
            return
        disabled = selected_entry is None or not selected_entry.get("active", False)
        super().__init__(label="Cancel Premium", style=discord.ButtonStyle.danger, disabled=disabled)

    async def callback(self, interaction: discord.Interaction) -> None:
        selected_entry = self.view.selected_entry
        if selected_entry is None:
            await interaction.response.send_message("Select a record first.", ephemeral=True)
            return

        if selected_entry["kind"] == "obypass":
            revoke_full_access_by_user_id(selected_entry["guild_id"], int(selected_entry["user_id"]))
            action_note = (
                f"Removed {format_actor_label(selected_entry['user_display_name'], selected_entry['user_id'])} "
                f"from `{selected_entry['guild_name']}`."
            )
            new_view = self.view.spawn(selected_key=None, action_note=action_note)
        else:
            cancelled = cancel_guild_premium_entitlement(
                selected_entry["guild_id"],
                cancelled_by_user_id=interaction.user.id,
                cancelled_by_display_name=getattr(interaction.user, "display_name", str(interaction.user)),
            )
            if cancelled is None:
                action_note = "Premium entitlement was not found anymore."
            else:
                action_note = (
                    f"Cancelled `{selected_entry['plan_name']}` for "
                    f"{format_actor_label(selected_entry['gifted_to_display_name'], selected_entry['gifted_to_user_id'])} "
                    f"in `{selected_entry['guild_name']}`."
                )
            new_view = self.view.spawn(selected_key=selected_entry["key"], action_note=action_note)

        await interaction.response.edit_message(view=new_view)


class DeveloperDashboardView(discord.ui.LayoutView):
    def __init__(
        self,
        bot_instance: commands.Bot,
        *,
        author_id: int,
        mode: str = "obypass",
        page: int = 0,
        selected_key: str | None = None,
        action_note: str | None = None,
    ) -> None:
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
        self.bot_user = bot_instance.user
        self.author_id = author_id
        self.mode = mode if mode in {"obypass", "premium"} else "obypass"
        self.action_note = action_note
        self.entries = build_developer_dashboard_entries(bot_instance, developer_id=author_id, mode=self.mode)
        self.max_page_index = max(0, (len(self.entries) - 1) // DEVELOPER_DASHBOARD_PAGE_SIZE) if self.entries else 0
        self.page = max(0, min(page, self.max_page_index))
        self.selected_key = selected_key if selected_key in {entry["key"] for entry in self.entries} else None
        self.rebuild()

    @property
    def page_entries(self) -> list[dict[str, Any]]:
        start = self.page * DEVELOPER_DASHBOARD_PAGE_SIZE
        return self.entries[start:start + DEVELOPER_DASHBOARD_PAGE_SIZE]

    @property
    def selected_entry(self) -> dict[str, Any] | None:
        if not self.selected_key:
            return None
        return next((entry for entry in self.entries if entry["key"] == self.selected_key), None)

    def spawn(
        self,
        *,
        mode: str | None = None,
        page: int | None = None,
        selected_key: str | None | object = Ellipsis,
        action_note: str | None | object = Ellipsis,
    ) -> "DeveloperDashboardView":
        return DeveloperDashboardView(
            self.bot_instance,
            author_id=self.author_id,
            mode=mode or self.mode,
            page=self.page if page is None else page,
            selected_key=self.selected_key if selected_key is Ellipsis else selected_key,
            action_note=self.action_note if action_note is Ellipsis else action_note,
        )

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "This developer console is locked to the CLINX developer.",
                ephemeral=True,
            )
            return False
        return True

    def _make_mode_button(self, mode_key: str, label: str) -> discord.ui.Button:
        button = discord.ui.Button(
            label=label,
            style=discord.ButtonStyle.primary if self.mode == mode_key else discord.ButtonStyle.secondary,
            disabled=self.mode == mode_key,
        )

        async def callback(interaction: discord.Interaction) -> None:
            await interaction.response.edit_message(view=self.spawn(mode=mode_key, page=0, selected_key=None, action_note=None))

        button.callback = callback
        return button

    def _make_refresh_button(self) -> discord.ui.Button:
        button = discord.ui.Button(label="Refresh", style=discord.ButtonStyle.success)

        async def callback(interaction: discord.Interaction) -> None:
            await interaction.response.edit_message(
                view=self.spawn(action_note=f"Console refreshed at `{format_backup_timestamp(utc_now_iso())}`.")
            )

        button.callback = callback
        return button

    def _build_page_feed(self) -> str:
        if not self.page_entries:
            return "- No records are available in this lane."
        lines: list[str] = []
        start_index = self.page * DEVELOPER_DASHBOARD_PAGE_SIZE
        for offset, entry in enumerate(self.page_entries, start=1):
            index = start_index + offset
            if entry["kind"] == "obypass":
                lines.append(
                    f"**{index}.** {format_actor_label(entry['user_display_name'], entry['user_id'])}\n"
                    f"Guild: `{entry['guild_name']}`\n"
                    f"Granted: `{format_backup_timestamp(entry.get('granted_at'))}`"
                )
                continue
            state = str(entry.get("state") or ("active" if entry.get("active") else "expired")).title()
            state_line = (
                f"Renews: `{format_backup_timestamp(entry.get('expires_at'))}`"
                if state == "Active" else
                f"Grace until: `{format_backup_timestamp(entry.get('grace_ends_at'))}`"
                if state == "Grace" else
                f"Expired: `{format_backup_timestamp(entry.get('grace_ends_at'))}`"
            )
            lines.append(
                f"**{index}.** `{entry['plan_name']}` -> {format_actor_label(entry['gifted_to_display_name'], entry['gifted_to_user_id'])}\n"
                f"Guild: `{entry['guild_name']}`\n"
                f"State: `{state}` · Gifted: `{format_backup_timestamp(entry.get('gifted_at'))}`\n"
                f"{state_line}"
            )
        return "\n".join(lines)

    def _build_selection_state(self) -> tuple[str, str, discord.ButtonStyle]:
        entry = self.selected_entry
        if entry is None:
            return (
                "Selection locked",
                "Pick a record from the selector below to unlock revoke or cancel actions.",
                discord.ButtonStyle.secondary,
            )

        if entry["kind"] == "obypass":
            return (
                "Override armed",
                (
                    f"User: {format_actor_label(entry['user_display_name'], entry['user_id'])}\n"
                    f"Guild: `{entry['guild_name']}`\n"
                    f"Granted: `{format_backup_timestamp(entry.get('granted_at'))}`\n"
                    f"Granted By: {format_actor_label(entry.get('granted_by_display_name'), entry.get('granted_by_user_id'))}"
                ),
                discord.ButtonStyle.primary,
            )

        state = str(entry.get("state") or ("active" if entry.get("active") else "expired"))
        status_label = state.title()
        cancelled_line = ""
        if entry.get("cancelled_at"):
            cancelled_line = (
                f"\nCancelled: `{format_backup_timestamp(entry.get('cancelled_at'))}`"
                f"\nCancelled By: {format_actor_label(entry.get('cancelled_by_display_name'), entry.get('cancelled_by_user_id'))}"
            )
        state_line = (
            f"\nRenews: `{format_backup_timestamp(entry.get('expires_at'))}`"
            if state == "active" else
            f"\nGrace Until: `{format_backup_timestamp(entry.get('grace_ends_at'))}`"
            if state == "grace" else
            f"\nGrace Ended: `{format_backup_timestamp(entry.get('grace_ends_at'))}`"
        )
        return (
            status_label,
            (
                f"Recipient: {format_actor_label(entry['gifted_to_display_name'], entry['gifted_to_user_id'])}\n"
                f"Guild: `{entry['guild_name']}`\n"
                f"Plan: `{entry['plan_name']}`\n"
                f"Gifted: `{format_backup_timestamp(entry.get('gifted_at'))}`"
                f"{state_line}"
                f"{cancelled_line}"
            ),
            discord.ButtonStyle.success if state == "active" else discord.ButtonStyle.primary if state == "grace" else discord.ButtonStyle.secondary,
        )

    def rebuild(self) -> None:
        self.clear_items()
        hero = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        lane_title = "OBypass Registry" if self.mode == "obypass" else "Premium Ledger"
        lane_summary = (
            f"`{len(self.entries)}` active override record(s) tracked across all cached CLINX servers."
            if self.mode == "obypass"
            else f"`{len(self.entries)}` gifted premium record(s) issued by your developer account."
        )
        selection_badge, selection_text, selection_style = self._build_selection_state()
        action_log = self.action_note or (
            "- Revoke OBypass removes the runtime override from the selected server.\n"
            "- Cancel Premium turns off the stored guild entitlement without deleting the record."
        )

        self.add_item(
            discord.ui.Container(
                discord.ui.Section(
                    discord.ui.TextDisplay("## <> Developer Console"),
                    discord.ui.TextDisplay("Track developer override grants and premium gifts from one private control surface."),
                    accessory=hero,
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Current Lane"),
                    discord.ui.TextDisplay(lane_summary),
                    accessory=discord.ui.Button(label=lane_title, style=discord.ButtonStyle.primary, disabled=True),
                ),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Selection State"),
                    discord.ui.TextDisplay(selection_text),
                    accessory=discord.ui.Button(label=selection_badge, style=selection_style, disabled=True),
                ),
                discord.ui.TextDisplay(
                    "### Page Feed\n"
                    f"Page: `{self.page + 1}` / `{self.max_page_index + 1 if self.entries else 1}`\n"
                    + self._build_page_feed()
                ),
                discord.ui.TextDisplay("### Action Log\n" + action_log),
                accent_color=EMBED_INFO if self.mode == "obypass" else EMBED_OK,
            )
        )
        self.add_item(
            discord.ui.ActionRow(
                self._make_mode_button("obypass", "OBypass"),
                self._make_mode_button("premium", "Premium"),
                self._make_refresh_button(),
            )
        )
        if self.page_entries:
            self.add_item(discord.ui.ActionRow(DeveloperDashboardSelect(self.page_entries, self.selected_key)))
        self.add_item(
            discord.ui.ActionRow(
                DeveloperDashboardPageButton(label="Previous", delta=-1, disabled=self.page <= 0),
                DeveloperDashboardPageButton(label="Next", delta=1, disabled=self.page >= self.max_page_index),
                DeveloperDashboardActionButton(self.mode, self.selected_entry),
            )
        )


class SafetyApprovalCardView(discord.ui.LayoutView):
    def __init__(self, bot_user: discord.ClientUser | None, request: dict[str, Any]) -> None:
        super().__init__(timeout=None)
        self.bot_user = bot_user
        self.request = request
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()
        hero = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        risk_style = discord.ButtonStyle.danger if self.request.get("tier") == 3 else discord.ButtonStyle.secondary
        status_style_map = {
            "pending": discord.ButtonStyle.secondary,
            "approved": discord.ButtonStyle.primary,
            "denied": discord.ButtonStyle.danger,
            "expired": discord.ButtonStyle.secondary,
            "completed": discord.ButtonStyle.success,
            "failed": discord.ButtonStyle.danger,
        }
        status = self.request.get("status", "pending")
        status_style = status_style_map.get(status, discord.ButtonStyle.secondary)
        sections: list[discord.ui.Item[Any]] = [
            discord.ui.Section(
                discord.ui.TextDisplay("## <> Owner Approval Required"),
                discord.ui.TextDisplay(self.request.get("subtitle", "A protected CLINX action is waiting for owner approval.")),
                accessory=hero,
            ),
            discord.ui.Separator(),
            discord.ui.Section(
                discord.ui.TextDisplay("### Request"),
                discord.ui.TextDisplay(
                    f"Requester: <@{self.request['requester_id']}>\n"
                    f"Command: `/{self.request['command_label']}`\n"
                    f"Target: `{self.request['target_name']}`"
                ),
                accessory=discord.ui.Button(label=self.request.get("risk_label", "Review"), style=risk_style, disabled=True),
            ),
            discord.ui.Section(
                discord.ui.TextDisplay("### Status"),
                discord.ui.TextDisplay(self.request.get("status_text", "Pending owner review.")),
                accessory=discord.ui.Button(label=status.title(), style=status_style, disabled=True),
            ),
        ]
        route_text = self.request.get("route_text")
        if route_text:
            sections.append(discord.ui.TextDisplay(f"### Route\n{route_text}"))
        if self.request.get("selected_actions_text"):
            sections.append(discord.ui.TextDisplay(f"### Selected Lanes\n{self.request['selected_actions_text']}"))
        if self.request.get("projected_text"):
            sections.append(discord.ui.TextDisplay(f"### Projected Changes\n{self.request['projected_text']}"))
        if self.request.get("result_text"):
            sections.append(discord.ui.TextDisplay(f"### Result\n{self.request['result_text']}"))
        sections.append(discord.ui.TextDisplay(f"Owner: <@{self.request['owner_id']}>"))
        accent_map = {
            "pending": EMBED_INFO,
            "approved": EMBED_INFO,
            "completed": EMBED_OK,
            "denied": EMBED_ERR,
            "failed": EMBED_ERR,
            "expired": EMBED_WARN,
        }
        self.add_item(discord.ui.Container(*sections, accent_color=accent_map.get(status, EMBED_INFO)))

        if status == "pending":
            self.add_item(
                discord.ui.ActionRow(
                    self._make_approve_button(),
                    self._make_deny_button(),
                )
            )

    def _make_approve_button(self) -> discord.ui.Button:
        button = discord.ui.Button(label="Approve", style=discord.ButtonStyle.success)

        async def callback(interaction: discord.Interaction) -> None:
            request = PENDING_SAFETY_REQUESTS.get(self.request["guild_id"])
            if request is None or request.get("id") != self.request.get("id"):
                await interaction.response.send_message("This approval request is no longer active.", ephemeral=True)
                return
            if request.get("status") != "pending":
                await interaction.response.send_message("This approval request is no longer pending.", ephemeral=True)
                return
            if interaction.user.id != request.get("owner_id"):
                await interaction.response.send_message("Only the actual server owner can approve this request.", ephemeral=True)
                return
            request["status"] = "approved"
            request["status_text"] = f"Approved by <@{interaction.user.id}>. CLINX is now executing this action."
            await interaction.response.edit_message(view=SafetyApprovalCardView(self.bot_user, request))
            try:
                await request["executor"](request)
            finally:
                if PENDING_SAFETY_REQUESTS.get(request["guild_id"], {}).get("id") == request.get("id"):
                    PENDING_SAFETY_REQUESTS.pop(request["guild_id"], None)

        button.callback = callback
        return button

    def _make_deny_button(self) -> discord.ui.Button:
        button = discord.ui.Button(label="Deny", style=discord.ButtonStyle.danger)

        async def callback(interaction: discord.Interaction) -> None:
            request = PENDING_SAFETY_REQUESTS.get(self.request["guild_id"])
            if request is None or request.get("id") != self.request.get("id"):
                await interaction.response.send_message("This approval request is no longer active.", ephemeral=True)
                return
            if interaction.user.id != request.get("owner_id"):
                await interaction.response.send_message("Only the actual server owner can deny this request.", ephemeral=True)
                return
            request["status"] = "denied"
            request["status_text"] = f"Denied by <@{interaction.user.id}>."
            await interaction.response.edit_message(view=SafetyApprovalCardView(self.bot_user, request))
            PENDING_SAFETY_REQUESTS.pop(request["guild_id"], None)

        button.callback = callback
        return button


async def sync_safety_request_message(guild_id: int) -> None:
    request = PENDING_SAFETY_REQUESTS.get(guild_id)
    if not request:
        return
    channel_id = request.get("channel_id")
    message_id = request.get("message_id")
    if not channel_id or not message_id:
        return
    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(channel_id)
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            return
    try:
        message = await channel.fetch_message(message_id)
        await message.edit(view=SafetyApprovalCardView(bot.user, request))
    except (discord.Forbidden, discord.NotFound, discord.HTTPException):
        return


async def expire_safety_request_after(guild_id: int, request_id: str) -> None:
    await asyncio.sleep(120)
    request = PENDING_SAFETY_REQUESTS.get(guild_id)
    if request is None or request.get("id") != request_id or request.get("status") != "pending":
        return
    request["status"] = "expired"
    request["status_text"] = "Owner approval expired after 2 minutes."
    await sync_safety_request_message(guild_id)
    PENDING_SAFETY_REQUESTS.pop(guild_id, None)


async def create_safety_request(
    interaction: discord.Interaction,
    *,
    command_name: str,
    subtitle: str,
    risk_label: str,
    route_text: str | None,
    selected_actions_text: str | None,
    projected_text: str | None,
    executor: Any,
) -> bool:
    guild = interaction.guild
    if guild is None or interaction.channel is None or not hasattr(interaction.channel, "send"):
        return False
    existing = PENDING_SAFETY_REQUESTS.get(guild.id)
    if existing and existing.get("status") == "pending":
        if interaction.response.is_done():
            await interaction.followup.send("Another protected CLINX request is already waiting for owner approval in this server.", ephemeral=True)
        else:
            await interaction.response.send_message("Another protected CLINX request is already waiting for owner approval in this server.", ephemeral=True)
        return False

    request_id = secrets.token_hex(4).upper()
    tier = 3 if risk_label == "Destructive" else 2
    request = {
        "id": request_id,
        "guild_id": guild.id,
        "owner_id": guild.owner_id,
        "requester_id": interaction.user.id,
        "command_label": command_name,
        "target_name": guild.name,
        "subtitle": subtitle,
        "risk_label": risk_label,
        "tier": tier,
        "status": "pending",
        "status_text": "Pending owner review. Only the actual server owner can approve or deny this request.",
        "route_text": route_text,
        "selected_actions_text": selected_actions_text,
        "projected_text": projected_text,
        "result_text": None,
        "executor": executor,
        "channel_id": interaction.channel.id,
        "preserve_channel_id": interaction.channel.id if isinstance(interaction.channel, discord.TextChannel) else None,
        "message_id": None,
        "task": None,
    }
    PENDING_SAFETY_REQUESTS[guild.id] = request
    if interaction.response.is_done():
        message = await interaction.followup.send(
            content=f"<@{guild.owner_id}>",
            view=SafetyApprovalCardView(interaction.client.user if isinstance(interaction.client, commands.Bot) else None, request),
            ephemeral=False,
            wait=True,
        )
    else:
        await interaction.response.send_message(
            content=f"<@{guild.owner_id}>",
            view=SafetyApprovalCardView(interaction.client.user if isinstance(interaction.client, commands.Bot) else None, request),
            ephemeral=False,
        )
        message = await interaction.original_response()
    request["message_id"] = message.id
    request["task"] = asyncio.create_task(expire_safety_request_after(guild.id, request_id))
    return True


async def send_access_denied(interaction: discord.Interaction, message: str) -> None:
    if interaction.response.is_done():
        await interaction.followup.send(embed=make_embed("Access Denied", message, EMBED_ERR, interaction), ephemeral=True)
    else:
        await interaction.response.send_message(embed=make_embed("Access Denied", message, EMBED_ERR, interaction), ephemeral=True)


async def resolve_backup_status_channel(guild_id: int, job: dict[str, Any]) -> discord.abc.Messageable | None:
    restore_channel_name = "clinx-restoring"
    channel_id = job.get("status_channel_id")
    channel = bot.get_channel(channel_id) if channel_id else None
    if channel is None and channel_id:
        try:
            channel = await bot.fetch_channel(channel_id)
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            channel = None
    if channel is not None and hasattr(channel, "send"):
        channel_name = getattr(channel, "name", restore_channel_name)
        job["status_channel_name"] = channel_name
        return channel

    guild = bot.get_guild(guild_id)
    if guild is None or bot.user is None:
        return None
    me = guild.me or guild.get_member(bot.user.id)

    restoring = discord.utils.get(guild.text_channels, name=restore_channel_name)
    if restoring is not None and me is not None:
        permissions = restoring.permissions_for(me)
        if permissions.view_channel and permissions.send_messages:
            job["status_channel_id"] = restoring.id
            job["status_message_id"] = None
            job["status_channel_name"] = restoring.name
            return restoring

    if me is not None and me.guild_permissions.manage_channels:
        try:
            restoring = await guild.create_text_channel(restore_channel_name, reason="CLINX backup load: restore status fallback channel")
            job["status_channel_id"] = restoring.id
            job["status_message_id"] = None
            job["status_channel_name"] = restoring.name
            return restoring
        except (discord.Forbidden, discord.HTTPException):
            pass

    fallback = resolve_notice_channel(guild)
    if fallback is not None and hasattr(fallback, "send"):
        fallback_id = getattr(fallback, "id", None)
        if fallback_id is not None:
            job["status_channel_id"] = fallback_id
            job["status_message_id"] = None
        job["status_channel_name"] = getattr(fallback, "name", restore_channel_name)
        return fallback
    return None


class BackupLoadStatusCardView(discord.ui.LayoutView):
    def __init__(self, bot_user: discord.ClientUser | None, job: dict[str, Any]) -> None:
        super().__init__(timeout=None)
        self.bot_user = bot_user
        self.job = job
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()
        hero = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        status = self.job.get("status", "unknown")
        badge_map = {
            "running": ("Live", EMBED_INFO, discord.ButtonStyle.primary),
            "completed": ("Completed", EMBED_OK, discord.ButtonStyle.success),
            "cancelled": ("Cancelled", 0x64748B, discord.ButtonStyle.secondary),
            "failed": ("Failed", EMBED_ERR, discord.ButtonStyle.danger),
        }
        badge_label, accent, badge_style = badge_map.get(status, ("Unknown", EMBED_INFO, discord.ButtonStyle.secondary))
        progress_percent, progress_bar, stage_index, stage_total = get_backup_progress_state(self.job)
        route_text = f"`{self.job.get('source_name', 'Unknown')}` -> `{self.job.get('target_name', 'Unknown')}`"
        lane_text = "\n".join(build_backup_lane_lines(set(self.job.get("selected_actions", []))))
        phase_text = self.job.get("phase", "Queued")
        phase_detail = self.job.get("phase_detail", "CLINX is preparing the restore lane.")
        live_stats = self.job.get("stats", {})

        if status == "running":
            preview = self.job.get("preview", {})
            state_lines = [f"- **Phase:** {phase_text}", f"- {phase_detail}"]
            if any(live_stats.values()):
                if live_stats.get("deleted_roles"):
                    state_lines.append(f"- `{live_stats['deleted_roles']}` roles deleted so far")
                if live_stats.get("deleted_channels"):
                    state_lines.append(f"- `{live_stats['deleted_channels']}` channels deleted so far")
                if live_stats.get("created_roles"):
                    state_lines.append(f"- `{live_stats['created_roles']}` roles created so far")
                if live_stats.get("updated_roles"):
                    state_lines.append(f"- `{live_stats['updated_roles']}` roles updated so far")
                if live_stats.get("created_categories"):
                    state_lines.append(f"- `{live_stats['created_categories']}` categories created so far")
                if live_stats.get("created_channels"):
                    state_lines.append(f"- `{live_stats['created_channels']}` channels created so far")
                if live_stats.get("updated_channels"):
                    state_lines.append(f"- `{live_stats['updated_channels']}` channels finalized so far")
                if live_stats.get("updated_settings"):
                    state_lines.append("- Settings sync has started")
                if live_stats.get("blocked_roles"):
                    state_lines.append(f"- `{live_stats['blocked_roles']}` role operations were blocked")
            else:
                if preview.get("deleted_roles"):
                    state_lines.append(f"- `{preview['deleted_roles']}` roles queued for deletion")
                if preview.get("deleted_channels"):
                    state_lines.append(f"- `{preview['deleted_channels']}` channels queued for deletion")
                if preview.get("created_roles"):
                    state_lines.append(f"- `{preview['created_roles']}` roles queued for creation")
                if preview.get("created_categories"):
                    state_lines.append(f"- `{preview['created_categories']}` categories queued for creation")
                if preview.get("created_channels"):
                    state_lines.append(f"- `{preview['created_channels']}` channels queued for creation")
                if preview.get("updated_settings"):
                    state_lines.append("- Guild settings queued for sync")
            body_text = "\n".join(state_lines)
            footer_text = "This card updates automatically in the active restore status channel."
        else:
            result_lines = []
            if live_stats:
                for key, label in (
                    ("deleted_roles", "Deleted roles"),
                    ("deleted_channels", "Deleted channels"),
                    ("created_roles", "Created roles"),
                    ("updated_roles", "Updated roles"),
                    ("created_categories", "Created categories"),
                    ("created_channels", "Created channels"),
                    ("updated_channels", "Updated channels"),
                    ("updated_settings", "Updated settings"),
                    ("blocked_roles", "Blocked roles"),
                ):
                    if live_stats.get(key):
                        result_lines.append(f"- `{live_stats[key]}` {label.lower()}")
            if self.job.get("error"):
                result_lines.append(f"- Error: `{self.job['error']}`")
            if not result_lines:
                result_lines.append("- No result counters available")
            body_text = "\n".join([f"- **Phase:** {phase_text}", f"- {phase_detail}", *result_lines])
            footer_text = f"Started `{self.job.get('started_at', 'n/a')}`"
            if self.job.get("finished_at"):
                footer_text += f" | Finished `{self.job['finished_at']}`"

        transit_summary = (
            f"### Transit Progress\n"
            f"```text\n{progress_bar}\n```\n"
            f"`{progress_percent}%` locked · Stage `{stage_index}` of `{stage_total}`"
        )
        status_channel_name = self.job.get("status_channel_name", "clinx-restoring")
        children: list[discord.ui.Item[Any]] = [
            discord.ui.Section(
                discord.ui.TextDisplay("## <> Backup Transit"),
                discord.ui.TextDisplay("Live restore telemetry for the current server load lane."),
                accessory=hero,
            ),
            discord.ui.Separator(),
            discord.ui.Section(
                discord.ui.TextDisplay("### Route"),
                discord.ui.TextDisplay(route_text),
                accessory=discord.ui.Button(label=badge_label, style=badge_style, disabled=True),
            ),
            discord.ui.Section(
                discord.ui.TextDisplay("### Live Stage"),
                discord.ui.TextDisplay(f"`{phase_text}`\n{phase_detail}"),
                accessory=discord.ui.Button(label=f"{progress_percent}%", style=discord.ButtonStyle.primary, disabled=True),
            ),
            discord.ui.TextDisplay(transit_summary),
            discord.ui.TextDisplay(f"### Transit Channel\n`#{status_channel_name}`"),
            discord.ui.TextDisplay(f"### Active Lanes\n{lane_text}"),
        ]
        children.extend(
            [
                discord.ui.Separator(),
                discord.ui.TextDisplay(f"### Telemetry\n{body_text}"),
                discord.ui.TextDisplay(footer_text),
            ]
        )
        self.add_item(discord.ui.Container(*children, accent_color=accent))


async def sync_backup_status_message(guild_id: int) -> None:
    job = BACKUP_LOAD_JOBS.get(guild_id)
    if not job:
        return
    channel = await resolve_backup_status_channel(guild_id, job)
    if channel is None:
        return

    view = BackupLoadStatusCardView(bot.user, job)
    message_id = job.get("status_message_id")
    try:
        if message_id:
            message = await channel.fetch_message(message_id)
            await message.edit(content=None, embed=None, view=view)
        else:
            message = await channel.send(view=view)
            job["status_message_id"] = message.id
    except discord.NotFound:
        job["status_message_id"] = None
        try:
            message = await channel.send(view=view)
            job["status_message_id"] = message.id
        except (discord.Forbidden, discord.HTTPException):
            return
    except (discord.Forbidden, discord.HTTPException):
        return


class BackupLoadActiveView(discord.ui.LayoutView):
    def __init__(
        self,
        bot_user: discord.ClientUser | None,
        guild_id: int,
        backup_id: str,
        source_name: str,
        target_name: str,
        selected_actions: set[str],
        preview: dict[str, int],
    ) -> None:
        super().__init__(timeout=1800)
        self.bot_user = bot_user
        self.guild_id = guild_id
        self.backup_id = backup_id
        self.source_name = source_name
        self.target_name = target_name
        self.selected_actions = set(selected_actions)
        self.preview = preview
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()
        hero = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        active_lanes = "\n".join(build_backup_lane_lines(self.selected_actions))
        delete_lines = []
        if self.preview.get("deleted_roles"):
            delete_lines.append(f"- `{self.preview['deleted_roles']}` roles queued for deletion")
        if self.preview.get("deleted_channels"):
            delete_lines.append(f"- `{self.preview['deleted_channels']}` channels queued for deletion")
        if not delete_lines:
            delete_lines.append("- No destructive delete lanes armed")

        build_lines = []
        if self.preview.get("created_roles"):
            build_lines.append(f"- `{self.preview['created_roles']}` roles queued for creation")
        if self.preview.get("updated_roles"):
            build_lines.append(f"- `{self.preview['updated_roles']}` roles queued for update")
        if self.preview.get("created_categories"):
            build_lines.append(f"- `{self.preview['created_categories']}` categories queued for creation")
        if self.preview.get("created_channels"):
            build_lines.append(f"- `{self.preview['created_channels']}` channels queued for creation")
        if self.preview.get("updated_channels"):
            build_lines.append(f"- `{self.preview['updated_channels']}` channels queued for update")
        if self.preview.get("updated_settings"):
            build_lines.append("- Guild settings queued for sync")
        if not build_lines:
            build_lines.append("- No build work armed")

        self.add_item(
            discord.ui.Container(
                discord.ui.Section(
                    discord.ui.TextDisplay("## <> Applying Backup"),
                    discord.ui.TextDisplay("Stage 3 of 3. The restore plan is live. CLINX pushes the public transit feed into `#clinx-restoring` and keeps it updated automatically."),
                    accessory=hero,
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Route"),
                    discord.ui.TextDisplay(f"`{self.source_name}` -> `{self.target_name}`"),
                    accessory=discord.ui.Button(label="Live", style=discord.ButtonStyle.primary, disabled=True),
                ),
                discord.ui.TextDisplay(f"### Active Lanes\n{active_lanes}"),
                discord.ui.TextDisplay(f"### Deletes In Flight\n{chr(10).join(delete_lines)}"),
                discord.ui.TextDisplay(f"### Build In Flight\n{chr(10).join(build_lines)}"),
                discord.ui.TextDisplay("Use **View Status** for a private snapshot or `/backup status` to re-post the public transit card. CLINX uses `#clinx-restoring` as the restore status lane."),
            )
        )
        self.add_item(discord.ui.ActionRow(self._make_status_button()))

    def _make_status_button(self) -> discord.ui.Button:
        button = discord.ui.Button(label="View Status", style=discord.ButtonStyle.primary)

        async def callback(interaction: discord.Interaction) -> None:
            job = BACKUP_LOAD_JOBS.get(self.guild_id)
            if not job:
                await interaction.response.send_message(
                    embed=make_embed("Backup Status", "No backup load job found for this server.", EMBED_INFO, interaction),
                    ephemeral=True,
                )
                return
            await interaction.response.send_message(
                embed=make_embed("Backup Status", build_backup_load_status_description(job), EMBED_INFO, interaction),
                ephemeral=True,
            )

        button.callback = callback
        return button


class BackupApprovalQueuedView(discord.ui.LayoutView):
    def __init__(self, bot_user: discord.ClientUser | None) -> None:
        super().__init__(timeout=None)
        hero = (
            discord.ui.Thumbnail(bot_user.display_avatar.url)
            if bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        self.add_item(
            discord.ui.Container(
                discord.ui.Section(
                    discord.ui.TextDisplay("## <> Approval Requested"),
                    discord.ui.TextDisplay("CLINX posted a public owner-approval card in this channel. The restore will start only after the actual server owner approves it."),
                    accessory=hero,
                ),
                discord.ui.Separator(),
                discord.ui.TextDisplay("### Next Step\n- Wait for the owner approval card.\n- After approval, the public backup status card will begin updating automatically."),
            )
        )


class BackupLoadPlannerView(discord.ui.LayoutView):
    def __init__(
        self,
        author_id: int,
        backup_id: str,
        source_name: str,
        snapshot: dict[str, Any],
        target: discord.Guild,
        bot_user: discord.ClientUser | None,
    ) -> None:
        super().__init__(timeout=300)
        self.author_id = author_id
        self.backup_id = backup_id
        self.source_name = source_name
        self.snapshot = snapshot
        self.target = target
        self.bot_user = bot_user
        self.author_is_developer = author_id in DEVELOPER_USER_IDS
        self.selected_actions: set[str] = {"load_roles", "load_channels", "load_settings", "delete_roles", "delete_channels"}
        self.review_mode = False
        self.detail_mode = False
        self.rebuild()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the command user can use these controls.", ephemeral=True)
            return False
        return True

    def normalize_actions(self) -> None:
        if self.author_is_developer:
            return
        if "load_roles" not in self.selected_actions or "load_channels" not in self.selected_actions:
            self.selected_actions.discard("delete_roles")
            self.selected_actions.discard("delete_channels")

    def rebuild(self) -> None:
        self.normalize_actions()
        self.clear_items()
        self.add_item(self._build_container())
        if self.review_mode:
            self.add_item(
                discord.ui.ActionRow(
                    self._make_apply_button(),
                    self._make_back_button(),
                    self._make_cancel_button(),
                )
            )
            self.add_item(discord.ui.ActionRow(self._make_detail_button()))
            return

        self.add_item(
            discord.ui.ActionRow(
                self._make_toggle_button("load_roles"),
                self._make_toggle_button("load_channels"),
                self._make_toggle_button("load_settings"),
            )
        )
        self.add_item(
            discord.ui.ActionRow(
                self._make_toggle_button("delete_roles"),
                self._make_toggle_button("delete_channels"),
                self._make_detail_button(),
            )
        )
        self.add_item(
            discord.ui.ActionRow(
                self._make_continue_button(),
                self._make_cancel_button(),
            )
        )

    def _build_container(self) -> discord.ui.Container:
        preview = build_backup_plan_preview(self.snapshot, self.target, self.selected_actions)
        hero = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        stage_badge = discord.ui.Button(
            label="Stage 2 / 3" if self.review_mode else "Stage 1 / 3",
            style=discord.ButtonStyle.secondary,
            disabled=True,
        )
        destructive = {"delete_roles", "delete_channels"} & self.selected_actions
        route_badge = discord.ui.Button(
            label="Destructive" if destructive else "Merge + Rebuild",
            style=discord.ButtonStyle.danger if destructive else discord.ButtonStyle.primary,
            disabled=True,
        )
        lane_lines = build_backup_lane_lines(self.selected_actions)
        snapshot_panel = (
            f"`{len(self.snapshot.get('roles', []))}` roles\n"
            f"`{len(self.snapshot.get('categories', []))}` categories\n"
            f"`{len(self.snapshot.get('channels', []))}` channels"
        )

        children: list[discord.ui.Item[Any]] = [
            discord.ui.Section(
                discord.ui.TextDisplay("## <> Backup Load Planner"),
                discord.ui.TextDisplay(
                    "Stage the restore lanes first. CLINX reveals the delete/build impact after you continue."
                    if not self.review_mode
                    else "Review the exact delete/build impact, verify the guard rails, then launch the restore lane."
                ),
                accessory=hero,
            ),
            discord.ui.Separator(),
            discord.ui.Section(
                discord.ui.TextDisplay("### Backup Vault"),
                discord.ui.TextDisplay(f"`{self.backup_id}`"),
                accessory=stage_badge,
            ),
            discord.ui.Section(
                discord.ui.TextDisplay("### Route"),
                discord.ui.TextDisplay(f"`{self.source_name}` -> `{self.target.name}`"),
                accessory=route_badge,
            ),
            discord.ui.Section(
                discord.ui.TextDisplay("### Snapshot Payload"),
                discord.ui.TextDisplay(snapshot_panel),
                accessory=discord.ui.Button(label="Vault", style=discord.ButtonStyle.primary, disabled=True),
            ),
            discord.ui.TextDisplay(f"### Restore Lanes\n{chr(10).join(lane_lines)}"),
            discord.ui.TextDisplay(
                "### Safety Locks\n"
                "- Delete Roles and Delete Channels stay armed only when both Load Roles and Load Channels are active.\n"
                "- Turning either rebuild lane off drops both destructive lanes.\n"
                "- Delete-only runs are blocked."
            ),
        ]

        if self.review_mode:
            delete_lines = []
            if preview["deleted_roles"]:
                delete_lines.append(f"- `{preview['deleted_roles']}` roles will be deleted")
            if preview["deleted_channels"]:
                delete_lines.append(f"- `{preview['deleted_channels']}` channels will be deleted")
            if not delete_lines:
                delete_lines.append("- No destructive deletes are armed")

            build_lines = []
            if preview["created_roles"]:
                build_lines.append(f"- `{preview['created_roles']}` roles will be created")
            if preview["updated_roles"]:
                build_lines.append(f"- `{preview['updated_roles']}` roles will be updated")
            if preview["created_categories"]:
                build_lines.append(f"- `{preview['created_categories']}` categories will be created")
            if preview["created_channels"]:
                build_lines.append(f"- `{preview['created_channels']}` channels will be created")
            if preview["updated_channels"]:
                build_lines.append(f"- `{preview['updated_channels']}` channels will be updated")
            if preview["updated_settings"]:
                build_lines.append("- Guild settings will be synced")
            if not build_lines:
                build_lines.append("- No build work armed")

            children.extend(
                [
                    discord.ui.Separator(),
                    discord.ui.TextDisplay(f"### Projected Deletes\n{chr(10).join(delete_lines)}"),
                    discord.ui.TextDisplay(f"### Projected Build\n{chr(10).join(build_lines)}"),
                ]
            )

        if self.detail_mode:
            children.extend(
                [
                    discord.ui.Separator(),
                    discord.ui.TextDisplay(f"### Detail View\n{build_snapshot_detail_lines(self.snapshot, self.target)}"),
                ]
            )

        return discord.ui.Container(*children, accent_color=EMBED_INFO)

    def _make_toggle_button(self, action: str) -> discord.ui.Button:
        selected = action in self.selected_actions
        destructive = action.startswith("delete_")
        enabled = not (
            not self.author_is_developer
            and
            action in {"delete_roles", "delete_channels"}
            and ("load_roles" not in self.selected_actions or "load_channels" not in self.selected_actions)
        )
        style = discord.ButtonStyle.danger if destructive and selected else (
            discord.ButtonStyle.success if selected else discord.ButtonStyle.secondary
        )
        button = discord.ui.Button(label=LOAD_ACTION_LABELS[action], style=style, disabled=not enabled)

        async def callback(interaction: discord.Interaction) -> None:
            if not enabled:
                await interaction.response.send_message("Enable the matching rebuild lane first.", ephemeral=True)
                return
            if action in self.selected_actions:
                self.selected_actions.remove(action)
            else:
                self.selected_actions.add(action)
            self.review_mode = False
            self.rebuild()
            await interaction.response.edit_message(view=self)

        button.callback = callback
        return button

    def _make_detail_button(self) -> discord.ui.Button:
        button = discord.ui.Button(
            label="Hide Detail" if self.detail_mode else "View Detail",
            style=discord.ButtonStyle.secondary,
        )

        async def callback(interaction: discord.Interaction) -> None:
            self.detail_mode = not self.detail_mode
            self.rebuild()
            await interaction.response.edit_message(view=self)

        button.callback = callback
        return button

    def _make_continue_button(self) -> discord.ui.Button:
        has_rebuild_lane = any(action in self.selected_actions for action in ("load_roles", "load_channels", "load_settings"))
        if self.author_is_developer:
            has_rebuild_lane = bool(self.selected_actions)
        button = discord.ui.Button(label="Continue", style=discord.ButtonStyle.primary, disabled=not has_rebuild_lane)

        async def callback(interaction: discord.Interaction) -> None:
            self.review_mode = True
            self.rebuild()
            await interaction.response.edit_message(view=self)

        button.callback = callback
        return button

    def _make_back_button(self) -> discord.ui.Button:
        button = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary)

        async def callback(interaction: discord.Interaction) -> None:
            self.review_mode = False
            self.rebuild()
            await interaction.response.edit_message(view=self)

        button.callback = callback
        return button

    def _make_cancel_button(self) -> discord.ui.Button:
        button = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.danger)

        async def callback(interaction: discord.Interaction) -> None:
            await interaction.response.edit_message(
                embed=make_embed("Cancelled", "Backup load was cancelled.", EMBED_WARN, interaction),
                view=None,
            )
            self.stop()

        button.callback = callback
        return button

    def _make_apply_button(self) -> discord.ui.Button:
        button = discord.ui.Button(label="Apply Backup", style=discord.ButtonStyle.success)

        async def callback(interaction: discord.Interaction) -> None:
            await self.start_backup_load(interaction)

        button.callback = callback
        return button

    async def start_backup_load(self, interaction: discord.Interaction) -> None:
        existing_job = BACKUP_LOAD_JOBS.get(self.target.id)
        if existing_job and existing_job.get("status") == "running":
            await sync_backup_status_message(self.target.id)
            channel_name = existing_job.get("status_channel_name", "clinx-restoring")
            await interaction.response.send_message(
                embed=make_embed(
                    "Backup Busy",
                    f"A backup load is already running in this server. CLINX is streaming the live transit feed in `#{channel_name}`.",
                    EMBED_INFO,
                    interaction,
                ),
                ephemeral=True,
            )
            return

        preview = build_backup_plan_preview(self.snapshot, self.target, self.selected_actions)
        warnings = build_backup_hierarchy_warnings(self.snapshot, self.target, self.selected_actions)
        access_mode, _, message = require_clinx_access(
            interaction,
            "backup_load",
            selected_actions=set(self.selected_actions),
        )
        if access_mode == "deny":
            await interaction.response.send_message(embed=make_embed("Access Denied", message or "You cannot run this restore plan.", EMBED_ERR, interaction), ephemeral=True)
            return
        if access_mode == "approval":
            delete_lines, build_lines = build_preview_lines(preview)
            selected_actions_text = "\n".join(build_backup_lane_lines(self.selected_actions))
            projected_text = f"Deletes:\n{chr(10).join(delete_lines)}\n\nBuild:\n{chr(10).join(build_lines)}"

            async def executor(request: dict[str, Any]) -> None:
                BACKUP_LOAD_JOBS[self.target.id] = {
                    "status": "running",
                    "started_at": utc_now_iso(),
                    "backup_id": self.backup_id,
                    "source_name": self.source_name,
                    "target_name": self.target.name,
                    "selected_actions": sorted(self.selected_actions),
                    "preview": preview,
                    "phase": "Queued",
                    "phase_detail": "Owner approval completed. CLINX is opening the restore lane and priming the transit feed.",
                    "stats": {},
                    "warnings": warnings,
                    "status_channel_id": None,
                    "status_message_id": None,
                    "status_channel_name": "clinx-restoring",
                    "preserve_channel_id": None,
                    "task": None,
                    "approval_request_id": request["id"],
                }
                await sync_backup_status_message(self.target.id)
                BACKUP_LOAD_JOBS[self.target.id]["preserve_channel_id"] = BACKUP_LOAD_JOBS[self.target.id].get("status_channel_id")
                task = asyncio.create_task(
                    run_backup_load_job(
                        self.target.id,
                        self.snapshot,
                        self.target,
                        backup_id=self.backup_id,
                        source_name=self.source_name,
                        selected_actions=set(self.selected_actions),
                    )
                )
                BACKUP_LOAD_JOBS[self.target.id]["task"] = task
                request["result_text"] = "Backup load approved. The public restore status card is now live in this channel."
                await sync_safety_request_message(request["guild_id"])

            await interaction.response.edit_message(view=BackupApprovalQueuedView(self.bot_user))
            created = await create_safety_request(
                interaction,
                command_name="backup load",
                subtitle="An untrusted admin wants CLINX to apply a backup load plan in this server.",
                risk_label="Destructive" if {"delete_roles", "delete_channels"} & self.selected_actions else "Non-Destructive",
                route_text=f"`{self.source_name}` -> `{self.target.name}`",
                selected_actions_text=selected_actions_text,
                projected_text=projected_text,
                executor=executor,
            )
            if created:
                self.stop()
            return

        BACKUP_LOAD_JOBS[self.target.id] = {
            "status": "running",
            "started_at": utc_now_iso(),
            "backup_id": self.backup_id,
            "source_name": self.source_name,
            "target_name": self.target.name,
            "selected_actions": sorted(self.selected_actions),
            "preview": preview,
            "phase": "Queued",
            "phase_detail": "CLINX is opening the restore lane and priming the public transit feed.",
            "stats": {},
            "warnings": warnings,
            "status_channel_id": None,
            "status_message_id": None,
            "status_channel_name": "clinx-restoring",
            "preserve_channel_id": None,
            "task": None,
            "approval_request_id": None,
        }
        await sync_backup_status_message(self.target.id)
        BACKUP_LOAD_JOBS[self.target.id]["preserve_channel_id"] = BACKUP_LOAD_JOBS[self.target.id].get("status_channel_id")
        task = asyncio.create_task(
            run_backup_load_job(
                self.target.id,
                self.snapshot,
                self.target,
                backup_id=self.backup_id,
                source_name=self.source_name,
                selected_actions=set(self.selected_actions),
            )
        )
        BACKUP_LOAD_JOBS[self.target.id]["task"] = task
        await interaction.response.edit_message(
            view=BackupLoadActiveView(
                self.bot_user,
                self.target.id,
                self.backup_id,
                self.source_name,
                self.target.name,
                self.selected_actions,
                preview,
            )
        )
        self.stop()

    async def on_timeout(self) -> None:
        for child in self.walk_children():
            if hasattr(child, "disabled"):
                child.disabled = True


COMMAND_LIBRARY_LANES: tuple[CommandLibraryLane, ...] = (
    CommandLibraryLane(
        key="backup",
        label="Backup",
        emoji="<>",
        accent=0x4F8CFF,
        blurb="Snapshot, load, track, and manage recovery jobs.",
        entries=(
            CommandLibraryEntry("/backup create", "Create a recovery snapshot of the current server.", "Captures channels, roles, overwrites, and server settings into a reusable backup ID.", "Private"),
            CommandLibraryEntry("/backup load", "Load a backup with lane selection and live status.", "Opens the restore planner so you can choose what to rebuild before CLINX touches the target server.", "Private"),
            CommandLibraryEntry("/backup list", "List only the backups owned by your account.", "Shows your stored backup codes with creation time so only your account can load or delete them.", "Private"),
            CommandLibraryEntry("/backup delete", "Remove a stored backup ID.", "Deletes a backup record from CLINX storage so it can no longer be loaded.", "Private"),
            CommandLibraryEntry("/backup interval on", "Arm automatic backups for this server.", "Locks in a fixed seal interval, keeps the latest configured number of interval backups, and stores them in the selected owner's vault.", "Private"),
            CommandLibraryEntry("/backup interval off", "Disable automatic backups for this server.", "Stops CLINX from sealing new interval backups while preserving the saved schedule and latest run data.", "Private"),
            CommandLibraryEntry("/backup interval show", "Inspect the interval lane for this server.", "Shows the active schedule, next seal window, last automatic backup, vault owner, and storage backend.", "Private"),
            CommandLibraryEntry("/backup status", "Inspect the current load job.", "Posts or refreshes the public live status card for the active restore job in this server.", "Public"),
            CommandLibraryEntry("/backup cancel", "Cancel the current backup load.", "Stops the active restore task for this server if one is running and updates the public live card.", "Public"),
        ),
    ),
    CommandLibraryLane(
        key="recovery",
        label="Recovery",
        emoji="<>",
        accent=0x39C6A5,
        blurb="High-impact recovery, rebuild, and cleanup operations.",
        entries=(
            CommandLibraryEntry("/restore_missing", "Recreate only the missing structure from a source server.", "Creates categories and channels that do not exist yet without wiping the target.", "Public"),
            CommandLibraryEntry("/cleantoday", "Owner-only: delete channels created today.", "Useful for nuked test runs and bad imports. Dry-run unless `confirm=true` is supplied.", "Private"),
            CommandLibraryEntry("/cleanempty", "Owner-only: delete text channels with no user messages.", "Scans text channels, deletes ones with no non-bot messages, and can remove categories whose entire channel tree is empty.", "Private"),
            CommandLibraryEntry("/import guild", "Import a full server snapshot JSON file.", "Runs a structured import job from an exported guild snapshot file.", "Private"),
            CommandLibraryEntry("/import status", "Check the import job state.", "Returns running, finished, or failed status for the active import in this server.", "Private"),
            CommandLibraryEntry("/import cancel", "Cancel the active import job.", "Stops the running import task if a guild import is in progress.", "Private"),
        ),
    ),
    CommandLibraryLane(
        key="export",
        label="Export",
        emoji="<>",
        accent=0xF2B94B,
        blurb="Pull clean JSON or CSV data out of the guild when you need a snapshot.",
        entries=(
            CommandLibraryEntry("/export guild", "Export the full guild snapshot as JSON.", "Captures the same snapshot structure CLINX uses for restore jobs.", "Private"),
            CommandLibraryEntry("/export channels", "Export all channels as JSON or CSV.", "Useful for archiving channel names, types, topics, and positions.", "Private"),
            CommandLibraryEntry("/export roles", "Export all roles as JSON or CSV.", "Dumps role metadata including permissions, color, and display state.", "Private"),
            CommandLibraryEntry("/export channel", "Export one channel as JSON.", "Returns the serialized payload for a specific channel by ID.", "Private"),
            CommandLibraryEntry("/export role", "Export one role as JSON.", "Returns the serialized payload for a specific role by ID.", "Private"),
            CommandLibraryEntry("/export message", "Export one message as JSON.", "Captures a single message payload for audit or migration work.", "Private"),
            CommandLibraryEntry("/export reactions", "Export message reactions as JSON or CSV.", "Serializes the reaction breakdown for a specific message.", "Private"),
        ),
    ),
    CommandLibraryLane(
        key="surfaces",
        label="Surfaces",
        emoji="<>",
        accent=0x9A7CFF,
        blurb="Operator-facing panels and public bot surfaces.",
        entries=(
            CommandLibraryEntry("/help", "Get a list of commands or more information about a specific command.", "Browse command lanes, page through the catalog, and inspect each command in a single surface.", "Public"),
            CommandLibraryEntry("/premium", "Show the active premium card for this server.", "Displays the current CLINX premium tier, vault cap, billing state, and retention window in a compact status card.", "Public"),
            CommandLibraryEntry("/invite", "Get the bot invite link.", "Returns the OAuth invite for CLINX with bot and slash command scopes.", "Public"),
            CommandLibraryEntry("/leave", "Make CLINX leave the current server.", "Tells the bot to exit the server immediately after confirmation.", "Private"),
        ),
    ),
    CommandLibraryLane(
        key="safety",
        label="Safety",
        emoji="<>",
        accent=0xF7A531,
        blurb="Trust lists, owner approval, and protected command governance.",
        entries=(
            CommandLibraryEntry("/safety grant", "Trust one admin for protected CLINX actions.", "Allows the actual server owner to mark an admin as trusted so protected actions skip owner approval.", "Private"),
            CommandLibraryEntry("/safety revoke", "Remove trust from a CLINX admin.", "Revokes trusted-admin status so future protected actions require owner approval again.", "Private"),
            CommandLibraryEntry("/safety list", "List trusted CLINX admins for this server.", "Shows the current trusted CLINX operators configured by the actual server owner.", "Private"),
        ),
    ),
)

COMMAND_LIBRARY_BY_KEY = {lane.key: lane for lane in COMMAND_LIBRARY_LANES}
COMMAND_LIBRARY_BY_PATH = {entry.path: entry for lane in COMMAND_LIBRARY_LANES for entry in lane.entries}
COMMAND_LIBRARY_PAGE_SIZE = 6


def build_invite_url(app_id: int | None) -> str:
    resolved_id = app_id or "YOUR_APP_ID"
    return f"https://discord.com/oauth2/authorize?client_id={resolved_id}&permissions=8&integration_type=0&scope=bot+applications.commands"


def emphasize_command_refs(text: str) -> str:
    return re.sub(
        r"(?<![`\\w])(/[a-z0-9_]+(?: [a-z0-9_]+)*)",
        lambda match: f"**`{match.group(1)}`**",
        text,
    )


def format_command_library_page(lane: CommandLibraryLane, page: int) -> tuple[str, int]:
    total_pages = max(1, (len(lane.entries) + COMMAND_LIBRARY_PAGE_SIZE - 1) // COMMAND_LIBRARY_PAGE_SIZE)
    safe_page = max(0, min(page, total_pages - 1))
    start = safe_page * COMMAND_LIBRARY_PAGE_SIZE
    entries = lane.entries[start : start + COMMAND_LIBRARY_PAGE_SIZE]
    lines = [f"- **`/{entry.path.lstrip('/')}`** - {entry.summary}" for entry in entries]
    return "\n".join(lines), total_pages


def format_command_library_detail(entry: CommandLibraryEntry | None) -> str:
    if entry is None:
        return (
            "Pick a command from the selector below to inspect what it does, how visible it is, "
            "and which alias CLINX accepts."
        )

    detail_lines = [
        f"**`/{entry.path.lstrip('/')}`**",
        emphasize_command_refs(entry.detail),
        "",
        f"**Visibility**: {entry.visibility}",
    ]
    if entry.aliases:
        aliases = ", ".join(f"**`{alias}`**" for alias in entry.aliases)
        detail_lines.append(f"**Aliases**: {aliases}")
    return "\n".join(detail_lines)

class CommandLibraryView(discord.ui.LayoutView):
    def __init__(self, bot_user: discord.ClientUser | None) -> None:
        super().__init__(timeout=600)
        self.bot_user = bot_user
        self.lane_key = COMMAND_LIBRARY_LANES[0].key
        self.page = 0
        self.selected_path: str | None = None
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()

        lane = COMMAND_LIBRARY_BY_KEY[self.lane_key]
        page_text, total_pages = format_command_library_page(lane, self.page)
        self.page = max(0, min(self.page, total_pages - 1))
        entry = COMMAND_LIBRARY_BY_PATH.get(self.selected_path) if self.selected_path else None
        total_commands = sum(len(group.entries) for group in COMMAND_LIBRARY_LANES)

        thumbnail = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        overview_badge = discord.ui.Button(
            label=f"{lane.label} {self.page + 1}/{total_pages}",
            style=discord.ButtonStyle.secondary,
            disabled=True,
        )
        detail_badge = discord.ui.Button(
            label="Focused" if entry else "Overview",
            style=discord.ButtonStyle.secondary,
            disabled=True,
        )

        container = discord.ui.Container(
            discord.ui.Section(
                discord.ui.TextDisplay("## <> Command Library"),
                discord.ui.TextDisplay(
                    f"Explore `{total_commands}` live commands across backup, recovery, export, and operator surfaces."
                ),
                accessory=thumbnail,
            ),
            discord.ui.Separator(),
            discord.ui.Section(
                discord.ui.TextDisplay(f"### {lane.label} Lane"),
                discord.ui.TextDisplay(lane.blurb),
                accessory=overview_badge,
            ),
            discord.ui.TextDisplay(page_text),
            discord.ui.Separator(),
            discord.ui.Section(
                discord.ui.TextDisplay("### Command Detail"),
                discord.ui.TextDisplay("Drill into one command without leaving the card."),
                accessory=detail_badge,
            ),
            discord.ui.TextDisplay(format_command_library_detail(entry)),
            accent_color=lane.accent,
        )
        self.add_item(container)

        lane_row = discord.ui.ActionRow(
            *[self._make_lane_button(group) for group in COMMAND_LIBRARY_LANES]
        )
        self.add_item(lane_row)
        self.add_item(discord.ui.ActionRow(self._make_detail_select(lane)))
        self.add_item(
            discord.ui.ActionRow(
                self._make_nav_button("Back", -1, self.page <= 0),
                self._make_nav_button("Next", 1, self.page >= total_pages - 1),
                discord.ui.Button(label="Invite", url=build_invite_url(self.bot_user.id if self.bot_user else None)),
                discord.ui.Button(label="Support", url=SUPPORT_URL),
            )
        )

    def _make_lane_button(self, lane: CommandLibraryLane) -> discord.ui.Button:
        style = discord.ButtonStyle.primary if lane.key == self.lane_key else discord.ButtonStyle.secondary
        button = discord.ui.Button(label=lane.label, style=style)

        async def callback(interaction: discord.Interaction) -> None:
            self.lane_key = lane.key
            self.page = 0
            self.selected_path = None
            self.rebuild()
            await interaction.response.edit_message(view=self)

        button.callback = callback
        return button

    def _make_detail_select(self, lane: CommandLibraryLane) -> discord.ui.Select:
        options = [
            discord.SelectOption(
                label=f"/{entry.path.lstrip('/')}",
                description=entry.summary[:100],
                value=entry.path,
                default=entry.path == self.selected_path,
            )
            for entry in lane.entries
        ]
        select = discord.ui.Select(
            custom_id=f"clinx-help-select:{lane.key}",
            placeholder="Select a command for details...",
            options=options,
        )

        async def callback(interaction: discord.Interaction) -> None:
            self.selected_path = select.values[0]
            self.rebuild()
            await interaction.response.edit_message(view=self)

        select.callback = callback
        return select

    def _make_nav_button(self, label: str, delta: int, disabled: bool) -> discord.ui.Button:
        button = discord.ui.Button(label=label, style=discord.ButtonStyle.secondary, disabled=disabled)

        async def callback(interaction: discord.Interaction) -> None:
            lane = COMMAND_LIBRARY_BY_KEY[self.lane_key]
            _, total_pages = format_command_library_page(lane, self.page)
            self.page = max(0, min(self.page + delta, total_pages - 1))
            self.selected_path = None
            self.rebuild()
            await interaction.response.edit_message(view=self)

        button.callback = callback
        return button


async def run_backup_interval_cycle(bot_instance: commands.Bot) -> None:
    store = load_safety_store()
    guilds = store.get("guilds", {})
    now = datetime.now(timezone.utc)
    due_runs: list[tuple[int, dict[str, Any]]] = []
    changed = False

    for guild_key in list(guilds.keys()):
        try:
            guild_id = int(guild_key)
        except ValueError:
            continue
        bucket = get_guild_safety_bucket(store, guild_id)
        config = bucket.get("backup_interval")
        if not isinstance(config, dict) or not config.get("enabled"):
            continue
        interval_hours = int(config.get("interval_hours") or 0)
        if interval_hours not in INTERVAL_PRESET_HOURS:
            config["enabled"] = False
            config["last_error"] = "Invalid interval preset configured."
            bucket["backup_interval"] = config
            changed = True
            continue
        next_run = parse_iso_timestamp(config.get("next_run_at"))
        if next_run is None:
            config["next_run_at"] = (now + timedelta(hours=interval_hours)).isoformat()
            bucket["backup_interval"] = config
            changed = True
            continue
        if next_run <= now:
            config["next_run_at"] = (now + timedelta(hours=interval_hours)).isoformat()
            config["last_run_at"] = utc_now_iso()
            bucket["backup_interval"] = config
            due_runs.append((guild_id, dict(config)))
            changed = True

    if changed:
        save_safety_store(store)

    for guild_id, config in due_runs:
        guild = bot_instance.get_guild(guild_id)
        if guild is None:
            update_backup_interval_runtime(guild_id, last_error="CLINX could not find this server in cache during the interval run.")
            continue
        owner_user_id = int(config.get("owner_user_id") or guild.owner_id)
        owner_display_name = str(config.get("owner_display_name") or guild.owner or f"Guild Owner {owner_user_id}")
        backup_limit, _ = get_backup_limit_for_guild(guild.id)
        keep_count = max(1, min(int(config.get("keep_count") or 1), backup_limit))
        can_rotate, _ = await trim_interval_backups_for_owner(
            owner_user_id,
            guild_id=guild.id,
            keep_count=keep_count,
            backup_limit=backup_limit,
        )
        if not can_rotate:
            update_backup_interval_runtime(
                guild.id,
                keep_count=keep_count,
                last_error="Vault is full. Delete older backups or upgrade the vault tier before the next automatic seal.",
            )
            continue
        try:
            record = await create_backup_record_for_owner(
                guild,
                owner_user_id=owner_user_id,
                owner_display_name=owner_display_name,
                include_assets=True,
                auto_backup={
                    "type": "interval",
                    "guild_id": str(guild.id),
                    "interval_hours": int(config.get("interval_hours") or 0),
                    "keep_count": keep_count,
                    "sealed_at": utc_now_iso(),
                },
            )
        except Exception as exc:
            update_backup_interval_runtime(guild.id, last_error=str(exc)[:180])
            continue
        update_backup_interval_runtime(
            guild.id,
            keep_count=keep_count,
            last_success_at=utc_now_iso(),
            last_backup_id=record["id"],
            last_error=None,
        )

intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True


class ClinxBot(commands.Bot):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._groups_added = False
        self._startup_synced = False
        self._backup_interval_task: asyncio.Task[Any] | None = None

    async def setup_hook(self) -> None:
        if not self._groups_added:
            self.tree.add_command(backup_group)
            self.tree.add_command(export_group)
            self.tree.add_command(import_group)
            self.tree.add_command(safety_group)
            self.tree.add_command(dev_group)
            self._groups_added = True

        if not self._startup_synced:
            synced = await self.tree.sync()
            self._startup_synced = True
            print(f"Synced {len(synced)} slash commands")

        if self._backup_interval_task is None:
            self._backup_interval_task = asyncio.create_task(self._backup_interval_worker(), name="clinx-backup-interval")

    async def _backup_interval_worker(self) -> None:
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                await run_backup_interval_cycle(self)
            except Exception as exc:
                print(f"Backup interval worker error: {exc}")
            await asyncio.sleep(60)


bot = ClinxBot(command_prefix=commands.when_mentioned_or("^^^"), intents=intents)

backup_group = app_commands.Group(name="backup", description="Backup and restore commands")
backup_group = app_commands.allowed_installs(guilds=True, users=False)(backup_group)
backup_group = app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)(backup_group)
backup_interval_group = app_commands.Group(name="interval", description="Automatic backup interval controls", parent=backup_group)
export_group = app_commands.Group(name="export", description="Export server objects")
export_group = app_commands.allowed_installs(guilds=True, users=False)(export_group)
export_group = app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)(export_group)
import_group = app_commands.Group(name="import", description="Import server objects")
import_group = app_commands.allowed_installs(guilds=True, users=False)(import_group)
import_group = app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)(import_group)
safety_group = app_commands.Group(name="safety", description="CLINX trust and approval controls")
safety_group = app_commands.allowed_installs(guilds=True, users=False)(safety_group)
safety_group = app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)(safety_group)
dev_group = app_commands.Group(name="dev", description="Developer-only CLINX controls")
dev_group = app_commands.allowed_installs(guilds=False, users=True)(dev_group)
dev_group = app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)(dev_group)


@bot.event
async def on_ready() -> None:
    await bot.change_presence(
        activity=discord.Activity(type=discord.ActivityType.listening, name="/help")
    )
    print(f"Logged in as {bot.user} ({bot.user.id})")


@bot.event
async def on_guild_join(guild: discord.Guild) -> None:
    warnings = build_source_hierarchy_warnings(guild)
    if not warnings:
        return
    channel = resolve_notice_channel(guild)
    if channel is None:
        return
    try:
        await channel.send(
            view=RoleSafetyWarningCardView(
                bot.user,
                "CLINX Role Setup Needed",
                "CLINX joined this server, but its role is not positioned safely for full role management.",
                warnings,
            )
        )
    except (discord.Forbidden, discord.HTTPException):
        return


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
    message = str(getattr(error, "original", error))
    embed = make_embed("Command Error", message[:1800], EMBED_ERR, interaction)
    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


@backup_group.command(name="create", description="Create a backup snapshot and return load ID")
@app_commands.describe(source_guild_id="Guild ID to backup (optional)")
@app_commands.default_permissions(administrator=True)
async def backup_create(interaction: discord.Interaction, source_guild_id: int | None = None) -> None:
    access_mode, _, message = require_clinx_access(interaction, "backup_create")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot create backups in this server.")
        return
    await interaction.response.defer(ephemeral=True, thinking=True)

    source = interaction.guild if source_guild_id is None else bot.get_guild(source_guild_id)
    if source is None:
        await interaction.followup.send(embed=make_embed("Error", "Source guild not found.", EMBED_ERR), ephemeral=True)
        return

    if not is_developer_user(interaction.user):
        existing_entries = await list_user_backup_entries_async(interaction.user.id)
        vault_policy = get_backup_vault_policy_for_guild(source.id)
        backup_limit = int(vault_policy.get("creation_limit") or FREE_BACKUP_LIMIT)
        plan_label = str(vault_policy.get("badge_label") or "Free")
        if len(existing_entries) >= backup_limit:
            await interaction.followup.send(
                embed=make_embed(
                    "Backup Limit Reached",
                    (
                        f"You already use `{len(existing_entries)}/{backup_limit}` backup slots.\n"
                        f"Vault tier: `{plan_label}`\n"
                        f"{vault_policy.get('status_text')}\n"
                        "Delete older backups, renew premium, or move back under the free cap before creating another snapshot."
                    ),
                    EMBED_WARN,
                    interaction,
                ),
                ephemeral=True,
            )
            return

    source_warnings = build_source_hierarchy_warnings(source)
    record = await create_backup_record_for_owner(
        source,
        owner_user_id=interaction.user.id,
        owner_display_name=str(interaction.user),
        include_assets=True,
    )
    snapshot = record["snapshot"]

    await interaction.followup.send(
        view=BackupCreatedCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            record["id"],
            source,
            snapshot,
        ),
        ephemeral=True,
    )
    if source_warnings:
        await interaction.followup.send(
            view=RoleSafetyWarningCardView(
                interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
                "Source Role Safety Audit",
                "CLINX detected a hierarchy or permission issue in the source server while building this backup.",
                source_warnings,
            ),
            ephemeral=True,
        )


@backup_group.command(name="load", description="Load backup by ID with action selection")
@app_commands.describe(load_id="Backup load ID", target_guild_id="Target guild ID (optional)")
@app_commands.autocomplete(load_id=backup_id_autocomplete)
@app_commands.default_permissions(administrator=True)
async def backup_load(interaction: discord.Interaction, load_id: str, target_guild_id: int | None = None) -> None:
    access_mode, _, message = require_clinx_access(interaction, "backup_load")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot open the backup planner in this server.")
        return
    record = await get_user_backup_record_async(interaction.user.id, load_id)
    if record is None:
        await interaction.response.send_message(embed=make_embed("Invalid Load ID", "No backup found with that ID in your vault.", EMBED_ERR), ephemeral=True)
        return

    target = interaction.guild if target_guild_id is None else bot.get_guild(target_guild_id)
    if target is None:
        await interaction.response.send_message(embed=make_embed("Error", "Target guild not found.", EMBED_ERR), ephemeral=True)
        return

    view = BackupLoadPlannerView(
        interaction.user.id,
        load_id,
        record.get("source_guild_name", "Unknown Source"),
        record["snapshot"],
        target,
        interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
    )
    await interaction.response.send_message(view=view, ephemeral=True)
    target_warnings = build_backup_hierarchy_warnings(record["snapshot"], target, set(view.selected_actions))
    if target_warnings:
        await interaction.followup.send(
            view=RoleSafetyWarningCardView(
                interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
                "Target Role Safety Audit",
                "CLINX detected a hierarchy or permission issue in the target server before the restore started.",
                target_warnings,
            ),
            ephemeral=True,
        )


@backup_group.command(name="list", description="List saved backup IDs")
@app_commands.default_permissions(administrator=True)
async def backup_list(interaction: discord.Interaction) -> None:
    access_mode, _, message = require_clinx_access(interaction, "backup_list")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot inspect backup ownership in this server.")
        return
    entries = await list_user_backup_entries_async(interaction.user.id)

    guild = interaction.guild
    backup_limit = FREE_BACKUP_LIMIT
    plan_label = "Free"
    if guild is not None:
        backup_limit, plan_label = get_backup_limit_for_guild(guild.id)
    await interaction.response.send_message(
        view=BackupListCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            author_id=interaction.user.id,
            entries=entries,
            guild_id=guild.id if guild else None,
            backup_limit=backup_limit,
            plan_label=plan_label,
        ),
        ephemeral=True,
    )


@backup_group.command(name="delete", description="Delete a backup ID")
@app_commands.describe(load_id="Backup load ID")
@app_commands.autocomplete(load_id=backup_id_autocomplete)
@app_commands.default_permissions(administrator=True)
async def backup_delete(interaction: discord.Interaction, load_id: str) -> None:
    access_mode, _, message = require_clinx_access(interaction, "backup_delete")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot delete backups in this server.")
        return
    record = await get_user_backup_record_async(interaction.user.id, load_id)
    if record is None:
        await interaction.response.send_message(embed=make_embed("Error", "Load ID not found.", EMBED_ERR), ephemeral=True)
        return
    await delete_user_backup_async(interaction.user.id, load_id)
    await interaction.response.send_message(embed=make_embed("Deleted", f"Removed `{load_id}`.", EMBED_OK), ephemeral=True)


@backup_interval_group.command(name="on", description="Enable automatic interval backups for this server")
@app_commands.describe(interval="How often CLINX should seal a backup", keep="How many interval backups CLINX should keep in the vault")
@app_commands.choices(interval=INTERVAL_PRESET_CHOICES)
@app_commands.default_permissions(administrator=True)
async def backup_interval_on(
    interaction: discord.Interaction,
    interval: app_commands.Choice[int],
    keep: app_commands.Range[int, 1, 250] = 1,
) -> None:
    access_mode, _, message = require_clinx_access(interaction, "backup_create")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot configure backup intervals in this server.")
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run this in the server you want to schedule.", EMBED_ERR), ephemeral=True)
        return
    backup_limit, plan_label = get_backup_limit_for_guild(guild.id)
    keep_count = min(int(keep), backup_limit)
    config = set_backup_interval_config(
        guild,
        owner_user_id=interaction.user.id,
        owner_display_name=str(interaction.user),
        interval_hours=interval.value,
        keep_count=keep_count,
    )
    entries = await list_user_backup_entries_async(interaction.user.id)
    subtitle = (
        f"Automatic backups are now armed every `{format_interval_label(interval.value)}`. "
        f"CLINX will keep the latest `{keep_count}` interval snapshot(s) in your vault."
    )
    await interaction.response.send_message(
        view=BackupIntervalCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            title="Backup Interval Armed",
            subtitle=subtitle,
            guild=guild,
            config=config,
            backup_count=len(entries),
            backup_limit=backup_limit,
            plan_label=plan_label,
            color=EMBED_OK,
        ),
        ephemeral=True,
    )


@backup_interval_group.command(name="off", description="Disable automatic interval backups for this server")
@app_commands.default_permissions(administrator=True)
async def backup_interval_off(interaction: discord.Interaction) -> None:
    access_mode, _, message = require_clinx_access(interaction, "backup_create")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot configure backup intervals in this server.")
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run this in the server you want to update.", EMBED_ERR), ephemeral=True)
        return
    config = disable_backup_interval_config(guild)
    owner_user_id = int(config.get("owner_user_id") or interaction.user.id) if config else interaction.user.id
    entries = await list_user_backup_entries_async(owner_user_id)
    backup_limit, plan_label = get_backup_limit_for_guild(guild.id)
    await interaction.response.send_message(
        view=BackupIntervalCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            title="Backup Interval Offline",
            subtitle="Automatic backups are disabled for this server. CLINX will stop sealing new interval backups until you turn the lane on again.",
            guild=guild,
            config=config,
            backup_count=len(entries),
            backup_limit=backup_limit,
            plan_label=plan_label,
            color=EMBED_WARN,
        ),
        ephemeral=True,
    )


@backup_interval_group.command(name="show", description="Show the automatic backup interval for this server")
@app_commands.default_permissions(administrator=True)
async def backup_interval_show(interaction: discord.Interaction) -> None:
    access_mode, _, message = require_clinx_access(interaction, "backup_create")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot inspect backup intervals in this server.")
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run this in the server you want to inspect.", EMBED_ERR), ephemeral=True)
        return
    config = get_backup_interval_config(guild.id)
    owner_user_id = int(config.get("owner_user_id") or interaction.user.id) if isinstance(config, dict) else interaction.user.id
    entries = await list_user_backup_entries_async(owner_user_id)
    backup_limit, plan_label = get_backup_limit_for_guild(guild.id)
    await interaction.response.send_message(
        view=BackupIntervalCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            title="Backup Interval",
            subtitle="Inspect the automatic seal lane for this server, including the next run window and the vault CLINX writes into.",
            guild=guild,
            config=config,
            backup_count=len(entries),
            backup_limit=backup_limit,
            plan_label=plan_label,
            color=EMBED_INFO,
        ),
        ephemeral=True,
    )


@backup_group.command(name="status", description="Get current backup load status")
@app_commands.default_permissions(administrator=True)
async def backup_status(interaction: discord.Interaction) -> None:
    access_mode, _, message = require_clinx_access(interaction, "backup_list")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot inspect backup status in this server.")
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    job = BACKUP_LOAD_JOBS.get(guild.id)
    if not job:
        await interaction.response.send_message(embed=make_embed("Backup Status", "No backup load job found.", EMBED_INFO), ephemeral=False)
        return
    await sync_backup_status_message(guild.id)
    channel_name = job.get("status_channel_name", "clinx-restoring")
    await interaction.response.send_message(
        embed=make_embed(
            "Backup Status",
            f"CLINX refreshed the live transit card in `#{channel_name}`.\nUse **View Status** if you only need the private snapshot.",
            EMBED_INFO,
            interaction,
        ),
        ephemeral=True,
    )


@backup_group.command(name="cancel", description="Cancel running backup load")
@app_commands.default_permissions(administrator=True)
async def backup_cancel(interaction: discord.Interaction) -> None:
    access_mode, _, message = require_clinx_access(interaction, "backup_list")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot cancel backup jobs in this server.")
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    job = BACKUP_LOAD_JOBS.get(guild.id)
    if not job or job.get("status") != "running":
        await interaction.response.send_message(embed=make_embed("Backup", "No running backup load to cancel.", EMBED_INFO), ephemeral=False)
        return

    channel = interaction.channel
    if channel is not None and hasattr(channel, "send"):
        job["status_channel_id"] = channel.id
        job["status_message_id"] = None
    task = job.get("task")
    if task:
        task.cancel()
    await interaction.response.send_message(embed=make_embed("Backup", "Cancel requested. The public live status card will update when the task stops.", EMBED_INFO), ephemeral=False)


@bot.tree.command(name="restore_missing", description="Restore only missing categories/channels from source")
@app_commands.describe(source_guild_id="Source guild ID", target_guild_id="Target guild ID")
@app_commands.default_permissions(administrator=True)
@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
async def restore_missing(interaction: discord.Interaction, source_guild_id: int | None = None, target_guild_id: int | None = None) -> None:
    resolved_source = source_guild_id or resolve_default_backup_guild_id()
    source = bot.get_guild(resolved_source) if resolved_source else None
    target = interaction.guild if target_guild_id is None else bot.get_guild(target_guild_id)

    if source is None or target is None:
        await interaction.response.send_message(embed=make_embed("Error", "Could not resolve source or target guild.", EMBED_ERR), ephemeral=True)
        return

    access_mode, _, message = require_clinx_access(interaction, "restore_missing")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot run restore-missing in this server.")
        return

    snapshot = await build_guild_snapshot(source, include_assets=False)
    preview = build_backup_plan_preview(snapshot, target, {"load_channels"})
    if access_mode == "approval":
        delete_lines, build_lines = build_preview_lines(preview)

        async def executor(request: dict[str, Any]) -> None:
            try:
                stats = await apply_snapshot_to_guild(
                    snapshot,
                    target,
                    delete_roles=False,
                    delete_channels=False,
                    load_roles=False,
                    load_channels=True,
                    load_settings=False,
                    create_only_missing=True,
                )
                request["status"] = "completed"
                request["status_text"] = "Owner-approved restore-missing completed."
                request["result_text"] = (
                    f"Created categories: `{stats['created_categories']}`\n"
                    f"Created channels: `{stats['created_channels']}`"
                )
            except Exception as exc:
                request["status"] = "failed"
                request["status_text"] = "Owner-approved restore-missing failed."
                request["result_text"] = str(exc)
            await sync_safety_request_message(request["guild_id"])

        await create_safety_request(
            interaction,
            command_name="restore_missing",
            subtitle="An untrusted admin wants CLINX to restore only missing channel structure from a source server.",
            risk_label="Non-Destructive",
            route_text=f"`{source.name}` -> `{target.name}`",
            selected_actions_text="Load Channels: ON",
            projected_text=f"Deletes:\n{chr(10).join(delete_lines)}\n\nBuild:\n{chr(10).join(build_lines)}",
            executor=executor,
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)
    stats = await apply_snapshot_to_guild(
        snapshot,
        target,
        delete_roles=False,
        delete_channels=False,
        load_roles=False,
        load_channels=True,
        load_settings=False,
        create_only_missing=True,
    )

    await interaction.followup.send(
        embed=make_embed(
            "Restore Missing Complete",
            f"Created categories: `{stats['created_categories']}`\nCreated channels: `{stats['created_channels']}`",
            EMBED_OK,
        ),
        ephemeral=True,
    )


@bot.tree.command(name="cleantoday", description="Owner-only: delete channels created today (UTC)")
@app_commands.default_permissions(administrator=True)
@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
async def cleantoday(interaction: discord.Interaction, confirm: bool = False) -> None:
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run this command in a server.", EMBED_ERR), ephemeral=True)
        return
    access_mode, _, message = require_clinx_access(interaction, "cleantoday", destructive=confirm)
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot run cleantoday in this server.")
        return

    today = datetime.now(timezone.utc).date()
    targets = [ch for ch in guild.channels if getattr(ch, "created_at", None) and ch.created_at.date() == today]

    if access_mode == "approval" and confirm:
        async def executor(request: dict[str, Any]) -> None:
            deleted = 0
            try:
                for ch in targets:
                    try:
                        await ch.delete(reason=f"/cleantoday approved for {interaction.user}")
                        deleted += 1
                    except discord.Forbidden:
                        pass
                request["status"] = "completed"
                request["status_text"] = "Owner-approved clean-today completed."
                request["result_text"] = f"Deleted `{deleted}` channels created today."
            except Exception as exc:
                request["status"] = "failed"
                request["status_text"] = "Owner-approved clean-today failed."
                request["result_text"] = str(exc)
            await sync_safety_request_message(request["guild_id"])

        await create_safety_request(
            interaction,
            command_name="cleantoday",
            subtitle="An untrusted admin wants CLINX to delete channels created today in this server.",
            risk_label="Destructive",
            route_text=f"`{guild.name}`",
            selected_actions_text="Delete Channels: ON",
            projected_text=f"Deletes:\n- `{len(targets)}` channels created today\n\nBuild:\n- none",
            executor=executor,
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    if not targets:
        await interaction.followup.send(embed=make_embed("Clean Today", "No channels created today (UTC).", EMBED_INFO), ephemeral=True)
        return

    if not confirm:
        await interaction.followup.send(
            embed=make_embed("Warning", f"Dry run: `{len(targets)}` channels would be deleted. Run again with `confirm=true`.", EMBED_WARN),
            ephemeral=True,
        )
        return

    deleted = 0
    for ch in targets:
        try:
            await ch.delete(reason=f"/cleantoday by {interaction.user}")
            deleted += 1
        except discord.Forbidden:
            pass

    await interaction.followup.send(embed=make_embed("Clean Today Complete", f"Deleted `{deleted}` channels.", EMBED_OK), ephemeral=True)


@bot.tree.command(name="cleanempty", description="Owner-only: delete text channels with no user messages")
@app_commands.default_permissions(administrator=True)
@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
@app_commands.describe(
    include_category="Also delete categories whose entire child channel set is empty",
    confirm="Actually delete the empty channels instead of running a dry scan",
)
async def cleanempty(interaction: discord.Interaction, include_category: bool = False, confirm: bool = False) -> None:
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run this command in a server.", EMBED_ERR), ephemeral=True)
        return

    access_mode, _, message = require_clinx_access(interaction, "cleanempty", destructive=confirm)
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot run cleanempty in this server.")
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    me = guild.me or (guild.get_member(bot.user.id) if bot.user else None)
    text_channels = [
        channel
        for channel in sorted(guild.text_channels, key=lambda item: (item.category.position if item.category else -1, item.position, item.id))
        if can_read_channel_history(channel, me)
    ]

    empty_channels: list[discord.TextChannel] = []
    unreadable_count = max(0, len(guild.text_channels) - len(text_channels))
    for channel in text_channels:
        if not await channel_has_user_messages(channel):
            empty_channels.append(channel)

    empty_channel_ids = {channel.id for channel in empty_channels}
    empty_categories: list[discord.CategoryChannel] = []
    if include_category:
        for category in sorted(guild.categories, key=lambda item: (item.position, item.id)):
            if category.channels and all(child.id in empty_channel_ids for child in category.channels):
                empty_categories.append(category)

    if not empty_channels and not empty_categories:
        detail = "No empty text channels were found."
        if unreadable_count:
            detail += f"\nUnreadable text channels skipped: `{unreadable_count}`"
        await interaction.followup.send(embed=make_embed("Clean Empty", detail, EMBED_INFO), ephemeral=True)
        return

    if not confirm:
        lines = [
            f"Scanned text channels: `{len(text_channels)}`",
            f"Empty text channels: `{len(empty_channels)}`",
        ]
        if include_category:
            lines.append(f"Empty categories: `{len(empty_categories)}`")
        if unreadable_count:
            lines.append(f"Unreadable text channels skipped: `{unreadable_count}`")
        lines.append("")
        lines.append("Run again with `confirm=true` to delete the empty channels.")
        await interaction.followup.send(embed=make_embed("Clean Empty Dry Run", "\n".join(lines), EMBED_WARN), ephemeral=True)
        return

    deleted_channels = 0
    failed_channels = 0
    for channel in sorted(empty_channels, key=lambda item: (item.category.position if item.category else -1, item.position, item.id), reverse=True):
        try:
            await channel.delete(reason=f"/cleanempty by {interaction.user}")
            deleted_channels += 1
        except (discord.Forbidden, discord.HTTPException):
            failed_channels += 1

    deleted_categories = 0
    failed_categories = 0
    if include_category:
        for category in sorted(empty_categories, key=lambda item: (item.position, item.id), reverse=True):
            try:
                await category.delete(reason=f"/cleanempty by {interaction.user}")
                deleted_categories += 1
            except (discord.Forbidden, discord.HTTPException):
                failed_categories += 1

    result_lines = [f"Deleted empty text channels: `{deleted_channels}`"]
    if include_category:
        result_lines.append(f"Deleted empty categories: `{deleted_categories}`")
    if failed_channels:
        result_lines.append(f"Failed text channels: `{failed_channels}`")
    if failed_categories:
        result_lines.append(f"Failed categories: `{failed_categories}`")
    if unreadable_count:
        result_lines.append(f"Unreadable text channels skipped: `{unreadable_count}`")
    await interaction.followup.send(embed=make_embed("Clean Empty Complete", "\n".join(result_lines), EMBED_OK if not (failed_channels or failed_categories) else EMBED_WARN), ephemeral=True)


async def masschannels_command_disabled(interaction: discord.Interaction, layout: str, create_categories: bool = True) -> None:
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run this command in a server.", EMBED_ERR), ephemeral=True)
        return
    access_mode, _, message = require_clinx_access(interaction, "masschannels")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot mass-create channels in this server.")
        return

    items = parse_layout(layout)
    if not items:
        await interaction.response.send_message(embed=make_embed("Mass Channels", "No valid channels found in layout.", EMBED_ERR), ephemeral=True)
        return

    if access_mode == "approval":
        projected_build = []
        category_names = {item.category for item in items if item.category}
        if create_categories and category_names:
            projected_build.append(f"- up to `{len(category_names)}` categories")
        projected_build.append(f"- up to `{len(items)}` channels")

        async def executor(request: dict[str, Any]) -> None:
            created = 0
            skipped = 0
            category_cache: dict[str, discord.CategoryChannel] = {c.name: c for c in guild.categories}
            try:
                for item in items:
                    category = None
                    if item.category:
                        category = category_cache.get(item.category)
                        if category is None and create_categories:
                            category = await guild.create_category(item.category)
                            category_cache[item.category] = category
                    if discord.utils.get(guild.channels, name=item.name):
                        skipped += 1
                        continue
                    if item.kind == "voice":
                        await guild.create_voice_channel(name=item.name, category=category)
                    else:
                        await guild.create_text_channel(name=item.name, category=category, topic=item.topic)
                    created += 1
                request["status"] = "completed"
                request["status_text"] = "Owner-approved mass channel creation completed."
                request["result_text"] = f"Created: `{created}`\nSkipped existing: `{skipped}`"
            except Exception as exc:
                request["status"] = "failed"
                request["status_text"] = "Owner-approved mass channel creation failed."
                request["result_text"] = str(exc)
            await sync_safety_request_message(request["guild_id"])

        await create_safety_request(
            interaction,
            command_name="masschannels",
            subtitle="An untrusted admin wants CLINX to create a large channel layout in this server.",
            risk_label="Non-Destructive",
            route_text=f"`{guild.name}`",
            selected_actions_text="Load Channels: ON",
            projected_text=f"Deletes:\n- none\n\nBuild:\n{chr(10).join(projected_build)}",
            executor=executor,
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    created = 0
    skipped = 0
    category_cache: dict[str, discord.CategoryChannel] = {c.name: c for c in guild.categories}

    for item in items:
        category = None
        if item.category:
            category = category_cache.get(item.category)
            if category is None and create_categories:
                category = await guild.create_category(item.category)
                category_cache[item.category] = category

        if discord.utils.get(guild.channels, name=item.name):
            skipped += 1
            continue

        if item.kind == "voice":
            await guild.create_voice_channel(name=item.name, category=category)
        else:
            await guild.create_text_channel(name=item.name, category=category, topic=item.topic)
        created += 1

    await interaction.followup.send(embed=make_embed("Mass Create Complete", f"Created: `{created}`\nSkipped existing: `{skipped}`", EMBED_OK), ephemeral=True)

async def send_text_file(interaction: discord.Interaction, text: str, filename: str) -> None:
    data = io.BytesIO(text.encode("utf-8"))
    await interaction.response.send_message(file=discord.File(data, filename=filename), ephemeral=True)


@export_group.command(name="guild", description="Export full guild snapshot as JSON")
async def export_guild(interaction: discord.Interaction) -> None:
    access_mode, _, message = require_clinx_access(interaction, "export_guild")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot export guild data in this server.")
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    snapshot = await build_guild_snapshot(guild)
    await send_text_file(interaction, json.dumps(snapshot, indent=2), f"guild_{guild.id}.json")


@export_group.command(name="channels", description="Export channels as JSON or CSV")
@app_commands.choices(fmt=[app_commands.Choice(name="json", value="json"), app_commands.Choice(name="csv", value="csv")])
async def export_channels(interaction: discord.Interaction, fmt: app_commands.Choice[str]) -> None:
    access_mode, _, message = require_clinx_access(interaction, "export_channels")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot export channels in this server.")
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    channels = serialize_guild_snapshot(guild)["channels"]
    if fmt.value == "json":
        await send_text_file(interaction, json.dumps(channels, indent=2), f"channels_{guild.id}.json")
        return

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["type", "name", "category", "topic", "position"])
    writer.writeheader()
    for ch in channels:
        writer.writerow({"type": ch.get("type"), "name": ch.get("name"), "category": ch.get("category"), "topic": ch.get("topic", ""), "position": ch.get("position", 0)})
    await send_text_file(interaction, output.getvalue(), f"channels_{guild.id}.csv")


@export_group.command(name="roles", description="Export all roles as JSON or CSV")
@app_commands.choices(fmt=[app_commands.Choice(name="json", value="json"), app_commands.Choice(name="csv", value="csv")])
async def export_roles(interaction: discord.Interaction, fmt: app_commands.Choice[str]) -> None:
    access_mode, _, message = require_clinx_access(interaction, "export_roles")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot export roles in this server.")
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    roles = serialize_roles(guild)
    if fmt.value == "json":
        await send_text_file(interaction, json.dumps(roles, indent=2), f"roles_{guild.id}.json")
        return

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["name", "permissions", "color", "hoist", "mentionable", "position"])
    writer.writeheader()
    for role in roles:
        writer.writerow(role)
    await send_text_file(interaction, output.getvalue(), f"roles_{guild.id}.csv")



@export_group.command(name="channel", description="Export one channel as JSON")
async def export_channel(
    interaction: discord.Interaction,
    channel: discord.TextChannel | discord.VoiceChannel | discord.CategoryChannel,
) -> None:
    access_mode, _, message = require_clinx_access(interaction, "export_channel")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot export channel data in this server.")
        return
    payload: dict[str, Any] = {
        "id": channel.id,
        "name": channel.name,
        "type": str(channel.type),
        "created_at": channel.created_at.isoformat(),
    }
    if isinstance(channel, discord.TextChannel):
        payload["topic"] = channel.topic
    if isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
        payload["category"] = channel.category.name if channel.category else None

    await send_text_file(interaction, json.dumps(payload, indent=2), f"channel_{channel.id}.json")


@export_group.command(name="role", description="Export one role as JSON")
async def export_role(interaction: discord.Interaction, role: discord.Role) -> None:
    access_mode, _, message = require_clinx_access(interaction, "export_role")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot export role data in this server.")
        return
    payload = {
        "id": role.id,
        "name": role.name,
        "permissions": role.permissions.value,
        "color": role.color.value,
        "hoist": role.hoist,
        "mentionable": role.mentionable,
        "position": role.position,
    }
    await send_text_file(interaction, json.dumps(payload, indent=2), f"role_{role.id}.json")
@export_group.command(name="message", description="Export one message as JSON")
async def export_message(interaction: discord.Interaction, channel: discord.TextChannel, message_id: str) -> None:
    access_mode, _, message = require_clinx_access(interaction, "export_message")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot export messages in this server.")
        return
    try:
        message = await channel.fetch_message(int(message_id))
    except Exception:
        await interaction.response.send_message(embed=make_embed("Error", "Message not found.", EMBED_ERR), ephemeral=True)
        return

    payload = {
        "id": message.id,
        "author": str(message.author),
        "author_id": message.author.id,
        "content": message.content,
        "created_at": message.created_at.isoformat(),
        "attachments": [a.url for a in message.attachments],
    }
    await send_text_file(interaction, json.dumps(payload, indent=2), f"message_{message.id}.json")


@export_group.command(name="reactions", description="Export message reactions as JSON or CSV")
@app_commands.choices(fmt=[app_commands.Choice(name="json", value="json"), app_commands.Choice(name="csv", value="csv")])
async def export_reactions(interaction: discord.Interaction, channel: discord.TextChannel, message_id: str, fmt: app_commands.Choice[str]) -> None:
    access_mode, _, message = require_clinx_access(interaction, "export_reactions")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot export reactions in this server.")
        return
    try:
        message = await channel.fetch_message(int(message_id))
    except Exception:
        await interaction.response.send_message(embed=make_embed("Error", "Message not found.", EMBED_ERR), ephemeral=True)
        return

    rows = [{"emoji": str(r.emoji), "count": r.count, "me": r.me} for r in message.reactions]
    if fmt.value == "json":
        await send_text_file(interaction, json.dumps(rows, indent=2), f"reactions_{message.id}.json")
        return

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["emoji", "count", "me"])
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    await send_text_file(interaction, output.getvalue(), f"reactions_{message.id}.csv")


async def run_import_job(guild: discord.Guild, snapshot: dict[str, Any]) -> None:
    try:
        await apply_snapshot_to_guild(
            snapshot,
            guild,
            delete_roles=False,
            delete_channels=False,
            load_roles=True,
            load_channels=True,
            load_settings=True,
            create_only_missing=False,
        )
        IMPORT_JOBS[guild.id]["status"] = "completed"
        IMPORT_JOBS[guild.id]["finished_at"] = utc_now_iso()
    except asyncio.CancelledError:
        IMPORT_JOBS[guild.id]["status"] = "cancelled"
        IMPORT_JOBS[guild.id]["finished_at"] = utc_now_iso()
        raise
    except Exception as exc:
        IMPORT_JOBS[guild.id]["status"] = "failed"
        IMPORT_JOBS[guild.id]["error"] = str(exc)
        IMPORT_JOBS[guild.id]["finished_at"] = utc_now_iso()


@import_group.command(name="guild", description="Import a guild snapshot JSON file")
@app_commands.default_permissions(administrator=True)
async def import_guild(interaction: discord.Interaction, file: discord.Attachment) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    try:
        payload = await file.read()
        snapshot = json.loads(payload.decode("utf-8"))
    except Exception:
        await interaction.response.send_message(embed=make_embed("Error", "Invalid JSON file.", EMBED_ERR), ephemeral=True)
        return

    access_mode, _, message = require_clinx_access(interaction, "import_guild")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot import guild snapshots in this server.")
        return

    if access_mode == "approval":
        preview = build_backup_plan_preview(snapshot, interaction.guild, {"load_roles", "load_channels", "load_settings"})

        async def executor(request: dict[str, Any]) -> None:
            if interaction.guild is None:
                request["status"] = "failed"
                request["status_text"] = "Import failed because the target server was unavailable."
                request["result_text"] = "Target server missing."
                await sync_safety_request_message(request["guild_id"])
                return
            try:
                await apply_snapshot_to_guild(
                    snapshot,
                    interaction.guild,
                    delete_roles=False,
                    delete_channels=False,
                    load_roles=True,
                    load_channels=True,
                    load_settings=True,
                    create_only_missing=False,
                )
                request["status"] = "completed"
                request["status_text"] = "Owner-approved import completed."
                request["result_text"] = "Guild import finished successfully."
            except Exception as exc:
                request["status"] = "failed"
                request["status_text"] = "Owner-approved import failed."
                request["result_text"] = str(exc)
            await sync_safety_request_message(request["guild_id"])

        delete_lines, build_lines = build_preview_lines(preview)
        await create_safety_request(
            interaction,
            command_name="import guild",
            subtitle="An untrusted admin wants CLINX to import a full guild snapshot in this server.",
            risk_label="Non-Destructive",
            route_text=f"`{interaction.guild.name}`",
            selected_actions_text="\n".join(build_backup_lane_lines({'load_roles', 'load_channels', 'load_settings'})),
            projected_text=f"Deletes:\n{chr(10).join(delete_lines)}\n\nBuild:\n{chr(10).join(build_lines)}",
            executor=executor,
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)
    if interaction.guild.id in IMPORT_JOBS and IMPORT_JOBS[interaction.guild.id].get("status") == "running":
        await interaction.followup.send(embed=make_embed("Busy", "An import is already running.", EMBED_WARN), ephemeral=True)
        return

    task = asyncio.create_task(run_import_job(interaction.guild, snapshot))
    IMPORT_JOBS[interaction.guild.id] = {"status": "running", "started_at": utc_now_iso(), "task": task}
    await interaction.followup.send(embed=make_embed("Import Started", "Use `/import status` to track progress.", EMBED_INFO), ephemeral=True)


@import_group.command(name="status", description="Get current import status")
@app_commands.default_permissions(administrator=True)
async def import_status(interaction: discord.Interaction) -> None:
    access_mode, _, message = require_clinx_access(interaction, "backup_list")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot inspect import status in this server.")
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    job = IMPORT_JOBS.get(guild.id)
    if not job:
        await interaction.response.send_message(embed=make_embed("Import Status", "No import job found.", EMBED_INFO), ephemeral=True)
        return

    desc = f"Status: `{job.get('status', 'unknown')}`\nStarted: `{job.get('started_at', 'n/a')}`"
    if job.get("finished_at"):
        desc += f"\nFinished: `{job['finished_at']}`"
    if job.get("error"):
        desc += f"\nError: `{job['error']}`"
    await interaction.response.send_message(embed=make_embed("Import Status", desc, EMBED_INFO), ephemeral=True)


@import_group.command(name="cancel", description="Cancel running import")
@app_commands.default_permissions(administrator=True)
async def import_cancel(interaction: discord.Interaction) -> None:
    access_mode, _, message = require_clinx_access(interaction, "backup_list")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "You cannot cancel imports in this server.")
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    job = IMPORT_JOBS.get(guild.id)
    if not job or job.get("status") != "running":
        await interaction.response.send_message(embed=make_embed("Import", "No running import to cancel.", EMBED_INFO), ephemeral=True)
        return

    task = job.get("task")
    if task:
        task.cancel()
    await interaction.response.send_message(embed=make_embed("Import", "Cancel requested.", EMBED_WARN), ephemeral=True)


@bot.tree.command(name="help", description="Get a list of commands or more information about a specific command")
@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
async def help_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.send_message(view=CommandLibraryView(interaction.client.user if isinstance(interaction.client, commands.Bot) else None))


@bot.tree.command(name="invite", description="Get bot invite link")
@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
async def invite(interaction: discord.Interaction) -> None:
    app_id = bot.user.id if bot.user else None
    link = build_invite_url(app_id)
    await interaction.response.send_message(embed=make_embed("Invite CLINX", link, EMBED_INFO), ephemeral=True)


@bot.tree.command(name="ping", description="Check CLINX gateway latency")
@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
async def ping(interaction: discord.Interaction) -> None:
    latency_ms = round(bot.latency * 1000)
    await interaction.response.send_message(
        view=PingCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            latency_ms=latency_ms,
        )
    )


@bot.tree.command(name="premium", description="Show the active CLINX premium card for this server")
@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
async def premium(interaction: discord.Interaction) -> None:
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run this command in a server.", EMBED_ERR), ephemeral=True)
        return
    entitlement = get_guild_premium_entitlement(guild.id)
    await interaction.response.send_message(
        view=PremiumStatusCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            guild,
            entitlement=entitlement,
        )
    )


@bot.tree.command(name="leave", description="Make bot leave this server")
@app_commands.default_permissions(administrator=True)
@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
async def leave(interaction: discord.Interaction) -> None:
    access_mode, _, message = require_clinx_access(interaction, "leave")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "Only the server owner can remove CLINX from this server.")
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    await interaction.response.send_message(embed=make_embed("Leaving", "CLINX is leaving this server.", EMBED_WARN), ephemeral=True)
    await guild.leave()


async def handle_access_grant(interaction: discord.Interaction, user: discord.Member, *, ephemeral: bool = True) -> None:
    if not is_developer_user(interaction.user):
        await send_access_denied(interaction, "This developer access command is locked to the CLINX developer.")
        return
    if interaction.guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    full_access_ids = grant_full_access_for_user(interaction.guild, user, granted_by=interaction.user)
    await interaction.response.send_message(
        view=FullAccessRosterCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            interaction.guild,
            title="Full Access Updated",
            subtitle=f"{user.mention} now bypasses CLINX runtime command locks in this server.",
            user_ids=full_access_ids,
            badge_label="Granted",
            badge_style=discord.ButtonStyle.success,
            accent_color=EMBED_OK,
        ),
        ephemeral=ephemeral,
    )


async def handle_access_revoke(interaction: discord.Interaction, user: discord.Member, *, ephemeral: bool = True) -> None:
    if not is_developer_user(interaction.user):
        await send_access_denied(interaction, "This developer access command is locked to the CLINX developer.")
        return
    if interaction.guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    full_access_ids = revoke_full_access_for_user(interaction.guild, user)
    await interaction.response.send_message(
        view=FullAccessRosterCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            interaction.guild,
            title="Full Access Updated",
            subtitle=f"{user.mention} no longer bypasses CLINX runtime command locks in this server.",
            user_ids=full_access_ids,
            badge_label="Revoked",
            badge_style=discord.ButtonStyle.secondary,
            accent_color=EMBED_WARN,
        ),
        ephemeral=ephemeral,
    )


@dev_group.command(name="grant", description="Grant CLINX runtime bypass to a user in this server")
@app_commands.allowed_installs(guilds=False, users=True)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
@app_commands.choices(visibility=DEV_VISIBILITY_CHOICES)
async def dev_grant_slash(
    interaction: discord.Interaction,
    user: discord.Member,
    visibility: app_commands.Choice[str] | None = None,
) -> None:
    await handle_access_grant(interaction, user, ephemeral=resolve_dev_visibility(visibility))


@dev_group.command(name="revoke", description="Revoke CLINX runtime bypass from a user in this server")
@app_commands.allowed_installs(guilds=False, users=True)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
@app_commands.choices(visibility=DEV_VISIBILITY_CHOICES)
async def dev_revoke_slash(
    interaction: discord.Interaction,
    user: discord.Member,
    visibility: app_commands.Choice[str] | None = None,
) -> None:
    await handle_access_revoke(interaction, user, ephemeral=resolve_dev_visibility(visibility))


@dev_group.command(name="dashboard", description="Open the developer console")
@app_commands.allowed_installs(guilds=False, users=True)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
@app_commands.choices(visibility=DEV_VISIBILITY_CHOICES)
async def dev_dashboard_slash(
    interaction: discord.Interaction,
    visibility: app_commands.Choice[str] | None = None,
) -> None:
    if not is_developer_user(interaction.user):
        await send_developer_interaction_denied(interaction)
        return
    await interaction.response.send_message(
        view=DeveloperDashboardView(bot, author_id=interaction.user.id),
        ephemeral=resolve_dev_visibility(visibility),
    )


@dev_group.command(name="gift", description="Gift a premium plan entitlement in this server")
@app_commands.allowed_installs(guilds=False, users=True)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
@app_commands.choices(
    plan=[
        app_commands.Choice(name="Pro", value="pro"),
        app_commands.Choice(name="Pro Plus", value="pro plus"),
        app_commands.Choice(name="Premium Max", value="premium(max)"),
    ],
    visibility=DEV_VISIBILITY_CHOICES,
)
async def dev_gift_slash(
    interaction: discord.Interaction,
    user: discord.Member,
    plan: app_commands.Choice[str],
    visibility: app_commands.Choice[str] | None = None,
) -> None:
    if not is_developer_user(interaction.user):
        await send_developer_interaction_denied(interaction)
        return
    if interaction.guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    plan_key = normalize_premium_plan(plan.value)
    if plan_key is None:
        await interaction.response.send_message(embed=make_embed("Gift Failed", "Unsupported premium plan.", EMBED_ERR), ephemeral=True)
        return

    entitlement = set_guild_premium_entitlement(
        interaction.guild,
        user.id,
        interaction.user.id,
        plan_key,
        gifted_to_display_name=user.display_name,
        gifted_by_display_name=interaction.user.display_name,
    )
    await interaction.response.send_message(
        view=PremiumGiftCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            interaction.guild,
            gifted_member=user,
            gifted_by_id=interaction.user.id,
            entitlement=entitlement,
        ),
        ephemeral=resolve_dev_visibility(visibility, default_private=False),
    )


@dev_group.command(name="purge", description="Purge recent messages in the current channel")
@app_commands.allowed_installs(guilds=False, users=True)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
@app_commands.choices(visibility=DEV_VISIBILITY_CHOICES)
async def dev_purge_slash(
    interaction: discord.Interaction,
    amount: app_commands.Range[int, 1, 200],
    visibility: app_commands.Choice[str] | None = None,
) -> None:
    if not is_developer_user(interaction.user):
        await send_developer_interaction_denied(interaction)
        return
    ephemeral = resolve_dev_visibility(visibility)
    if interaction.guild is None or not isinstance(interaction.channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(embed=make_embed("Purge Failed", "Use this inside a server text channel or thread.", EMBED_ERR), ephemeral=ephemeral)
        return

    await interaction.response.defer(ephemeral=ephemeral, thinking=False)
    try:
        deleted_messages = await interaction.channel.purge(
            limit=amount + 1,
            bulk=True,
            reason=f"CLINX developer purge by {interaction.user} ({interaction.user.id})",
        )
    except (discord.Forbidden, discord.HTTPException):
        await interaction.followup.send(embed=make_embed("Purge Failed", "CLINX could not purge messages in this channel.", EMBED_ERR), ephemeral=ephemeral)
        return

    deleted_count = max(0, len(deleted_messages) - 1)
    await interaction.followup.send(
        embed=make_embed("Purge Complete", f"Deleted `{deleted_count}` recent message(s) in {interaction.channel.mention}.", EMBED_OK),
        ephemeral=ephemeral,
    )


@dev_group.command(name="kick", description="Kick a member with developer override")
@app_commands.allowed_installs(guilds=False, users=True)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
@app_commands.choices(visibility=DEV_VISIBILITY_CHOICES)
async def dev_kick_slash(
    interaction: discord.Interaction,
    user: discord.Member,
    reason: str | None = None,
    visibility: app_commands.Choice[str] | None = None,
) -> None:
    if not is_developer_user(interaction.user):
        await send_developer_interaction_denied(interaction)
        return
    ephemeral = resolve_dev_visibility(visibility)
    if interaction.guild is None:
        await interaction.response.send_message(embed=make_embed("Kick Failed", "Run in a server.", EMBED_ERR), ephemeral=ephemeral)
        return
    try:
        await user.kick(reason=reason or f"CLINX developer kick by {interaction.user} ({interaction.user.id})")
        await interaction.response.send_message(
            embed=make_embed("Kick Executed", f"{user.mention} was kicked by CLINX developer override.", EMBED_OK),
            ephemeral=ephemeral,
        )
    except (discord.Forbidden, discord.HTTPException):
        await interaction.response.send_message(
            embed=make_embed("Kick Failed", f"CLINX could not kick {user.mention}.", EMBED_ERR),
            ephemeral=ephemeral,
        )


@dev_group.command(name="ban", description="Ban a member with developer override")
@app_commands.allowed_installs(guilds=False, users=True)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
@app_commands.choices(visibility=DEV_VISIBILITY_CHOICES)
async def dev_ban_slash(
    interaction: discord.Interaction,
    user: discord.Member,
    reason: str | None = None,
    visibility: app_commands.Choice[str] | None = None,
) -> None:
    if not is_developer_user(interaction.user):
        await send_developer_interaction_denied(interaction)
        return
    ephemeral = resolve_dev_visibility(visibility)
    if interaction.guild is None:
        await interaction.response.send_message(embed=make_embed("Ban Failed", "Run in a server.", EMBED_ERR), ephemeral=ephemeral)
        return
    try:
        await interaction.guild.ban(user, reason=reason or f"CLINX developer ban by {interaction.user} ({interaction.user.id})")
        await interaction.response.send_message(
            embed=make_embed("Ban Executed", f"{user.mention} was banned by CLINX developer override.", EMBED_OK),
            ephemeral=ephemeral,
        )
    except (discord.Forbidden, discord.HTTPException):
        await interaction.response.send_message(
            embed=make_embed("Ban Failed", f"CLINX could not ban {user.mention}.", EMBED_ERR),
            ephemeral=ephemeral,
        )


@dev_group.command(name="deleteallroles", description="Delete every manageable role in this server")
@app_commands.allowed_installs(guilds=False, users=True)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
@app_commands.choices(visibility=DEV_VISIBILITY_CHOICES)
async def dev_delete_all_roles_slash(
    interaction: discord.Interaction,
    visibility: app_commands.Choice[str] | None = None,
) -> None:
    if not is_developer_user(interaction.user):
        await send_developer_interaction_denied(interaction)
        return
    ephemeral = resolve_dev_visibility(visibility)
    if interaction.guild is None:
        await interaction.response.send_message(embed=make_embed("Role Purge Failed", "Run this in a server.", EMBED_ERR), ephemeral=ephemeral)
        return

    me = interaction.guild.me or (interaction.guild.get_member(bot.user.id) if bot.user else None)
    if me is None:
        await interaction.response.send_message(embed=make_embed("Role Purge Failed", "CLINX could not resolve its member state in this server.", EMBED_ERR), ephemeral=ephemeral)
        return

    await interaction.response.defer(ephemeral=ephemeral, thinking=False)
    deletable_roles: list[discord.Role] = []
    blocked_roles = 0
    for role in sorted(interaction.guild.roles, key=lambda item: item.position, reverse=True):
        if role.is_default() or role.managed:
            continue
        if role >= me.top_role:
            blocked_roles += 1
            continue
        deletable_roles.append(role)

    deleted = 0
    failed = 0
    for role in deletable_roles:
        try:
            await role.delete(reason=f"CLINX developer role purge by {interaction.user} ({interaction.user.id})")
            deleted += 1
        except (discord.Forbidden, discord.HTTPException):
            failed += 1

    result_lines = [f"Deleted: `{deleted}`"]
    if blocked_roles:
        result_lines.append(f"Blocked by hierarchy: `{blocked_roles}`")
    if failed:
        result_lines.append(f"Failed: `{failed}`")
    await interaction.followup.send(
        embed=make_embed("Role Purge Complete", "\n".join(result_lines), EMBED_OK if failed == 0 else EMBED_WARN),
        ephemeral=ephemeral,
    )


@dev_group.command(name="deleteallchannels", description="Delete every manageable channel in this server")
@app_commands.allowed_installs(guilds=False, users=True)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
@app_commands.choices(visibility=DEV_VISIBILITY_CHOICES)
async def dev_delete_all_channels_slash(
    interaction: discord.Interaction,
    visibility: app_commands.Choice[str] | None = None,
) -> None:
    if not is_developer_user(interaction.user):
        await send_developer_interaction_denied(interaction)
        return
    ephemeral = resolve_dev_visibility(visibility)
    if interaction.guild is None:
        await interaction.response.send_message(embed=make_embed("Channel Purge Failed", "Run this in a server.", EMBED_ERR), ephemeral=ephemeral)
        return

    guild = interaction.guild
    me = guild.me or (guild.get_member(bot.user.id) if bot.user else None)
    if me is None:
        await interaction.response.send_message(embed=make_embed("Channel Purge Failed", "CLINX could not resolve its member state in this server.", EMBED_ERR), ephemeral=ephemeral)
        return

    await interaction.response.defer(ephemeral=ephemeral, thinking=False)
    channels = [channel for channel in guild.channels if channel.permissions_for(me).manage_channels]
    categories = [channel for channel in channels if isinstance(channel, discord.CategoryChannel)]
    non_categories = [channel for channel in channels if not isinstance(channel, discord.CategoryChannel)]
    deleted = 0
    failed = 0

    for channel in sorted(non_categories, key=lambda item: item.position, reverse=True):
        try:
            await channel.delete(reason=f"CLINX developer channel purge by {interaction.user} ({interaction.user.id})")
            deleted += 1
        except (discord.Forbidden, discord.HTTPException):
            failed += 1

    for category in sorted(categories, key=lambda item: item.position, reverse=True):
        try:
            await category.delete(reason=f"CLINX developer channel purge by {interaction.user} ({interaction.user.id})")
            deleted += 1
        except (discord.Forbidden, discord.HTTPException):
            failed += 1

    await interaction.followup.send(
        embed=make_embed("Channel Purge Complete", f"Deleted: `{deleted}`\nFailed: `{failed}`", EMBED_OK if failed == 0 else EMBED_WARN),
        ephemeral=ephemeral,
    )


@dev_group.command(name="backupmessages", description="Archive full channel and thread message history")
@app_commands.allowed_installs(guilds=False, users=True)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
@app_commands.choices(visibility=DEV_VISIBILITY_CHOICES)
async def dev_backup_messages_slash(
    interaction: discord.Interaction,
    guild_id: int | None = None,
    visibility: app_commands.Choice[str] | None = None,
) -> None:
    if not is_developer_user(interaction.user):
        await send_developer_interaction_denied(interaction)
        return
    ephemeral = resolve_dev_visibility(visibility)

    target_guild = interaction.guild if guild_id is None else bot.get_guild(guild_id)
    if target_guild is None:
        await interaction.response.send_message(embed=make_embed("Message Backup Failed", "Target guild not found.", EMBED_ERR), ephemeral=ephemeral)
        return

    await interaction.response.defer(ephemeral=ephemeral, thinking=True)
    try:
        archive_path, summary = await build_message_archive_for_guild(target_guild, requested_by=interaction.user)
    except discord.Forbidden:
        await interaction.followup.send(
            embed=make_embed(
                "Message Backup Failed",
                "CLINX cannot read one or more channels in that server. Check `View Channel` and `Read Message History` permissions.",
                EMBED_ERR,
            ),
            ephemeral=ephemeral,
        )
        return
    except Exception as exc:
        await interaction.followup.send(
            embed=make_embed(
                "Message Backup Failed",
                f"CLINX hit an exception while archiving messages.\n`{str(exc)[:1500]}`",
                EMBED_ERR,
            ),
            ephemeral=ephemeral,
        )
        return

    description = (
        f"Archive ready for `{target_guild.name}`.\n"
        f"Channels scanned: `{summary['channel_count']}`\n"
        f"Threads scanned: `{summary['thread_count']}`\n"
        f"Messages archived: `{summary['message_count']}`\n"
        f"Saved as: `{archive_path.name}`"
    )
    dm_sent = False
    if archive_path.stat().st_size <= 24 * 1024 * 1024:
        try:
            await interaction.user.send(
                embed=make_embed("Message Backup Complete", description, EMBED_OK),
                file=discord.File(str(archive_path), filename=archive_path.name),
            )
            dm_sent = True
        except discord.HTTPException:
            dm_sent = False

    description += "\nDM delivery: `sent`" if dm_sent else "\nDM delivery: `failed or archive too large`"
    if summary.get("storage", {}).get("r2_key"):
        description += f"\nCloud storage: `{summary['storage']['r2_key']}`"
    await interaction.followup.send(embed=make_embed("Message Backup Complete", description, EMBED_OK), ephemeral=ephemeral)


def grant_full_access_for_user(
    guild: discord.Guild,
    user: discord.abc.User,
    *,
    granted_by: discord.abc.User | None = None,
) -> list[str]:
    store = load_safety_store()
    bucket = get_guild_safety_bucket(store, guild.id)
    full_access_ids = set(bucket.get("full_access_user_ids", []))
    user_id = str(user.id)
    full_access_ids.add(user_id)
    bucket["full_access_user_ids"] = sorted(full_access_ids)
    records = bucket.setdefault("full_access_records", {})
    records[user_id] = {
        "user_id": user_id,
        "user_display_name": getattr(user, "display_name", str(user)),
        "granted_at": utc_now_iso(),
        "granted_by_user_id": str(granted_by.id) if granted_by else "",
        "granted_by_display_name": getattr(granted_by, "display_name", str(granted_by)) if granted_by else "Unknown Operator",
        "guild_name": guild.name,
    }
    save_safety_store(store)
    return bucket["full_access_user_ids"]


def revoke_full_access_for_user(guild: discord.Guild, user: discord.Member) -> list[str]:
    return revoke_full_access_by_user_id(guild.id, user.id)


def revoke_full_access_by_user_id(guild_id: int, user_id: int) -> list[str]:
    store = load_safety_store()
    bucket = get_guild_safety_bucket(store, guild_id)
    user_id_str = str(user_id)
    full_access_ids = {str(raw_user_id) for raw_user_id in bucket.get("full_access_user_ids", [])}
    full_access_ids.discard(user_id_str)
    bucket["full_access_user_ids"] = sorted(full_access_ids)
    records = bucket.setdefault("full_access_records", {})
    records.pop(user_id_str, None)
    save_safety_store(store)
    return bucket["full_access_user_ids"]


async def send_temp_prefix_notice(
    ctx: commands.Context,
    title: str,
    description: str,
    color: int,
    *,
    delay_seconds: float = 3.0,
) -> None:
    message = await ctx.send(embed=make_embed(title, description, color))
    await asyncio.sleep(delay_seconds)
    try:
        await message.delete()
    except (discord.Forbidden, discord.HTTPException):
        return


async def send_temp_prefix_notice_nowait(
    ctx: commands.Context,
    title: str,
    description: str,
    color: int,
    *,
    delay_seconds: float = 3.0,
) -> None:
    message = await ctx.send(embed=make_embed(title, description, color))

    async def _delete_later() -> None:
        await asyncio.sleep(delay_seconds)
        try:
            await message.delete()
        except (discord.Forbidden, discord.HTTPException):
            return

    asyncio.create_task(_delete_later(), name="clinx-temp-prefix-notice-delete")


@bot.command(name="grant", hidden=True)
async def dev_grant(ctx: commands.Context, member: discord.Member, mode: str | None = None) -> None:
    if not is_developer_user(ctx.author):
        return
    if ctx.guild is None:
        await ctx.send("Run this in a server.")
        return
    if (mode or "").casefold() != "obypass":
        await ctx.send("Usage: `^^^grant @user obypass`")
        return
    full_access_ids = grant_full_access_for_user(ctx.guild, member, granted_by=ctx.author)
    await ctx.send(
        view=FullAccessRosterCardView(
            bot.user,
            ctx.guild,
            title="Override Granted",
            subtitle=f"{member.mention} now has full CLINX operator bypass in this server.",
            user_ids=full_access_ids,
            badge_label="Granted",
            badge_style=discord.ButtonStyle.success,
            accent_color=EMBED_OK,
        )
    )


@bot.command(name="revoke", hidden=True)
async def dev_revoke(ctx: commands.Context, member: discord.Member, mode: str | None = None) -> None:
    if not is_developer_user(ctx.author):
        return
    if ctx.guild is None:
        await ctx.send("Run this in a server.")
        return
    if (mode or "").casefold() != "obypass":
        await ctx.send("Usage: `^^^revoke @user obypass`")
        return
    full_access_ids = revoke_full_access_for_user(ctx.guild, member)
    await ctx.send(
        view=FullAccessRosterCardView(
            bot.user,
            ctx.guild,
            title="Override Revoked",
            subtitle=f"{member.mention} no longer has full CLINX operator bypass in this server.",
            user_ids=full_access_ids,
            badge_label="Revoked",
            badge_style=discord.ButtonStyle.secondary,
            accent_color=EMBED_WARN,
        )
    )


@safety_group.command(name="grant", description="Trust one admin account for protected CLINX actions")
@app_commands.default_permissions(administrator=True)
async def safety_grant(interaction: discord.Interaction, user: discord.Member) -> None:
    access_mode, _, message = require_clinx_access(interaction, "safety_grant")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "Only the server owner can grant CLINX trust.")
        return
    if interaction.guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return
    store = load_safety_store()
    bucket = get_guild_safety_bucket(store, interaction.guild.id)
    trusted_ids = set(bucket.get("trusted_admin_ids", []))
    trusted_ids.add(str(user.id))
    bucket["trusted_admin_ids"] = sorted(trusted_ids)
    save_safety_store(store)
    await interaction.response.send_message(
        view=SafetyRosterCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            interaction.guild,
            title="Safety Trust Updated",
            subtitle=f"{user.mention} is now a trusted CLINX admin in this server.",
            trusted_ids=bucket["trusted_admin_ids"],
            badge_label="Granted",
            badge_style=discord.ButtonStyle.success,
            accent_color=EMBED_OK,
        ),
        ephemeral=True,
    )


@safety_group.command(name="revoke", description="Remove CLINX trust from an admin account")
@app_commands.default_permissions(administrator=True)
async def safety_revoke(interaction: discord.Interaction, user: discord.Member) -> None:
    access_mode, _, message = require_clinx_access(interaction, "safety_revoke")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "Only the server owner can revoke CLINX trust.")
        return
    if interaction.guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return
    store = load_safety_store()
    bucket = get_guild_safety_bucket(store, interaction.guild.id)
    trusted_ids = {str(user_id) for user_id in bucket.get("trusted_admin_ids", [])}
    trusted_ids.discard(str(user.id))
    bucket["trusted_admin_ids"] = sorted(trusted_ids)
    save_safety_store(store)
    await interaction.response.send_message(
        view=SafetyRosterCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            interaction.guild,
            title="Safety Trust Updated",
            subtitle=f"{user.mention} is no longer a trusted CLINX admin in this server.",
            trusted_ids=bucket["trusted_admin_ids"],
            badge_label="Revoked",
            badge_style=discord.ButtonStyle.secondary,
            accent_color=EMBED_WARN,
        ),
        ephemeral=True,
    )


@safety_group.command(name="list", description="List trusted CLINX admins for this server")
@app_commands.default_permissions(administrator=True)
async def safety_list(interaction: discord.Interaction) -> None:
    access_mode, _, message = require_clinx_access(interaction, "safety_list")
    if access_mode == "deny":
        await send_access_denied(interaction, message or "Only the server owner can inspect the CLINX trust list.")
        return
    if interaction.guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return
    store = load_safety_store()
    bucket = get_guild_safety_bucket(store, interaction.guild.id)
    trusted_ids = bucket.get("trusted_admin_ids", [])
    await interaction.response.send_message(
        view=SafetyRosterCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            interaction.guild,
            title="Safety Trust Roster",
            subtitle="These admins can bypass owner approval for protected CLINX actions in this server.",
            trusted_ids=trusted_ids,
            badge_label="Owner Only",
            badge_style=discord.ButtonStyle.primary,
            accent_color=EMBED_INFO,
        ),
        ephemeral=True,
    )


@bot.command(name="deleteallroles", hidden=True)
async def dev_delete_all_roles(ctx: commands.Context) -> None:
    if not is_developer_user(ctx.author):
        return
    if ctx.guild is None:
        await send_temp_prefix_notice(ctx, "Role Purge Failed", "Run this in a server.", EMBED_ERR)
        return

    me = ctx.guild.me or (ctx.guild.get_member(bot.user.id) if bot.user else None)
    if me is None:
        await send_temp_prefix_notice(ctx, "Role Purge Failed", "CLINX could not resolve its member state in this server.", EMBED_ERR)
        return

    deletable_roles: list[discord.Role] = []
    blocked_roles = 0
    for role in sorted(ctx.guild.roles, key=lambda item: item.position, reverse=True):
        if role.is_default() or role.managed:
            continue
        if role >= me.top_role:
            blocked_roles += 1
            continue
        deletable_roles.append(role)

    deleted = 0
    failed = 0
    for role in deletable_roles:
        try:
            await role.delete(reason=f"CLINX developer role purge by {ctx.author} ({ctx.author.id})")
            deleted += 1
        except (discord.Forbidden, discord.HTTPException):
            failed += 1

    result_lines = [f"Deleted: `{deleted}`"]
    if blocked_roles:
        result_lines.append(f"Blocked by hierarchy: `{blocked_roles}`")
    if failed:
        result_lines.append(f"Failed: `{failed}`")
    await send_temp_prefix_notice(ctx, "Role Purge Complete", "\n".join(result_lines), EMBED_OK if failed == 0 else EMBED_WARN)


@bot.command(name="deleteallchannels", hidden=True)
async def dev_delete_all_channels(ctx: commands.Context) -> None:
    if not is_developer_user(ctx.author):
        return
    if ctx.guild is None:
        await send_temp_prefix_notice(ctx, "Channel Purge Failed", "Run this in a server.", EMBED_ERR)
        return

    guild = ctx.guild
    me = guild.me or (guild.get_member(bot.user.id) if bot.user else None)
    if me is None:
        await send_temp_prefix_notice(ctx, "Channel Purge Failed", "CLINX could not resolve its member state in this server.", EMBED_ERR)
        return

    channels = [channel for channel in guild.channels if channel.permissions_for(me).manage_channels]
    categories = [channel for channel in channels if isinstance(channel, discord.CategoryChannel)]
    non_categories = [channel for channel in channels if not isinstance(channel, discord.CategoryChannel)]
    deleted = 0
    failed = 0

    for channel in sorted(non_categories, key=lambda item: item.position, reverse=True):
        try:
            await channel.delete(reason=f"CLINX developer channel purge by {ctx.author} ({ctx.author.id})")
            deleted += 1
        except (discord.Forbidden, discord.HTTPException):
            failed += 1

    for category in sorted(categories, key=lambda item: item.position, reverse=True):
        try:
            await category.delete(reason=f"CLINX developer channel purge by {ctx.author} ({ctx.author.id})")
            deleted += 1
        except (discord.Forbidden, discord.HTTPException):
            failed += 1

    try:
        await ctx.author.send(embed=make_embed("Channel Purge Complete", f"Deleted: `{deleted}`\nFailed: `{failed}`", EMBED_OK if failed == 0 else EMBED_WARN))
    except (discord.Forbidden, discord.HTTPException):
        pass


@bot.command(name="kick", hidden=True)
async def dev_kick(ctx: commands.Context, member: discord.Member, *, reason: str | None = None) -> None:
    if not is_developer_user(ctx.author):
        return
    if ctx.guild is None:
        await send_temp_prefix_notice(ctx, "Kick Failed", "Run this in a server.", EMBED_ERR)
        return
    try:
        await member.kick(reason=reason or f"CLINX developer kick by {ctx.author} ({ctx.author.id})")
        await send_temp_prefix_notice(ctx, "Kick Executed", f"{member.mention} was kicked by CLINX developer override.", EMBED_OK)
    except (discord.Forbidden, discord.HTTPException):
        await send_temp_prefix_notice(ctx, "Kick Failed", f"CLINX could not kick {member.mention}.", EMBED_ERR)


@bot.command(name="ban", hidden=True)
async def dev_ban(ctx: commands.Context, member: discord.Member, *, reason: str | None = None) -> None:
    if not is_developer_user(ctx.author):
        return
    if ctx.guild is None:
        await send_temp_prefix_notice(ctx, "Ban Failed", "Run this in a server.", EMBED_ERR)
        return
    try:
        await ctx.guild.ban(member, reason=reason or f"CLINX developer ban by {ctx.author} ({ctx.author.id})")
        await send_temp_prefix_notice(ctx, "Ban Executed", f"{member.mention} was banned by CLINX developer override.", EMBED_OK)
    except (discord.Forbidden, discord.HTTPException):
        await send_temp_prefix_notice(ctx, "Ban Failed", f"CLINX could not ban {member.mention}.", EMBED_ERR)


@bot.command(name="purge", hidden=True)
async def dev_purge(ctx: commands.Context, amount: int | None = None) -> None:
    if not is_developer_user(ctx.author):
        return
    if ctx.guild is None:
        await send_temp_prefix_notice(ctx, "Purge Failed", "Run this in a server channel.", EMBED_ERR)
        return
    if not isinstance(ctx.channel, (discord.TextChannel, discord.Thread)):
        await send_temp_prefix_notice(ctx, "Purge Failed", "Use this inside a text channel or thread.", EMBED_ERR)
        return

    purge_count = max(1, min(int(amount or 1), 200))
    try:
        deleted_messages = await ctx.channel.purge(
            limit=purge_count + 1,
            bulk=True,
            reason=f"CLINX developer purge by {ctx.author} ({ctx.author.id})",
        )
    except (discord.Forbidden, discord.HTTPException):
        await send_temp_prefix_notice(ctx, "Purge Failed", "CLINX could not purge messages in this channel.", EMBED_ERR)
        return

    deleted_count = max(0, len(deleted_messages) - 1)
    await send_temp_prefix_notice_nowait(
        ctx,
        "Purge Complete",
        f"Deleted `{deleted_count}` recent message(s) in {ctx.channel.mention}.",
        EMBED_OK,
    )


@bot.command(name="gift", hidden=True)
async def dev_gift(ctx: commands.Context, member: discord.Member, *, plan_text: str | None = None) -> None:
    if not is_developer_user(ctx.author):
        return
    if ctx.guild is None:
        await ctx.send("Run this in a server.")
        return

    plan_key = normalize_premium_plan(plan_text)
    if plan_key is None:
        await ctx.send("Usage: `^^^gift @user pro`, `^^^gift @user pro plus`, or `^^^gift @user premium(max)`")
        return

    entitlement = set_guild_premium_entitlement(
        ctx.guild,
        member.id,
        ctx.author.id,
        plan_key,
        gifted_to_display_name=member.display_name,
        gifted_by_display_name=ctx.author.display_name,
    )
    await ctx.send(
        view=PremiumGiftCardView(
            bot.user,
            ctx.guild,
            gifted_member=member,
            gifted_by_id=ctx.author.id,
            entitlement=entitlement,
        )
    )


@bot.command(name="backupmessages", hidden=True)
async def dev_backup_messages(ctx: commands.Context, guild_id: int | None = None) -> None:
    if not is_developer_user(ctx.author):
        return

    target_guild = ctx.guild if guild_id is None else bot.get_guild(guild_id)
    if target_guild is None:
        await send_temp_prefix_notice(ctx, "Message Backup Failed", "Target guild not found.", EMBED_ERR, delay_seconds=5.0)
        return

    status_message = await ctx.send(
        embed=make_embed(
            "Message Backup Started",
            f"CLINX is scanning accessible text history in `{target_guild.name}` now.",
            EMBED_INFO,
        )
    )

    async def update_progress(text: str) -> None:
        try:
            await status_message.edit(embed=make_embed("Message Backup Running", text, EMBED_INFO))
        except discord.HTTPException:
            pass

    try:
        archive_path, summary = await build_message_archive_for_guild(
            target_guild,
            requested_by=ctx.author,
            progress_hook=update_progress,
        )
    except discord.Forbidden:
        await status_message.edit(
            embed=make_embed(
                "Message Backup Failed",
                "CLINX cannot read one or more channels in that server. Check `View Channel` and `Read Message History` permissions.",
                EMBED_ERR,
            )
        )
        return
    except Exception as exc:
        await status_message.edit(
            embed=make_embed(
                "Message Backup Failed",
                f"CLINX hit an exception while archiving messages.\n`{str(exc)[:1500]}`",
                EMBED_ERR,
            )
        )
        return

    description = (
        f"Archive ready for `{target_guild.name}`.\n"
        f"Channels scanned: `{summary['channel_count']}`\n"
        f"Threads scanned: `{summary['thread_count']}`\n"
        f"Messages archived: `{summary['message_count']}`\n"
        f"Saved as: `{archive_path.name}`"
    )

    dm_sent = False
    if archive_path.stat().st_size <= 24 * 1024 * 1024:
        try:
            await ctx.author.send(
                embed=make_embed("Message Backup Complete", description, EMBED_OK),
                file=discord.File(str(archive_path), filename=archive_path.name),
            )
            dm_sent = True
        except discord.HTTPException:
            dm_sent = False

    if dm_sent:
        description += "\nDM delivery: `sent`"
    else:
        description += "\nDM delivery: `failed or archive too large`"
    if summary.get("storage", {}).get("r2_key"):
        description += f"\nCloud storage: `{summary['storage']['r2_key']}`"

    await status_message.edit(embed=make_embed("Message Backup Complete", description, EMBED_OK))


@bot.command(name="dashboard", hidden=True, aliases=["devpanel"])
async def dev_dashboard(ctx: commands.Context) -> None:
    if not is_developer_user(ctx.author):
        return

    dashboard_view = DeveloperDashboardView(bot, author_id=ctx.author.id)
    if ctx.guild is None:
        await ctx.send(view=dashboard_view)
        return

    try:
        dm_channel = ctx.author.dm_channel or await ctx.author.create_dm()
        await dm_channel.send(view=dashboard_view)
    except (discord.Forbidden, discord.HTTPException):
        try:
            await ctx.send(view=dashboard_view)
        except (discord.Forbidden, discord.HTTPException):
            await send_temp_prefix_notice(
                ctx,
                "Dashboard Delivery Failed",
                "CLINX could not deliver the developer console in DM or this channel.",
                EMBED_ERR,
                delay_seconds=5.0,
            )
        return

    try:
        await ctx.message.delete()
    except (discord.Forbidden, discord.HTTPException):
        pass

    await send_temp_prefix_notice(
        ctx,
        "Developer Console",
        "CLINX delivered the developer dashboard in your DMs.",
        EMBED_INFO,
    )


if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN is not set. Put BOT_TOKEN in .env and run start.bat")

    ensure_storage()
    cooldown_seconds = max(60, int(os.getenv("BOT_LOGIN_429_COOLDOWN", "900")))
    backoff_cap_seconds = max(cooldown_seconds, int(os.getenv("BOT_LOGIN_429_COOLDOWN_MAX", "3600")))

    while True:
        try:
            bot.run(TOKEN)
            break
        except discord.LoginFailure:
            raise
        except discord.HTTPException as exc:
            if exc.status != 429:
                raise

            response = getattr(exc, "response", None)
            retry_header = None
            if response is not None:
                retry_header = response.headers.get("Retry-After") or response.headers.get("X-RateLimit-Reset-After")

            try:
                retry_after = max(float(retry_header), float(cooldown_seconds)) if retry_header else float(cooldown_seconds)
            except (TypeError, ValueError):
                retry_after = float(cooldown_seconds)

            wait_seconds = min(int(retry_after), backoff_cap_seconds)
            print(
                "Discord login is rate limited (HTTP 429). "
                f"Waiting {wait_seconds} seconds before retrying."
            )
            time.sleep(wait_seconds)
            cooldown_seconds = min(cooldown_seconds * 2, backoff_cap_seconds)








