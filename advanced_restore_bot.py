import asyncio
import csv
import io
import json
import os
import re
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, TypeVar

import discord
from discord import app_commands
from discord.ext import commands

TOKEN = os.getenv("BOT_TOKEN")
DEFAULT_BACKUP_GUILD_ID = os.getenv("DEFAULT_BACKUP_GUILD_ID")
SUPPORT_SERVER_URL = os.getenv("SUPPORT_SERVER_URL", "https://discord.gg/V6YEw2Wxcb")

DATA_DIR = Path(__file__).parent / "data"
BACKUP_FILE = DATA_DIR / "backups.json"

EMBED_OK = 0x2ECC71
EMBED_WARN = 0xF1C40F
EMBED_ERR = 0xE74C3C
EMBED_INFO = 0x3498DB

IMPORT_JOBS: dict[int, dict[str, Any]] = {}
ACTION_DELAY_SECONDS = max(0.0, float(os.getenv("DISCORD_ACTION_DELAY_SECONDS", "1.0")))
ACTION_RETRY_LIMIT = max(1, int(os.getenv("DISCORD_ACTION_RETRY_LIMIT", "5")))
T = TypeVar("T")


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


def build_invite_link() -> str:
    app_id = bot.user.id if "bot" in globals() and bot.user else "YOUR_APP_ID"
    return f"https://discord.com/oauth2/authorize?client_id={app_id}&permissions=8&integration_type=0&scope=bot+applications.commands"


def ensure_storage() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not BACKUP_FILE.exists():
        BACKUP_FILE.write_text(json.dumps({"backups": {}, "users": {}}, indent=2), encoding="utf-8")


def normalize_backup_store(store: dict[str, Any]) -> dict[str, Any]:
    backups = store.get("backups")
    if not isinstance(backups, dict):
        backups = {}
    store["backups"] = backups

    users = store.get("users")
    if not isinstance(users, dict):
        users = {}
    store["users"] = users

    for backup_id, record in backups.items():
        if not isinstance(record, dict):
            continue

        user_id = record.get("created_by_user_id")
        if user_id is None:
            continue

        user_key = str(user_id)
        user_entry = users.setdefault(
            user_key,
            {
                "user_id": user_id,
                "display_name": record.get("created_by_display_name", "Unknown User"),
                "backup_ids": [],
            },
        )
        if not isinstance(user_entry.get("backup_ids"), list):
            user_entry["backup_ids"] = []
        if backup_id not in user_entry["backup_ids"]:
            user_entry["backup_ids"].append(backup_id)

    return store


def load_backup_store() -> dict[str, Any]:
    ensure_storage()
    store = json.loads(BACKUP_FILE.read_text(encoding="utf-8"))
    return normalize_backup_store(store)


def save_backup_store(store: dict[str, Any]) -> None:
    ensure_storage()
    normalized = normalize_backup_store(store)
    BACKUP_FILE.write_text(json.dumps(normalized, indent=2), encoding="utf-8")


def register_backup_owner(
    store: dict[str, Any],
    *,
    backup_id: str,
    user_id: int,
    display_name: str,
) -> None:
    users = store.setdefault("users", {})
    user_key = str(user_id)
    user_entry = users.setdefault(
        user_key,
        {
            "user_id": user_id,
            "display_name": display_name,
            "backup_ids": [],
        },
    )
    user_entry["display_name"] = display_name
    if not isinstance(user_entry.get("backup_ids"), list):
        user_entry["backup_ids"] = []
    if backup_id not in user_entry["backup_ids"]:
        user_entry["backup_ids"].append(backup_id)


def unregister_backup_owner(store: dict[str, Any], *, backup_id: str, user_id: int | None) -> None:
    if user_id is None:
        return

    users = store.get("users", {})
    user_entry = users.get(str(user_id))
    if not isinstance(user_entry, dict):
        return

    backup_ids = user_entry.get("backup_ids")
    if isinstance(backup_ids, list):
        user_entry["backup_ids"] = [item for item in backup_ids if item != backup_id]


def parse_backup_created_at(record: dict[str, Any]) -> datetime:
    created_at = record.get("created_at")
    if isinstance(created_at, str):
        try:
            return datetime.fromisoformat(created_at)
        except ValueError:
            pass
    return datetime.min.replace(tzinfo=timezone.utc)


def get_user_backup_records(store: dict[str, Any], user_id: int) -> list[dict[str, Any]]:
    backups = store.get("backups", {})
    users = store.get("users", {})
    user_entry = users.get(str(user_id), {})
    backup_ids = user_entry.get("backup_ids", []) if isinstance(user_entry, dict) else []

    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    if isinstance(backup_ids, list):
        for backup_id in backup_ids:
            record = backups.get(backup_id)
            if isinstance(record, dict):
                records.append(record)
                seen_ids.add(backup_id)

    for backup_id, record in backups.items():
        if backup_id in seen_ids or not isinstance(record, dict):
            continue
        if record.get("created_by_user_id") == user_id:
            records.append(record)

    records.sort(key=parse_backup_created_at, reverse=True)
    return records


def format_backup_timestamp(record: dict[str, Any]) -> str:
    created_at = parse_backup_created_at(record)
    if created_at == datetime.min.replace(tzinfo=timezone.utc):
        return "Unknown time"
    return created_at.astimezone(timezone.utc).strftime("%d %b %Y - %H:%M UTC")


def build_backup_choice_label(record: dict[str, Any]) -> str:
    source_name = str(record.get("source_guild_name", "Unknown Source")).strip() or "Unknown Source"
    backup_id = str(record.get("id", "UNKNOWN"))
    timestamp = format_backup_timestamp(record)
    label = f"{source_name} | {timestamp} ({backup_id})"
    if len(label) <= 100:
        return label

    headroom = max(10, 100 - len(timestamp) - len(backup_id) - 6)
    truncated_source = source_name[:headroom].rstrip()
    return f"{truncated_source} | {timestamp} ({backup_id})"


async def user_backup_id_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    store = load_backup_store()
    records = get_user_backup_records(store, interaction.user.id)
    needle = current.casefold().strip()
    choices: list[app_commands.Choice[str]] = []

    for record in records:
        backup_id = str(record.get("id", ""))
        source_name = str(record.get("source_guild_name", ""))
        timestamp = format_backup_timestamp(record)
        haystack = f"{backup_id} {source_name} {timestamp}".casefold()
        if needle and needle not in haystack:
            continue
        choices.append(app_commands.Choice(name=build_backup_choice_label(record), value=backup_id))
        if len(choices) >= 25:
            break

    return choices


@dataclass
class ParsedChannel:
    kind: str
    name: str
    topic: str | None
    category: str | None


@dataclass(frozen=True)
class LibraryCommand:
    name: str
    summary: str
    detail: str


@dataclass
class LayoutLine:
    raw: str
    text: str
    indent: int


COMMAND_PAGE_SIZE = 5
LIBRARY_ACCENT = 0x5B7CFF
SUGGESTION_ACCENT = 0x39D0C8
BACKUP_PLANNER_ACCENT = 0xF4C542
BACKUP_ACTION_ORDER = (
    "delete_roles",
    "delete_channels",
    "load_roles",
    "load_channels",
    "load_settings",
)
BACKUP_ACTION_LABELS = {
    "delete_roles": "Delete Roles",
    "delete_channels": "Delete Channels",
    "load_roles": "Load Roles",
    "load_channels": "Load Channels",
    "load_settings": "Load Settings",
}
COMMAND_LIBRARY: dict[str, list[LibraryCommand]] = {
    "Backup": [
        LibraryCommand("backup create", "Create a fresh server backup and return its load ID.", "Snapshots the selected source guild, stores it in the CLINX backup DB, and tags the creator in the backup record."),
        LibraryCommand("backup load", "Open the restore planner for one of your backups.", "The load ID field autocompletes only backups created by your account and then opens the paced restore planner."),
        LibraryCommand("backup list", "Show only the backups created by your account.", "Lists your own recent backup IDs with source guild names and exact creation timestamps."),
        LibraryCommand("backup delete", "Delete one of your backup codes from the DB.", "Removes a backup only if it belongs to your account and unregisters it from your stored backup list."),
        LibraryCommand("backupcreate", "Alias of /backup create.", "Shortcut alias for fast backup creation."),
        LibraryCommand("backupload", "Alias of /backup load.", "Shortcut alias for opening the backup restore planner."),
        LibraryCommand("backuplist", "Alias of /backup list.", "Shortcut alias for opening your private backup list anywhere CLINX is installed."),
    ],
    "Transfer": [
        LibraryCommand("restore_missing", "Create only missing categories and channels.", "Compares the source guild snapshot against the target guild and creates only what is absent."),
        LibraryCommand("cleantoday", "Delete channels created today.", "Runs as a dry-run first unless confirm is enabled, then deletes only channels created on the current UTC date."),
        LibraryCommand("masschannels", "Open the bulk layout importer modal.", "Paste one or two large layout blocks and CLINX will infer categories, text channels, voice channels, and channel topics."),
        LibraryCommand("panel suggestion", "Post the public suggestion board.", "Sends a public CLINX suggestion board to the current channel while keeping the command acknowledgement private."),
    ],
    "Data": [
        LibraryCommand("export guild", "Export the full guild snapshot as JSON.", "Includes categories, channels, settings, and roles in one file."),
        LibraryCommand("export channels", "Export channel data as JSON or CSV.", "Good for auditing channel names, categories, and topics before a migration."),
        LibraryCommand("export roles", "Export all server roles as JSON or CSV.", "Captures role names, permissions, colors, hoist, mentionability, and positions."),
        LibraryCommand("export channel", "Export one channel as JSON.", "Useful for quickly inspecting one channel's metadata."),
        LibraryCommand("export role", "Export one role as JSON.", "Useful for role permission checks and targeted migration prep."),
        LibraryCommand("export message", "Export one message as JSON.", "Fetches a message by ID and saves author, content, timestamp, and attachment URLs."),
        LibraryCommand("export reactions", "Export reactions on one message.", "Supports JSON and CSV outputs for reaction counts."),
        LibraryCommand("import guild", "Start a background import from a snapshot file.", "Reads a guild snapshot JSON and launches a tracked import job."),
        LibraryCommand("import status", "Check the current import job state.", "Shows running, completed, cancelled, or failed status plus timing details."),
        LibraryCommand("import cancel", "Cancel a running import job.", "Sends a cancellation request to the active import task for this guild."),
    ],
    "Utility": [
        LibraryCommand("help", "Open the CLINX command library.", "Shows this interactive command browser with categories, paging, and detailed command notes."),
        LibraryCommand("invite", "Generate the bot invite link.", "Builds the current CLINX OAuth2 invite using the active application ID."),
        LibraryCommand("leave", "Make CLINX leave the current server.", "Only for admins when you intentionally want the bot removed from a guild."),
    ],
}


def clean_channel_name(raw: str) -> str:
    name = raw.strip().lower()
    name = re.sub(r"^[\s\-*]+", "", name)
    name = name.replace("#", "")
    name = re.sub(r"\s+", "-", name)
    name = re.sub(r"[^a-z0-9_\-]", "", name)
    name = re.sub(r"-+", "-", name).strip("-")
    return name[:100]


def clean_category_name(raw: str) -> str:
    name = raw.strip().strip("[]")
    name = re.sub(r"^[\s\-*•·▪▫●○◆◇▶▷▸▹►➜➤➥│┃┆┊└├┌┬╰╭╮╯─═>]+", "", name).strip()
    name = re.sub(r"\s{2,}", " ", name)
    name = re.sub(r"\s*:\s*$", "", name)
    return name[:100]


def strip_layout_prefix(raw: str) -> str:
    cleaned = raw.strip()
    cleaned = re.sub(r"^[`>]+", "", cleaned).strip()
    cleaned = re.sub(r"^[\-\*\u2022\u00b7\u25aa\u25ab\u25cf\u25cb\u25c6\u25c7\u25b6\u25b7\u25b8\u25b9\u25ba\u279c\u27a4\u27a5│┃┆┊└├┌┬╰╭╮╯─═]+\s*", "", cleaned)
    cleaned = re.sub(r"^[0-9]+\.\s*", "", cleaned)
    return cleaned.strip()


def mostly_upper(text: str) -> bool:
    letters = [char for char in text if char.isalpha()]
    if not letters:
        return False
    upper = sum(1 for char in letters if char.isupper())
    return upper / len(letters) >= 0.75


def build_layout_lines(layout: str) -> list[LayoutLine]:
    lines: list[LayoutLine] = []
    for raw_line in layout.splitlines():
        if not raw_line.strip():
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" \t"))
        text = strip_layout_prefix(raw_line)
        if text:
            lines.append(LayoutLine(raw=raw_line, text=text, indent=indent))
    return lines


def is_explicit_category_line(text: str) -> bool:
    lowered = text.lower()
    if re.match(r"^\[(.+?)\]$", text):
        return True
    if lowered.startswith("category:"):
        return True
    if text.endswith(":") and not lowered.startswith(("voice:", "text:")):
        return True
    return False


def is_explicit_channel_line(text: str) -> bool:
    lowered = text.lower()
    if lowered.startswith(("voice:", "text:")):
        return True
    if text.startswith("#"):
        return True
    if "|" in text:
        return True
    return False


def resolve_category_name(text: str) -> str:
    lowered = text.lower()
    if lowered.startswith("category:"):
        return clean_category_name(text.split(":", 1)[1])
    if re.match(r"^#{1,6}\s+.+$", text):
        return clean_category_name(re.sub(r"^#{1,6}\s+", "", text))
    if text.endswith(":"):
        return clean_category_name(text[:-1])
    return clean_category_name(text)


def looks_like_category(index: int, lines: list[LayoutLine]) -> bool:
    current = lines[index]
    text = current.text

    if re.match(r"^#{1,6}\s+.+$", text) and "|" not in text:
        next_line = lines[index + 1] if index + 1 < len(lines) else None
        heading_text = clean_category_name(re.sub(r"^#{1,6}\s+", "", text))
        if next_line is not None and heading_text:
            heading_style = mostly_upper(heading_text) or heading_text.istitle() or " " in heading_text
            next_channelish = is_explicit_channel_line(next_line.text) or bool(clean_channel_name(next_line.text))
            if heading_style and next_channelish:
                return True

    if is_explicit_category_line(text):
        return True
    if is_explicit_channel_line(text):
        return False

    next_line = lines[index + 1] if index + 1 < len(lines) else None
    if next_line is None:
        return False

    current_name = clean_category_name(text)
    next_text = next_line.text

    if not current_name:
        return False

    if next_line.indent > current.indent:
        return True

    if mostly_upper(text) and not mostly_upper(next_text):
        return True

    heading_style = mostly_upper(text) or text.istitle() or any(char.isupper() for char in text)
    if heading_style and next_line.indent >= current.indent and not is_explicit_category_line(next_text):
        next_channelish = is_explicit_channel_line(next_text) or bool(clean_channel_name(next_text))
        return next_channelish

    return False


def parse_layout(layout: str) -> list[ParsedChannel]:
    parsed: list[ParsedChannel] = []
    active_category: str | None = None

    lines = build_layout_lines(layout)

    for index, entry in enumerate(lines):
        line = entry.text

        if looks_like_category(index, lines):
            active_category = resolve_category_name(line)
            continue

        kind = "text"
        lowered = line.lower()
        if lowered.startswith("voice:"):
            kind = "voice"
            line = line.split(":", 1)[1].strip()
        elif lowered.startswith("text:"):
            line = line.split(":", 1)[1].strip()
        elif line.startswith("#"):
            line = line[1:].strip()

        if ">" in line and not line.lower().startswith(("voice:", "text:")):
            left, right = [part.strip() for part in line.split(">", 1)]
            left_category = clean_category_name(left)
            if left_category and right:
                active_category = left_category
                line = right

        topic = None
        if "|" in line:
            left, right = line.split("|", 1)
            line = left.strip()
            topic = right.strip()[:1024] if right.strip() else None

        name = clean_channel_name(line)
        if name:
            parsed.append(ParsedChannel(kind=kind, name=name, topic=topic, category=active_category))

    return parsed


def extract_retry_after(exc: discord.HTTPException, fallback: float) -> float:
    response = getattr(exc, "response", None)
    if response is not None:
        header = response.headers.get("Retry-After") or response.headers.get("X-RateLimit-Reset-After")
        try:
            return max(float(header), fallback)
        except (TypeError, ValueError):
            pass
    return fallback


async def throttled_discord_call(factory: Callable[[], Awaitable[T]]) -> T:
    last_error: discord.HTTPException | None = None

    for _ in range(ACTION_RETRY_LIMIT):
        try:
            result = await factory()
        except discord.HTTPException as exc:
            if exc.status != 429:
                raise
            last_error = exc
            await asyncio.sleep(extract_retry_after(exc, ACTION_DELAY_SECONDS or 1.0))
            continue

        if ACTION_DELAY_SECONDS > 0:
            await asyncio.sleep(ACTION_DELAY_SECONDS)
        return result

    if last_error is not None:
        raise last_error

    raise RuntimeError("Discord action failed before execution.")


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


def deserialize_overwrites(entries: list[dict[str, Any]], guild: discord.Guild) -> dict[Any, discord.PermissionOverwrite]:
    built: dict[Any, discord.PermissionOverwrite] = {}
    for entry in entries:
        allow = discord.Permissions(entry.get("allow", 0))
        deny = discord.Permissions(entry.get("deny", 0))
        overwrite = discord.PermissionOverwrite.from_pair(allow, deny)

        target = None
        if entry.get("target_type") == "role":
            target = guild.get_role(entry.get("target_id")) or discord.utils.get(guild.roles, name=entry.get("target_name"))
        elif entry.get("target_type") == "member":
            target = guild.get_member(entry.get("target_id"))

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


def serialize_settings(guild: discord.Guild) -> dict[str, Any]:
    return {
        "verification_level": int(guild.verification_level.value),
        "default_notifications": int(guild.default_notifications.value),
        "explicit_content_filter": int(guild.explicit_content_filter.value),
        "afk_timeout": guild.afk_timeout,
    }


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
    }

    if delete_channels and not create_only_missing:
        for channel in list(target.channels):
            try:
                await throttled_discord_call(lambda channel=channel: channel.delete(reason="CLINX backup load: delete channels"))
                result["deleted_channels"] += 1
            except discord.Forbidden:
                pass

    if delete_roles and not create_only_missing:
        for role in sorted(target.roles, key=lambda r: r.position, reverse=True):
            if role.managed or role.is_default():
                continue
            try:
                await throttled_discord_call(lambda role=role: role.delete(reason="CLINX backup load: delete roles"))
                result["deleted_roles"] += 1
            except discord.Forbidden:
                pass

    if load_roles and not create_only_missing:
        for role_data in sorted(snapshot.get("roles", []), key=lambda r: r.get("position", 0)):
            role = discord.utils.get(target.roles, name=role_data["name"])
            permissions = discord.Permissions(role_data.get("permissions", 0))
            color = discord.Colour(role_data.get("color", 0))

            if role is None:
                try:
                    await throttled_discord_call(
                        lambda role_data=role_data, permissions=permissions, color=color: target.create_role(
                            name=role_data["name"],
                            permissions=permissions,
                            colour=color,
                            hoist=role_data.get("hoist", False),
                            mentionable=role_data.get("mentionable", False),
                            reason="CLINX backup load: create role",
                        )
                    )
                    result["created_roles"] += 1
                except discord.Forbidden:
                    pass
            else:
                try:
                    await throttled_discord_call(
                        lambda role=role, permissions=permissions, color=color, role_data=role_data: role.edit(
                            permissions=permissions,
                            colour=color,
                            hoist=role_data.get("hoist", False),
                            mentionable=role_data.get("mentionable", False),
                            reason="CLINX backup load: update role",
                        )
                    )
                    result["updated_roles"] += 1
                except discord.Forbidden:
                    pass

    if load_settings and not create_only_missing:
        settings = snapshot.get("settings", {})
        try:
            await throttled_discord_call(
                lambda settings=settings: target.edit(
                    verification_level=discord.VerificationLevel(settings.get("verification_level", target.verification_level.value)),
                    default_notifications=discord.NotificationLevel(settings.get("default_notifications", target.default_notifications.value)),
                    explicit_content_filter=discord.ContentFilter(settings.get("explicit_content_filter", target.explicit_content_filter.value)),
                    afk_timeout=settings.get("afk_timeout", target.afk_timeout),
                    reason="CLINX backup load: update settings",
                )
            )
            result["updated_settings"] = 1
        except discord.Forbidden:
            pass

    if load_channels:
        category_map: dict[str, discord.CategoryChannel] = {}

        for cat_data in snapshot.get("categories", []):
            existing = discord.utils.get(target.categories, name=cat_data["name"])
            overwrites = deserialize_overwrites(cat_data.get("overwrites", []), target)

            if existing is None:
                try:
                    existing = await throttled_discord_call(
                        lambda cat_data=cat_data, overwrites=overwrites: target.create_category(
                            name=cat_data["name"],
                            overwrites=overwrites,
                        )
                    )
                    result["created_categories"] += 1
                except discord.Forbidden:
                    continue
            elif not create_only_missing:
                try:
                    await throttled_discord_call(
                        lambda existing=existing, overwrites=overwrites, cat_data=cat_data: existing.edit(
                            overwrites=overwrites,
                            position=cat_data.get("position", existing.position),
                        )
                    )
                except discord.Forbidden:
                    pass

            category_map[cat_data["name"]] = existing

        for ch_data in snapshot.get("channels", []):
            existing = discord.utils.get(target.channels, name=ch_data["name"])
            category = category_map.get(ch_data.get("category")) if ch_data.get("category") else None
            overwrites = deserialize_overwrites(ch_data.get("overwrites", []), target)

            if existing is None:
                try:
                    if ch_data["type"] == "text":
                        await throttled_discord_call(
                            lambda ch_data=ch_data, category=category, overwrites=overwrites: target.create_text_channel(
                                name=ch_data["name"],
                                category=category,
                                topic=ch_data.get("topic"),
                                slowmode_delay=ch_data.get("slowmode_delay", 0),
                                nsfw=ch_data.get("nsfw", False),
                                overwrites=overwrites,
                            )
                        )
                    elif ch_data["type"] == "voice":
                        await throttled_discord_call(
                            lambda ch_data=ch_data, category=category, overwrites=overwrites: target.create_voice_channel(
                                name=ch_data["name"],
                                category=category,
                                bitrate=ch_data.get("bitrate"),
                                user_limit=ch_data.get("user_limit", 0),
                                overwrites=overwrites,
                            )
                        )
                    result["created_channels"] += 1
                except discord.Forbidden:
                    pass
                continue

            if create_only_missing:
                continue

            try:
                if isinstance(existing, discord.TextChannel) and ch_data["type"] == "text":
                    await throttled_discord_call(
                        lambda existing=existing, category=category, ch_data=ch_data, overwrites=overwrites: existing.edit(
                            category=category,
                            topic=ch_data.get("topic"),
                            slowmode_delay=ch_data.get("slowmode_delay", 0),
                            nsfw=ch_data.get("nsfw", False),
                            overwrites=overwrites,
                        )
                    )
                    result["updated_channels"] += 1
                elif isinstance(existing, discord.VoiceChannel) and ch_data["type"] == "voice":
                    await throttled_discord_call(
                        lambda existing=existing, category=category, ch_data=ch_data, overwrites=overwrites: existing.edit(
                            category=category,
                            bitrate=ch_data.get("bitrate", existing.bitrate),
                            user_limit=ch_data.get("user_limit", 0),
                            overwrites=overwrites,
                        )
                    )
                    result["updated_channels"] += 1
            except discord.Forbidden:
                pass

    return result


async def create_mass_channels_from_layout(
    guild: discord.Guild,
    layout: str,
    *,
    create_categories: bool,
) -> dict[str, int]:
    items = parse_layout(layout)
    if not items:
        raise ValueError("No valid channels found in layout.")

    result = {
        "created_categories": 0,
        "created_channels": 0,
        "skipped_channels": 0,
    }

    category_cache: dict[str, discord.CategoryChannel] = {category.name: category for category in guild.categories}
    seen_channels: set[tuple[str | None, str]] = set()

    for item in items:
        category = None
        if item.category:
            category = category_cache.get(item.category)
            if category is None and create_categories:
                category = await throttled_discord_call(
                    lambda item=item: guild.create_category(item.category)
                )
                category_cache[item.category] = category
                result["created_categories"] += 1

        channel_key = (item.category, item.name)
        if channel_key in seen_channels:
            result["skipped_channels"] += 1
            continue
        seen_channels.add(channel_key)

        existing_channel = discord.utils.get(guild.channels, name=item.name)
        if existing_channel is not None:
            existing_category = existing_channel.category.name if isinstance(existing_channel, (discord.TextChannel, discord.VoiceChannel)) and existing_channel.category else None
            if existing_category == item.category or existing_category is None:
                result["skipped_channels"] += 1
                continue

        if item.kind == "voice":
            await throttled_discord_call(
                lambda item=item, category=category: guild.create_voice_channel(
                    name=item.name,
                    category=category,
                )
            )
        else:
            await throttled_discord_call(
                lambda item=item, category=category: guild.create_text_channel(
                    name=item.name,
                    category=category,
                    topic=item.topic,
                )
            )
        result["created_channels"] += 1

    return result


def build_backup_plan_preview(snapshot: dict[str, Any], target: discord.Guild, selected_actions: set[str]) -> dict[str, int]:
    target_roles = [role for role in target.roles if not role.managed and not role.is_default()]
    target_channels = list(target.channels)
    snapshot_roles = snapshot.get("roles", [])
    snapshot_categories = snapshot.get("categories", [])
    snapshot_channels = snapshot.get("channels", [])

    delete_roles_enabled = "delete_roles" in selected_actions
    delete_channels_enabled = "delete_channels" in selected_actions
    load_roles_enabled = "load_roles" in selected_actions
    load_channels_enabled = "load_channels" in selected_actions

    preview = {
        "deleted_roles": len(target_roles) if delete_roles_enabled else 0,
        "deleted_channels": len(target_channels) if delete_channels_enabled else 0,
        "created_roles": 0,
        "updated_roles": 0,
        "created_categories": 0,
        "created_channels": 0,
        "updated_channels": 0,
        "updated_settings": 1 if "load_settings" in selected_actions else 0,
        "conflicting_channels": 0,
    }

    if load_roles_enabled:
        if delete_roles_enabled:
            preview["created_roles"] = len(snapshot_roles)
        else:
            for role_data in snapshot_roles:
                existing = discord.utils.get(target.roles, name=role_data["name"])
                if existing is None:
                    preview["created_roles"] += 1
                else:
                    preview["updated_roles"] += 1

    if load_channels_enabled:
        if delete_channels_enabled:
            preview["created_categories"] = len(snapshot_categories)
            preview["created_channels"] = len(snapshot_channels)
        else:
            existing_categories = {category.name for category in target.categories}
            existing_channels = {channel.name: channel for channel in target.channels}

            for category_data in snapshot_categories:
                if category_data["name"] not in existing_categories:
                    preview["created_categories"] += 1

            for channel_data in snapshot_channels:
                existing = existing_channels.get(channel_data["name"])
                if existing is None:
                    preview["created_channels"] += 1
                elif str(existing.type) == channel_data["type"]:
                    preview["updated_channels"] += 1
                else:
                    preview["conflicting_channels"] += 1

    return preview


def render_backup_plan_lines(plan: dict[str, int]) -> str:
    lines: list[str] = []
    label_map = {
        "deleted_roles": "roles will be deleted",
        "created_roles": "roles will be created",
        "updated_roles": "roles will be updated",
        "deleted_channels": "channels will be deleted",
        "created_categories": "categories will be created",
        "created_channels": "channels will be created",
        "updated_channels": "channels will be updated",
        "updated_settings": "server settings will be updated",
        "conflicting_channels": "channel names have type conflicts",
    }

    for key in (
        "deleted_roles",
        "created_roles",
        "updated_roles",
        "deleted_channels",
        "created_categories",
        "created_channels",
        "updated_channels",
        "updated_settings",
        "conflicting_channels",
    ):
        value = plan.get(key, 0)
        if value:
            lines.append(f"- **{value}** {label_map[key]}")

    if not lines:
        return "- No changes are currently selected."

    return "\n".join(lines)


def render_backup_detail_lines(snapshot: dict[str, Any], target: discord.Guild, selected_actions: set[str], plan: dict[str, int]) -> str:
    selected = ", ".join(BACKUP_ACTION_LABELS[action] for action in BACKUP_ACTION_ORDER if action in selected_actions) or "No actions selected"
    detail_lines = [
        f"**Backup Source**\n`{snapshot.get('source_guild_name', 'Unknown Source')}`",
        f"**Target Server**\n`{target.name}`",
        f"**Selected Actions**\n{selected}",
        f"**Snapshot Objects**\nRoles `{len(snapshot.get('roles', []))}` | Categories `{len(snapshot.get('categories', []))}` | Channels `{len(snapshot.get('channels', []))}`",
    ]
    if plan.get("conflicting_channels"):
        detail_lines.append("**Heads Up**\nConflicting channel types were detected. Names that already exist with a different type will not auto-convert.")
    return "\n\n".join(detail_lines)


class BackupLoadActionButton(discord.ui.Button["BackupLoadPlannerView"]):
    def __init__(
        self,
        *,
        backup_id: str,
        source_name: str,
        author_id: int,
        snapshot: dict[str, Any],
        target: discord.Guild,
        selected_actions: set[str],
        detail_mode: bool,
        action_key: str,
    ) -> None:
        is_selected = action_key in selected_actions
        is_destructive = action_key.startswith("delete_")
        style = discord.ButtonStyle.secondary
        if is_selected and is_destructive:
            style = discord.ButtonStyle.danger
        elif is_selected:
            style = discord.ButtonStyle.primary

        row = 1 if action_key in {"delete_roles", "delete_channels", "load_roles"} else 2
        super().__init__(label=BACKUP_ACTION_LABELS[action_key], style=style, row=row)
        self.backup_id = backup_id
        self.source_name = source_name
        self.author_id = author_id
        self.snapshot = snapshot
        self.target = target
        self.selected_actions = set(selected_actions)
        self.detail_mode = detail_mode
        self.action_key = action_key

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the command user can use these controls.", ephemeral=True)
            return

        next_actions = set(self.selected_actions)
        if self.action_key in next_actions:
            next_actions.remove(self.action_key)
        else:
            next_actions.add(self.action_key)

        await interaction.response.edit_message(
            view=BackupLoadPlannerView(
                backup_id=self.backup_id,
                source_name=self.source_name,
                author_id=self.author_id,
                snapshot=self.snapshot,
                target=self.target,
                selected_actions=next_actions,
                detail_mode=self.detail_mode,
            )
        )


class BackupLoadDetailButton(discord.ui.Button["BackupLoadPlannerView"]):
    def __init__(
        self,
        *,
        backup_id: str,
        source_name: str,
        author_id: int,
        snapshot: dict[str, Any],
        target: discord.Guild,
        selected_actions: set[str],
        detail_mode: bool,
    ) -> None:
        super().__init__(
            label="Hide Detail" if detail_mode else "View Detail",
            style=discord.ButtonStyle.secondary,
            row=2,
        )
        self.backup_id = backup_id
        self.source_name = source_name
        self.author_id = author_id
        self.snapshot = snapshot
        self.target = target
        self.selected_actions = set(selected_actions)
        self.detail_mode = detail_mode

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the command user can use these controls.", ephemeral=True)
            return

        await interaction.response.edit_message(
            view=BackupLoadPlannerView(
                backup_id=self.backup_id,
                source_name=self.source_name,
                author_id=self.author_id,
                snapshot=self.snapshot,
                target=self.target,
                selected_actions=self.selected_actions,
                detail_mode=not self.detail_mode,
            )
        )


class BackupLoadContinueButton(discord.ui.Button["BackupLoadPlannerView"]):
    def __init__(
        self,
        *,
        backup_id: str,
        source_name: str,
        author_id: int,
        snapshot: dict[str, Any],
        target: discord.Guild,
        selected_actions: set[str],
    ) -> None:
        super().__init__(
            label="Continue",
            style=discord.ButtonStyle.success,
            disabled=not selected_actions,
            row=3,
        )
        self.backup_id = backup_id
        self.source_name = source_name
        self.author_id = author_id
        self.snapshot = snapshot
        self.target = target
        self.selected_actions = set(selected_actions)

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the command user can use these controls.", ephemeral=True)
            return

        await interaction.response.edit_message(
            view=BackupLoadStatusView(
                title="## `!` Applying Backup",
                subtitle=f"Backup `{self.backup_id}` is now being applied to `{self.target.name}`.",
                body="CLINX is pacing every action to avoid slamming Discord while roles, channels, and settings are updated.",
                accent_color=LIBRARY_ACCENT,
            )
        )

        try:
            stats = await apply_snapshot_to_guild(
                self.snapshot,
                self.target,
                delete_roles="delete_roles" in self.selected_actions,
                delete_channels="delete_channels" in self.selected_actions,
                load_roles="load_roles" in self.selected_actions,
                load_channels="load_channels" in self.selected_actions,
                load_settings="load_settings" in self.selected_actions,
                create_only_missing=False,
            )
        except Exception as exc:
            await interaction.edit_original_response(
                view=BackupLoadStatusView(
                    title="## `x` Backup Load Failed",
                    subtitle=f"Backup `{self.backup_id}` did not finish cleanly.",
                    body=f"`{exc}`",
                    accent_color=EMBED_ERR,
                )
            )
            return

        result_body = (
            f"- **{stats['deleted_roles']}** roles deleted\n"
            f"- **{stats['created_roles']}** roles created\n"
            f"- **{stats['updated_roles']}** roles updated\n"
            f"- **{stats['deleted_channels']}** channels deleted\n"
            f"- **{stats['created_categories']}** categories created\n"
            f"- **{stats['created_channels']}** channels created\n"
            f"- **{stats['updated_channels']}** channels updated\n"
            f"- **{stats['updated_settings']}** settings updated"
        )
        await interaction.edit_original_response(
            view=BackupLoadStatusView(
                title="## `<>` Backup Load Complete",
                subtitle=f"Backup `{self.backup_id}` finished for `{self.target.name}`.",
                body=result_body,
                accent_color=EMBED_OK,
            )
        )


class BackupLoadCancelButton(discord.ui.Button["BackupLoadPlannerView"]):
    def __init__(self) -> None:
        super().__init__(label="Cancel", style=discord.ButtonStyle.danger, row=3)

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.edit_message(
            view=BackupLoadStatusView(
                title="## `-` Backup Load Cancelled",
                subtitle="No changes were applied.",
                body="Re-run `/backup load` whenever you want to reopen the planner.",
                accent_color=EMBED_WARN,
            )
        )


class BackupLoadStatusView(discord.ui.LayoutView):
    def __init__(self, *, title: str, subtitle: str, body: str, accent_color: int) -> None:
        super().__init__(timeout=300)
        self.add_item(
            discord.ui.Container(
                discord.ui.TextDisplay(title),
                discord.ui.TextDisplay(subtitle),
                discord.ui.Separator(),
                discord.ui.TextDisplay(body),
                accent_color=accent_color,
            )
        )


class BackupLoadPlannerView(discord.ui.LayoutView):
    def __init__(
        self,
        *,
        backup_id: str,
        source_name: str,
        author_id: int,
        snapshot: dict[str, Any],
        target: discord.Guild,
        selected_actions: set[str] | None = None,
        detail_mode: bool = False,
    ) -> None:
        super().__init__(timeout=300)
        self.backup_id = backup_id
        self.source_name = source_name
        self.author_id = author_id
        self.snapshot = snapshot
        self.target = target
        self.selected_actions = set(selected_actions or {"load_roles", "load_channels", "load_settings"})
        self.detail_mode = detail_mode

        plan = build_backup_plan_preview(snapshot, target, self.selected_actions)
        selected_count = len(self.selected_actions)
        selected_labels = ", ".join(BACKUP_ACTION_LABELS[action] for action in BACKUP_ACTION_ORDER if action in self.selected_actions) or "No actions selected"

        self.add_item(
            discord.ui.Container(
                discord.ui.TextDisplay("## `!` Backup Load Planner"),
                discord.ui.TextDisplay(
                    f"Backup `{backup_id}` from `{source_name}` is staged for `{target.name}`. Review the impact below before you continue."
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay(f"**Selected Actions**\n{selected_labels}"),
                    discord.ui.TextDisplay(f"**Impact Preview**\n{render_backup_plan_lines(plan)}"),
                    accessory=discord.ui.Button(
                        label=f"{selected_count} active",
                        style=discord.ButtonStyle.secondary,
                        disabled=True,
                    ),
                ),
                discord.ui.Separator(),
                discord.ui.TextDisplay(
                    render_backup_detail_lines(snapshot, target, self.selected_actions, plan)
                    if self.detail_mode
                    else "Toggle `View Detail` to inspect the source, target, and conflict notes before you run the load."
                ),
                accent_color=BACKUP_PLANNER_ACCENT,
            )
        )

        for action_key in BACKUP_ACTION_ORDER:
            self.add_item(
                BackupLoadActionButton(
                    backup_id=backup_id,
                    source_name=source_name,
                    author_id=author_id,
                    snapshot=snapshot,
                    target=target,
                    selected_actions=self.selected_actions,
                    detail_mode=detail_mode,
                    action_key=action_key,
                )
            )

        self.add_item(
            BackupLoadDetailButton(
                backup_id=backup_id,
                source_name=source_name,
                author_id=author_id,
                snapshot=snapshot,
                target=target,
                selected_actions=self.selected_actions,
                detail_mode=detail_mode,
            )
        )
        self.add_item(
            BackupLoadContinueButton(
                backup_id=backup_id,
                source_name=source_name,
                author_id=author_id,
                snapshot=snapshot,
                target=target,
                selected_actions=self.selected_actions,
            )
        )
        self.add_item(BackupLoadCancelButton())


intents = discord.Intents.default()
intents.guilds = True
intents.members = True


class ClinxBot(commands.Bot):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._groups_added = False
        self._startup_synced = False
        self._persistent_views_added = False

    async def setup_hook(self) -> None:
        if not self._groups_added:
            self.tree.add_command(backup_group)
            self.tree.add_command(export_group)
            self.tree.add_command(import_group)
            self.tree.add_command(panel_group)
            self._groups_added = True

        if not self._persistent_views_added:
            self.add_view(SuggestionBoardView())
            self._persistent_views_added = True

        if not self._startup_synced:
            synced = await self.tree.sync()
            self._startup_synced = True
            print(f"Synced {len(synced)} slash commands")


bot = ClinxBot(command_prefix=commands.when_mentioned, intents=intents)

backup_group = app_commands.Group(name="backup", description="Backup and restore commands")
export_group = app_commands.Group(name="export", description="Export server objects")
import_group = app_commands.Group(name="import", description="Import server objects")
panel_group = app_commands.Group(name="panel", description="Send styled embed panels")


@bot.event
async def on_ready() -> None:
    print(f"Logged in as {bot.user} ({bot.user.id})")


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
    await interaction.response.defer(ephemeral=True, thinking=True)

    source = interaction.guild if source_guild_id is None else bot.get_guild(source_guild_id)
    if source is None:
        await interaction.followup.send(embed=make_embed("Error", "Source guild not found.", EMBED_ERR), ephemeral=True)
        return

    store = load_backup_store()
    backup_id = f"BKP-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{secrets.token_hex(3).upper()}"
    store["backups"][backup_id] = {
        "id": backup_id,
        "created_at": utc_now_iso(),
        "created_by_user_id": interaction.user.id,
        "created_by_display_name": str(interaction.user),
        "source_guild_id": source.id,
        "source_guild_name": source.name,
        "snapshot": serialize_guild_snapshot(source),
    }
    register_backup_owner(
        store,
        backup_id=backup_id,
        user_id=interaction.user.id,
        display_name=str(interaction.user),
    )
    save_backup_store(store)

    await interaction.followup.send(
        embed=make_embed(
            "Backup Created",
            f"Load ID: `{backup_id}`\nSource: `{source.name}` ({source.id})",
            EMBED_OK,
        ),
        ephemeral=True,
    )


@backup_group.command(name="load", description="Load backup by ID with action selection")
@app_commands.describe(load_id="Backup load ID", target_guild_id="Target guild ID (optional)")
@app_commands.default_permissions(administrator=True)
async def backup_load(interaction: discord.Interaction, load_id: str, target_guild_id: int | None = None) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)

    store = load_backup_store()
    record = store.get("backups", {}).get(load_id)
    if record is None:
        await interaction.followup.send(embed=make_embed("Invalid Load ID", "No backup found with that ID.", EMBED_ERR), ephemeral=True)
        return
    if record.get("created_by_user_id") != interaction.user.id:
        await interaction.followup.send(
            embed=make_embed("Access Denied", "You can only load backups created by your account.", EMBED_ERR),
            ephemeral=True,
        )
        return

    target = interaction.guild if target_guild_id is None else bot.get_guild(target_guild_id)
    if target is None:
        await interaction.followup.send(embed=make_embed("Error", "Target guild not found.", EMBED_ERR), ephemeral=True)
        return

    view = BackupLoadPlannerView(
        backup_id=load_id,
        source_name=record.get("source_guild_name", "Unknown Source"),
        author_id=interaction.user.id,
        snapshot=record["snapshot"],
        target=target,
    )
    await interaction.followup.send(view=view, ephemeral=True)


@backup_group.command(name="list", description="List saved backup IDs")
@app_commands.default_permissions(administrator=True)
async def backup_list(interaction: discord.Interaction) -> None:
    store = load_backup_store()
    entries = get_user_backup_records(store, interaction.user.id)[:20]
    if not entries:
        await interaction.response.send_message(embed=make_embed("Backups", "You have not created any backups yet.", EMBED_INFO), ephemeral=True)
        return

    lines = []
    for entry in entries:
        source_name = entry.get("source_guild_name", "unknown")
        created_at = format_backup_timestamp(entry)
        lines.append(f"`{entry['id']}`\n`{source_name}` • `{created_at}`")
    await interaction.response.send_message(embed=make_embed("Backups", "\n".join(lines), EMBED_INFO), ephemeral=True)


@backup_group.command(name="delete", description="Delete a backup ID")
@app_commands.describe(load_id="Backup load ID")
@app_commands.default_permissions(administrator=True)
async def backup_delete(interaction: discord.Interaction, load_id: str) -> None:
    store = load_backup_store()
    record = store.get("backups", {}).get(load_id)
    if record is None:
        await interaction.response.send_message(embed=make_embed("Error", "Load ID not found.", EMBED_ERR), ephemeral=True)
        return
    if record.get("created_by_user_id") != interaction.user.id:
        await interaction.response.send_message(
            embed=make_embed("Access Denied", "You can only delete backups created by your account.", EMBED_ERR),
            ephemeral=True,
        )
        return

    unregister_backup_owner(
        store,
        backup_id=load_id,
        user_id=record.get("created_by_user_id"),
    )
    del store["backups"][load_id]
    save_backup_store(store)
    await interaction.response.send_message(embed=make_embed("Deleted", f"Removed `{load_id}`.", EMBED_OK), ephemeral=True)


@backup_load.autocomplete("load_id")
async def backup_load_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    return await user_backup_id_autocomplete(interaction, current)


@backup_delete.autocomplete("load_id")
async def backup_delete_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    return await user_backup_id_autocomplete(interaction, current)


@bot.tree.command(name="backupcreate", description="Alias of /backup create")
@app_commands.default_permissions(administrator=True)
async def backupcreate_alias(interaction: discord.Interaction, source_guild_id: int | None = None) -> None:
    await backup_create.callback(interaction, source_guild_id)


@bot.tree.command(name="backupload", description="Alias of /backup load")
@app_commands.default_permissions(administrator=True)
async def backupload_alias(interaction: discord.Interaction, load_id: str, target_guild_id: int | None = None) -> None:
    await backup_load.callback(interaction, load_id, target_guild_id)


@backupload_alias.autocomplete("load_id")
async def backupload_alias_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    return await user_backup_id_autocomplete(interaction, current)


@bot.tree.command(name="backuplist", description="Alias of /backup list")
@app_commands.default_permissions(administrator=True)
async def backuplist_alias(interaction: discord.Interaction) -> None:
    await backup_list.callback(interaction)


@bot.tree.command(name="restore_missing", description="Restore only missing categories/channels from source")
@app_commands.describe(source_guild_id="Source guild ID", target_guild_id="Target guild ID")
@app_commands.default_permissions(administrator=True)
async def restore_missing(interaction: discord.Interaction, source_guild_id: int | None = None, target_guild_id: int | None = None) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)

    resolved_source = source_guild_id or resolve_default_backup_guild_id()
    source = bot.get_guild(resolved_source) if resolved_source else None
    target = interaction.guild if target_guild_id is None else bot.get_guild(target_guild_id)

    if source is None or target is None:
        await interaction.followup.send(embed=make_embed("Error", "Could not resolve source or target guild.", EMBED_ERR), ephemeral=True)
        return

    snapshot = serialize_guild_snapshot(source)
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


@bot.tree.command(name="cleantoday", description="Delete channels created today (UTC)")
@app_commands.default_permissions(administrator=True)
async def cleantoday(interaction: discord.Interaction, confirm: bool = False) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)

    guild = interaction.guild
    if guild is None:
        await interaction.followup.send(embed=make_embed("Error", "Run this command in a server.", EMBED_ERR), ephemeral=True)
        return

    today = datetime.now(timezone.utc).date()
    targets = [ch for ch in guild.channels if getattr(ch, "created_at", None) and ch.created_at.date() == today]

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
            await throttled_discord_call(
                lambda ch=ch: ch.delete(reason=f"/cleantoday by {interaction.user}")
            )
            deleted += 1
        except discord.Forbidden:
            pass

    await interaction.followup.send(embed=make_embed("Clean Today Complete", f"Deleted `{deleted}` channels.", EMBED_OK), ephemeral=True)


class MassChannelsModal(discord.ui.Modal, title="Mass Channel Creator"):
    layout_input = discord.ui.TextInput(
        label="Layout",
        placeholder="[Welcome]\nrules | Read before chatting\nannouncements\n\nGeneral:\nchat\nmedia | Image drops\nvoice: Hangout",
        style=discord.TextStyle.paragraph,
        max_length=4000,
    )
    extra_layout_input = discord.ui.TextInput(
        label="More Layout (optional)",
        placeholder="Paste the next chunk here if your layout is long.",
        style=discord.TextStyle.paragraph,
        max_length=4000,
        required=False,
    )

    def __init__(self, create_categories: bool) -> None:
        super().__init__()
        self.create_categories = create_categories

    async def on_submit(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(embed=make_embed("Error", "Run this command in a server.", EMBED_ERR), ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        layout = self.layout_input.value
        if self.extra_layout_input.value:
            layout = f"{layout}\n{self.extra_layout_input.value}"

        try:
            stats = await create_mass_channels_from_layout(
                guild,
                layout,
                create_categories=self.create_categories,
            )
        except ValueError as exc:
            await interaction.followup.send(embed=make_embed("Mass Channels", str(exc), EMBED_ERR), ephemeral=True)
            return

        summary = (
            f"Created categories: `{stats['created_categories']}`\n"
            f"Created channels: `{stats['created_channels']}`\n"
            f"Skipped existing/duplicates: `{stats['skipped_channels']}`"
        )
        await interaction.followup.send(embed=make_embed("Mass Create Complete", summary, EMBED_OK), ephemeral=True)


@bot.tree.command(name="masschannels", description="Open the bulk channel creator modal")
@app_commands.default_permissions(administrator=True)
async def masschannels(interaction: discord.Interaction, create_categories: bool = True) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run this command in a server.", EMBED_ERR), ephemeral=True)
        return

    await interaction.response.send_modal(MassChannelsModal(create_categories=create_categories))

class FeedbackModal(discord.ui.Modal):
    def __init__(self, feedback_kind: str) -> None:
        super().__init__(title=f"{feedback_kind} Intake")
        self.feedback_kind = feedback_kind
        self.title_input = discord.ui.TextInput(
            label="Title",
            placeholder=f"{feedback_kind} title",
            max_length=100,
        )
        self.details_input = discord.ui.TextInput(
            label="Details",
            placeholder="Write the exact idea, problem, or request here.",
            style=discord.TextStyle.paragraph,
            max_length=1800,
        )
        self.add_item(self.title_input)
        self.add_item(self.details_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        channel = interaction.channel
        if channel is None:
            await interaction.response.send_message("Channel not available for feedback posting.", ephemeral=True)
            return

        embed = discord.Embed(
            title=f"{self.feedback_kind} Intake",
            description=self.details_input.value,
            color=SUGGESTION_ACCENT,
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Title", value=self.title_input.value, inline=False)
        embed.add_field(name="Submitted By", value=interaction.user.mention, inline=True)
        if interaction.guild is not None:
            embed.add_field(name="Server", value=interaction.guild.name, inline=True)
        embed.set_footer(text="CLINX Feedback Stream")

        await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        await interaction.response.send_message(f"{self.feedback_kind} sent to this channel.", ephemeral=True)


class SuggestionBoardLaunchButton(discord.ui.Button["SuggestionBoardView"]):
    def __init__(self, feedback_kind: str, *, label: str, style: discord.ButtonStyle, custom_id: str, emoji: str | None = None) -> None:
        super().__init__(
            label=label,
            style=style,
            custom_id=custom_id,
            emoji=emoji,
            row=1,
        )
        self.feedback_kind = feedback_kind

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_modal(FeedbackModal(self.feedback_kind))


class SuggestionBoardView(discord.ui.LayoutView):
    def __init__(self) -> None:
        super().__init__(timeout=None)
        board = discord.ui.Container(
            discord.ui.TextDisplay("## `<>` CLINX Suggestion Board"),
            discord.ui.TextDisplay("Drop polished ideas, bug reports, and upgrade requests straight into the current channel."),
            discord.ui.Separator(),
            discord.ui.Section(
                discord.ui.TextDisplay("**How it works**\nUse the buttons below to open the right intake form."),
                discord.ui.TextDisplay("**Keep it sharp**\nOne request per submission, include context, and explain the expected result."),
                accessory=discord.ui.Button(
                    label="Live Intake",
                    style=discord.ButtonStyle.success,
                    disabled=True,
                    custom_id="suggestion-board-badge",
                ),
            ),
            discord.ui.Separator(),
            discord.ui.TextDisplay("`Suggestion` is for features, flows, or UX upgrades.\n`Bug Report` is for broken commands, wrong output, or failing restores."),
            accent_color=SUGGESTION_ACCENT,
        )
        self.add_item(board)
        self.add_item(
            SuggestionBoardLaunchButton(
                "Suggestion",
                label="Submit Suggestion",
                style=discord.ButtonStyle.primary,
                custom_id="suggestion-board-open-suggestion",
                emoji="💡",
            )
        )
        self.add_item(
            SuggestionBoardLaunchButton(
                "Bug Report",
                label="Report Bug",
                style=discord.ButtonStyle.secondary,
                custom_id="suggestion-board-open-bug",
                emoji="🛠️",
            )
        )
        self.add_item(
            discord.ui.Button(
                label="Support",
                style=discord.ButtonStyle.link,
                url=SUPPORT_SERVER_URL,
                row=1,
            )
        )


def get_command_entries(category: str) -> list[LibraryCommand]:
    return COMMAND_LIBRARY[category]


def get_command_page_count(category: str) -> int:
    entries = get_command_entries(category)
    return max(1, (len(entries) + COMMAND_PAGE_SIZE - 1) // COMMAND_PAGE_SIZE)


class LibraryCategoryButton(discord.ui.Button["CommandLibraryView"]):
    def __init__(self, category: str, current_category: str) -> None:
        super().__init__(
            label=category,
            style=discord.ButtonStyle.primary if category == current_category else discord.ButtonStyle.secondary,
            row=1,
        )
        self.category = category

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.edit_message(view=CommandLibraryView(category=self.category))


class LibraryPageButton(discord.ui.Button["CommandLibraryView"]):
    def __init__(self, *, direction: int, category: str, page: int, selected_command: str | None) -> None:
        target_page = page + direction
        max_page = get_command_page_count(category) - 1
        super().__init__(
            label="Prev" if direction < 0 else "Next",
            style=discord.ButtonStyle.secondary,
            disabled=target_page < 0 or target_page > max_page,
            row=3,
        )
        self.direction = direction
        self.category = category
        self.page = page
        self.selected_command = selected_command

    async def callback(self, interaction: discord.Interaction) -> None:
        next_page = self.page + self.direction
        max_page = get_command_page_count(self.category) - 1
        next_page = max(0, min(next_page, max_page))
        await interaction.response.edit_message(
            view=CommandLibraryView(
                category=self.category,
                page=next_page,
                selected_command=None,
            )
        )


class LibraryCommandSelect(discord.ui.Select["CommandLibraryView"]):
    def __init__(self, category: str, page: int, selected_command: str | None) -> None:
        entries = get_command_entries(category)
        start = page * COMMAND_PAGE_SIZE
        current_page_entries = entries[start : start + COMMAND_PAGE_SIZE]
        options = [
            discord.SelectOption(
                label=f"/{entry.name}",
                description=entry.summary[:100],
                value=entry.name,
                default=entry.name == selected_command,
            )
            for entry in current_page_entries
        ]
        super().__init__(
            placeholder="Select a command for details...",
            options=options,
            row=2,
        )
        self.category = category
        self.page = page

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.edit_message(
            view=CommandLibraryView(
                category=self.category,
                page=self.page,
                selected_command=self.values[0],
            )
        )


class CommandLibraryView(discord.ui.LayoutView):
    def __init__(self, *, category: str = "Backup", page: int = 0, selected_command: str | None = None) -> None:
        super().__init__(timeout=900)
        categories = list(COMMAND_LIBRARY.keys())
        if category not in COMMAND_LIBRARY:
            category = categories[0]

        max_page = get_command_page_count(category) - 1
        page = max(0, min(page, max_page))
        self.category = category
        self.page = page
        self.selected_command = selected_command

        self.add_item(
            discord.ui.Container(
                discord.ui.TextDisplay("## `<>` Command Library"),
                discord.ui.TextDisplay(
                    f"Explore **{sum(len(entries) for entries in COMMAND_LIBRARY.values())}** CLINX commands with live categories, paging, and deeper command notes."
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay(f"**Lane**\n`{self.category}`"),
                    discord.ui.TextDisplay(f"**Page**\n`{self.page + 1}/{get_command_page_count(self.category)}`"),
                    accessory=discord.ui.Button(
                        label=f"{len(get_command_entries(self.category))} commands",
                        style=discord.ButtonStyle.secondary,
                        disabled=True,
                    ),
                ),
                discord.ui.Separator(),
                discord.ui.TextDisplay(self.render_page_block()),
                discord.ui.Separator(),
                discord.ui.TextDisplay(self.render_detail_block()),
                accent_color=LIBRARY_ACCENT,
            )
        )

        for category_name in categories:
            self.add_item(LibraryCategoryButton(category_name, self.category))

        self.add_item(LibraryCommandSelect(self.category, self.page, self.selected_command))
        self.add_item(
            LibraryPageButton(
                direction=-1,
                category=self.category,
                page=self.page,
                selected_command=self.selected_command,
            )
        )
        self.add_item(
            LibraryPageButton(
                direction=1,
                category=self.category,
                page=self.page,
                selected_command=self.selected_command,
            )
        )
        self.add_item(
            discord.ui.Button(
                label="Invite",
                style=discord.ButtonStyle.link,
                url=build_invite_link(),
                row=3,
            )
        )
        self.add_item(
            discord.ui.Button(
                label="Support",
                style=discord.ButtonStyle.link,
                url=SUPPORT_SERVER_URL,
                row=3,
            )
        )

    def render_page_block(self) -> str:
        entries = get_command_entries(self.category)
        start = self.page * COMMAND_PAGE_SIZE
        current_page_entries = entries[start : start + COMMAND_PAGE_SIZE]
        lines = [f"- `/{entry.name}` - {entry.summary}" for entry in current_page_entries]
        return "\n".join(lines)

    def render_detail_block(self) -> str:
        if not self.selected_command:
            return "Select a command from the menu below to inspect what it does and where it fits in the workflow."

        for entry in get_command_entries(self.category):
            if entry.name == self.selected_command:
                return f"**/{entry.name}**\n{entry.detail}"

        return "Select a command from the menu below to inspect what it does and where it fits in the workflow."


@panel_group.command(name="suggestion", description="Post the public suggestion board")
async def panel_suggestion(interaction: discord.Interaction) -> None:
    channel = interaction.channel
    if channel is None:
        await interaction.response.send_message("Channel not available.", ephemeral=True)
        return

    await channel.send(
        view=SuggestionBoardView(),
        allowed_mentions=discord.AllowedMentions.none(),
    )
    await interaction.response.send_message("Suggestion board posted in this channel.", ephemeral=True)
async def send_text_file(interaction: discord.Interaction, text: str, filename: str) -> None:
    data = io.BytesIO(text.encode("utf-8"))
    await interaction.response.send_message(file=discord.File(data, filename=filename), ephemeral=True)


@export_group.command(name="guild", description="Export full guild snapshot as JSON")
async def export_guild(interaction: discord.Interaction) -> None:
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    snapshot = serialize_guild_snapshot(guild)
    await send_text_file(interaction, json.dumps(snapshot, indent=2), f"guild_{guild.id}.json")


@export_group.command(name="channels", description="Export channels as JSON or CSV")
@app_commands.choices(fmt=[app_commands.Choice(name="json", value="json"), app_commands.Choice(name="csv", value="csv")])
async def export_channels(interaction: discord.Interaction, fmt: app_commands.Choice[str]) -> None:
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

    await interaction.response.defer(ephemeral=True, thinking=True)

    try:
        payload = await file.read()
        snapshot = json.loads(payload.decode("utf-8"))
    except Exception:
        await interaction.followup.send(embed=make_embed("Error", "Invalid JSON file.", EMBED_ERR), ephemeral=True)
        return

    if interaction.guild.id in IMPORT_JOBS and IMPORT_JOBS[interaction.guild.id].get("status") == "running":
        await interaction.followup.send(embed=make_embed("Busy", "An import is already running.", EMBED_WARN), ephemeral=True)
        return

    task = asyncio.create_task(run_import_job(interaction.guild, snapshot))
    IMPORT_JOBS[interaction.guild.id] = {"status": "running", "started_at": utc_now_iso(), "task": task}
    await interaction.followup.send(embed=make_embed("Import Started", "Use `/import status` to track progress.", EMBED_INFO), ephemeral=True)


@import_group.command(name="status", description="Get current import status")
@app_commands.default_permissions(administrator=True)
async def import_status(interaction: discord.Interaction) -> None:
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


@bot.tree.command(name="help", description="Get command help")
async def help_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.send_message(view=CommandLibraryView())


@bot.tree.command(name="invite", description="Get bot invite link")
async def invite(interaction: discord.Interaction) -> None:
    link = build_invite_link()
    await interaction.response.send_message(embed=make_embed("Invite CLINX", link, EMBED_INFO), ephemeral=True)


@bot.tree.command(name="leave", description="Make bot leave this server")
@app_commands.default_permissions(administrator=True)
async def leave(interaction: discord.Interaction) -> None:
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    await interaction.response.send_message(embed=make_embed("Leaving", "CLINX is leaving this server.", EMBED_WARN), ephemeral=True)
    await guild.leave()


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








