import asyncio
import base64
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
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands

TOKEN = os.getenv("BOT_TOKEN")
DEFAULT_BACKUP_GUILD_ID = os.getenv("DEFAULT_BACKUP_GUILD_ID")
SUPPORT_URL = "https://discord.gg/V6YEw2Wxcb"

DATA_DIR = Path(__file__).parent / "data"
BACKUP_FILE = DATA_DIR / "backups.json"

EMBED_OK = 0x2ECC71
EMBED_WARN = 0xF1C40F
EMBED_ERR = 0xE74C3C
EMBED_INFO = 0x3498DB

IMPORT_JOBS: dict[int, dict[str, Any]] = {}
BACKUP_LOAD_JOBS: dict[int, dict[str, Any]] = {}


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
    if not BACKUP_FILE.exists():
        BACKUP_FILE.write_text(json.dumps({"backups": {}, "users": {}}, indent=2), encoding="utf-8")


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


def format_backup_timestamp(created_at: str | None) -> str:
    if not created_at:
        return "Unknown time"
    try:
        dt = datetime.fromisoformat(created_at)
    except ValueError:
        return "Unknown time"
    return dt.astimezone(timezone.utc).strftime("%d %b %Y - %H:%M UTC")


def get_user_backup_entries(store: dict[str, Any], user_id: int) -> list[dict[str, Any]]:
    owner_id = str(user_id)
    backup_ids = store.get("users", {}).get(owner_id, {}).get("backup_ids", [])
    entries = [store["backups"][backup_id] for backup_id in backup_ids if backup_id in store.get("backups", {})]
    return sorted(entries, key=lambda entry: entry.get("created_at", ""), reverse=True)


def can_access_backup(record: dict[str, Any], user_id: int) -> bool:
    return str(record.get("created_by_user_id", "")) == str(user_id)


def build_backup_choice_label(record: dict[str, Any]) -> str:
    source_name = record.get("source_guild_name", "Unknown Source")
    timestamp = format_backup_timestamp(record.get("created_at"))
    label = f"{source_name} | {timestamp} ({record.get('id', 'unknown')})"
    return label[:100]


async def backup_id_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    store = load_backup_store()
    entries = get_user_backup_entries(store, interaction.user.id)
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
    if job.get("error"):
        desc += f"\nError: `{job['error']}`"
    return desc


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
    precreated_categories: dict[str, discord.CategoryChannel] = {}
    precreated_channels: dict[str, discord.abc.GuildChannel] = {}

    if delete_channels and not create_only_missing:
        for channel in list(target.channels):
            try:
                await channel.delete(reason="CLINX backup load: delete channels")
                result["deleted_channels"] += 1
            except discord.Forbidden:
                pass

    if load_channels and not create_only_missing:
        structure_category_map: dict[str, discord.CategoryChannel] = (
            {} if delete_channels else {category.name: category for category in target.categories}
        )

        for cat_data in snapshot.get("categories", []):
            if cat_data["name"] in structure_category_map:
                continue
            try:
                created_category = await target.create_category(name=cat_data["name"])
                precreated_categories[cat_data["name"]] = created_category
                structure_category_map[cat_data["name"]] = created_category
                result["created_categories"] += 1
            except discord.Forbidden:
                continue

        for ch_data in snapshot.get("channels", []):
            if discord.utils.get(target.channels, name=ch_data["name"]) is not None:
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
                precreated_channels[ch_data["name"]] = created_channel
                result["created_channels"] += 1
            except discord.Forbidden:
                continue

    if delete_roles and not create_only_missing:
        for role in sorted(target.roles, key=lambda r: r.position, reverse=True):
            if role.managed or role.is_default():
                continue
            try:
                await role.delete(reason="CLINX backup load: delete roles")
                result["deleted_roles"] += 1
            except (discord.Forbidden, discord.HTTPException):
                pass

    if load_roles and not create_only_missing:
        existing_roles = {} if delete_roles else {role.name: role for role in target.roles}
        role_order: list[tuple[discord.Role, int]] = []
        for role_data in sorted(snapshot.get("roles", []), key=lambda r: r.get("position", 0)):
            role = existing_roles.get(role_data["name"])
            permissions = discord.Permissions(role_data.get("permissions", 0))
            color = discord.Colour(role_data.get("color", 0))

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
                    existing_roles[role_data["name"]] = created_role
                    role_order.append((created_role, int(role_data.get("position", created_role.position))))
                    result["created_roles"] += 1
                except (discord.Forbidden, discord.HTTPException):
                    pass
            else:
                try:
                    await role.edit(
                        permissions=permissions,
                        colour=color,
                        hoist=role_data.get("hoist", False),
                        mentionable=role_data.get("mentionable", False),
                        reason="CLINX backup load: update role",
                    )
                    role_order.append((role, int(role_data.get("position", role.position))))
                    result["updated_roles"] += 1
                except (discord.Forbidden, discord.HTTPException):
                    pass

        bot_member = target.me
        if role_order and bot_member is not None:
            max_position = max(1, bot_member.top_role.position - 1)
            position_map = {
                role: min(desired_position, max_position)
                for role, desired_position in role_order
                if role < bot_member.top_role
            }
            if position_map:
                try:
                    await target.edit_role_positions(position_map)
                except (discord.Forbidden, discord.HTTPException):
                    pass

    if load_channels:
        category_map: dict[str, discord.CategoryChannel] = dict(precreated_categories)
        existing_categories = {} if delete_channels else {category.name: category for category in target.categories}

        for cat_data in snapshot.get("categories", []):
            existing = category_map.get(cat_data["name"]) or existing_categories.get(cat_data["name"])
            overwrites = deserialize_overwrites(cat_data.get("overwrites", []), target)

            if existing is None:
                try:
                    existing = await target.create_category(name=cat_data["name"], overwrites=overwrites)
                    result["created_categories"] += 1
                except discord.Forbidden:
                    continue
            elif not create_only_missing:
                try:
                    await existing.edit(overwrites=overwrites, position=cat_data.get("position", existing.position))
                except discord.Forbidden:
                    pass

            category_map[cat_data["name"]] = existing

        existing_channels = {} if delete_channels else {channel.name: channel for channel in target.channels}
        for ch_data in snapshot.get("channels", []):
            existing = precreated_channels.get(ch_data["name"]) or existing_channels.get(ch_data["name"])
            category = category_map.get(ch_data.get("category")) if ch_data.get("category") else None
            overwrites = deserialize_overwrites(ch_data.get("overwrites", []), target)

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
                except discord.Forbidden:
                    pass
                continue

            if create_only_missing:
                continue

            try:
                if isinstance(existing, discord.TextChannel) and ch_data["type"] == "text":
                    await existing.edit(
                        category=category,
                        topic=ch_data.get("topic"),
                        slowmode_delay=ch_data.get("slowmode_delay", 0),
                        nsfw=ch_data.get("nsfw", False),
                        overwrites=overwrites,
                    )
                    if ch_data["name"] not in precreated_channels:
                        result["updated_channels"] += 1
                elif isinstance(existing, discord.VoiceChannel) and ch_data["type"] == "voice":
                    await existing.edit(
                        category=category,
                        bitrate=ch_data.get("bitrate", existing.bitrate),
                        user_limit=ch_data.get("user_limit", 0),
                        overwrites=overwrites,
                    )
                    if ch_data["name"] not in precreated_channels:
                        result["updated_channels"] += 1
            except discord.Forbidden:
                pass

    if load_settings and not create_only_missing:
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

        if profile_kwargs:
            try:
                await target.edit(reason="CLINX backup load: update settings", **profile_kwargs)
                result["updated_settings"] += 1
            except (discord.Forbidden, discord.HTTPException):
                pass

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
            except (discord.Forbidden, discord.HTTPException):
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
        )
        BACKUP_LOAD_JOBS[guild_id]["status"] = "completed"
        BACKUP_LOAD_JOBS[guild_id]["finished_at"] = utc_now_iso()
        BACKUP_LOAD_JOBS[guild_id]["stats"] = stats
    except asyncio.CancelledError:
        BACKUP_LOAD_JOBS[guild_id]["status"] = "cancelled"
        BACKUP_LOAD_JOBS[guild_id]["finished_at"] = utc_now_iso()
        raise
    except Exception as exc:
        BACKUP_LOAD_JOBS[guild_id]["status"] = "failed"
        BACKUP_LOAD_JOBS[guild_id]["finished_at"] = utc_now_iso()
        BACKUP_LOAD_JOBS[guild_id]["error"] = str(exc)


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
    target_channel_names = {channel.name for channel in target_channels}

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
            preview["created_channels"] = sum(1 for channel in snapshot_channels if channel.get("name") not in target_channel_names)
            preview["updated_channels"] = sum(1 for channel in snapshot_channels if channel.get("name") in target_channel_names)

    return preview


def build_backup_lane_lines(selected_actions: set[str]) -> list[str]:
    return [
        f"- **Load Roles**: {'ON' if 'load_roles' in selected_actions else 'OFF'} - restore role stack from backup",
        f"- **Load Channels**: {'ON' if 'load_channels' in selected_actions else 'OFF'} - restore categories and channels",
        f"- **Load Settings**: {'ON' if 'load_settings' in selected_actions else 'OFF'} - sync server profile and config",
        f"- **Delete Roles**: {'ON' if 'delete_roles' in selected_actions else 'OFF'} - wipe live roles before rebuild",
        f"- **Delete Channels**: {'ON' if 'delete_channels' in selected_actions else 'OFF'} - wipe live channels before rebuild",
    ]


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
                    discord.ui.TextDisplay("Stage 3 of 3. CLINX is now applying the reviewed recovery plan."),
                    accessory=hero,
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Route"),
                    discord.ui.TextDisplay(f"`{self.source_name}` -> `{self.target_name}`"),
                    accessory=discord.ui.Button(label="Live", style=discord.ButtonStyle.secondary, disabled=True),
                ),
                discord.ui.TextDisplay(f"### Active Lanes\n{active_lanes}"),
                discord.ui.TextDisplay(f"### Deletes In Flight\n{chr(10).join(delete_lines)}"),
                discord.ui.TextDisplay(f"### Build In Flight\n{chr(10).join(build_lines)}"),
                discord.ui.TextDisplay("Use **View Status** or `/backup status` for live counters while the restore is running."),
                accent_color=EMBED_INFO,
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
            label="Destructive" if destructive else "Merge Only",
            style=discord.ButtonStyle.danger if destructive else discord.ButtonStyle.secondary,
            disabled=True,
        )
        lane_lines = build_backup_lane_lines(self.selected_actions)

        children: list[discord.ui.Item[Any]] = [
            discord.ui.Section(
                discord.ui.TextDisplay("## <> Backup Load Planner"),
                discord.ui.TextDisplay(
                    "Arm the restore lanes first. CLINX will only show the destructive math after you continue."
                    if not self.review_mode
                    else "Review the exact delete and build impact below, then apply the backup."
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

        accent = EMBED_WARN if destructive else (EMBED_WARN if self.review_mode else 0x5B8CFF)
        return discord.ui.Container(*children, accent_color=accent)

    def _make_toggle_button(self, action: str) -> discord.ui.Button:
        selected = action in self.selected_actions
        destructive = action.startswith("delete_")
        enabled = not (
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
        button = discord.ui.Button(label="Continue", style=discord.ButtonStyle.success, disabled=not has_rebuild_lane)

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
        button = discord.ui.Button(label="Apply Backup", style=discord.ButtonStyle.primary)

        async def callback(interaction: discord.Interaction) -> None:
            await self.start_backup_load(interaction)

        button.callback = callback
        return button

    async def start_backup_load(self, interaction: discord.Interaction) -> None:
        existing_job = BACKUP_LOAD_JOBS.get(self.target.id)
        if existing_job and existing_job.get("status") == "running":
            await interaction.response.send_message(
                embed=make_embed("Backup Busy", "A backup load is already running in this server. Use `/backup status` to check it.", EMBED_WARN, interaction),
                ephemeral=True,
            )
            return

        preview = build_backup_plan_preview(self.snapshot, self.target, self.selected_actions)
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
        BACKUP_LOAD_JOBS[self.target.id] = {
            "status": "running",
            "started_at": utc_now_iso(),
            "backup_id": self.backup_id,
            "source_name": self.source_name,
            "target_name": self.target.name,
            "selected_actions": sorted(self.selected_actions),
            "preview": preview,
            "task": task,
        }
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
            CommandLibraryEntry("/backup create", "Create a recovery snapshot of the current server.", "Captures channels, roles, overwrites, and server settings into a reusable backup ID.", "Private", ("/backupcreate",)),
            CommandLibraryEntry("/backup load", "Load a backup with lane selection and live status.", "Opens the restore planner so you can choose what to rebuild before CLINX touches the target server.", "Private", ("/backupload",)),
            CommandLibraryEntry("/backup list", "List only the backups owned by your account.", "Shows your stored backup codes with creation time so only your account can load or delete them.", "Private", ("/backuplist",)),
            CommandLibraryEntry("/backup delete", "Remove a stored backup ID.", "Deletes a backup record from CLINX storage so it can no longer be loaded.", "Private"),
            CommandLibraryEntry("/backup status", "Inspect the current load job.", "Returns the running state, route, and counters for the active restore job in this server.", "Private"),
            CommandLibraryEntry("/backup cancel", "Cancel the current backup load.", "Stops the active restore task for this server if one is running.", "Private"),
            CommandLibraryEntry("/backupcreate", "Alias for `/backup create`.", "Shortcut alias for creating a backup without using the group command.", "Private", ("/backup create",)),
            CommandLibraryEntry("/backupload", "Alias for `/backup load`.", "Shortcut alias for opening the restore planner without using the group command.", "Private", ("/backup load",)),
            CommandLibraryEntry("/backuplist", "Alias for `/backup list`.", "Shortcut alias for opening your personal backup list.", "Private", ("/backup list",)),
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
            CommandLibraryEntry("/cleantoday", "Delete channels created today.", "Useful for nuked test runs and bad imports. Dry-run unless `confirm=true` is supplied.", "Public"),
            CommandLibraryEntry("/masschannels", "Paste a layout and bulk-create the structure.", "Reads a copied category and channel layout, then recreates the structure in one pass.", "Public"),
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
            CommandLibraryEntry("/panel suggestion", "Post the CLINX ideas board in the current channel.", "Drops a standalone public board with suggestion and bug-report entry points.", "Private"),
            CommandLibraryEntry("/help", "Open the CLINX command library.", "Browse command lanes, page through the catalog, and inspect each command in a single surface.", "Public"),
            CommandLibraryEntry("/invite", "Get the bot invite link.", "Returns the OAuth invite for CLINX with bot and slash command scopes.", "Public"),
            CommandLibraryEntry("/leave", "Make CLINX leave the current server.", "Tells the bot to exit the server immediately after confirmation.", "Private"),
        ),
    ),
)

COMMAND_LIBRARY_BY_KEY = {lane.key: lane for lane in COMMAND_LIBRARY_LANES}
COMMAND_LIBRARY_BY_PATH = {entry.path: entry for lane in COMMAND_LIBRARY_LANES for entry in lane.entries}
COMMAND_LIBRARY_PAGE_SIZE = 6


def build_invite_url(app_id: int | None) -> str:
    resolved_id = app_id or "YOUR_APP_ID"
    return f"https://discord.com/oauth2/authorize?client_id={resolved_id}&permissions=8&integration_type=0&scope=bot+applications.commands"


def format_command_library_page(lane: CommandLibraryLane, page: int) -> tuple[str, int]:
    total_pages = max(1, (len(lane.entries) + COMMAND_LIBRARY_PAGE_SIZE - 1) // COMMAND_LIBRARY_PAGE_SIZE)
    safe_page = max(0, min(page, total_pages - 1))
    start = safe_page * COMMAND_LIBRARY_PAGE_SIZE
    entries = lane.entries[start : start + COMMAND_LIBRARY_PAGE_SIZE]
    lines = [f"- `/{entry.path.lstrip('/')}` - {entry.summary}" for entry in entries]
    return "\n".join(lines), total_pages


def format_command_library_detail(entry: CommandLibraryEntry | None) -> str:
    if entry is None:
        return (
            "Pick a command from the selector below to inspect what it does, how visible it is, "
            "and which alias CLINX accepts."
        )

    detail_lines = [
        f"**`/{entry.path.lstrip('/')}`**",
        entry.detail,
        "",
        f"**Visibility**: {entry.visibility}",
    ]
    if entry.aliases:
        aliases = ", ".join(f"`{alias}`" for alias in entry.aliases)
        detail_lines.append(f"**Aliases**: {aliases}")
    return "\n".join(detail_lines)


def build_feedback_card_view(
    bot_user: discord.ClientUser | None,
    *,
    mode: str,
    ticket_id: str,
    title: str,
    details: str,
    operator: str,
    outcome: str,
) -> discord.ui.LayoutView:
    title_text = discord.utils.escape_markdown(title.strip())
    detail_text = discord.utils.escape_markdown(details.strip())
    outcome_text = discord.utils.escape_markdown(outcome.strip())
    operator_text = discord.utils.escape_markdown(operator)
    accent = EMBED_OK if mode == "suggestion" else EMBED_WARN
    badge = "Suggestion" if mode == "suggestion" else "Bug Report"

    view = discord.ui.LayoutView(timeout=None)
    header = discord.ui.Section(
        discord.ui.TextDisplay(f"## <> {badge} Intake"),
        discord.ui.TextDisplay(f"`{ticket_id}` filed by `{operator_text}`"),
        accessory=discord.ui.Thumbnail(bot_user.display_avatar.url) if bot_user else discord.ui.Button(label="CLINX", disabled=True),
    )
    container = discord.ui.Container(
        header,
        discord.ui.Separator(),
        discord.ui.TextDisplay(f"### Title\n{title_text}"),
        discord.ui.TextDisplay(f"### Brief\n{detail_text}"),
        discord.ui.TextDisplay(f"### Outcome\n{outcome_text}"),
        accent_color=accent,
    )
    view.add_item(container)
    return view


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


class FeedbackModal(discord.ui.Modal):
    def __init__(self, mode: str) -> None:
        self.mode = mode
        label = "Suggestion" if mode == "suggestion" else "Bug Report"
        super().__init__(title=f"{label} Intake")
        self.headline = discord.ui.TextInput(
            label="Headline",
            placeholder="Short summary",
            max_length=100,
        )
        self.details = discord.ui.TextInput(
            label="Details",
            placeholder="Describe the idea, issue, or requested change.",
            style=discord.TextStyle.paragraph,
            max_length=1500,
        )
        self.outcome = discord.ui.TextInput(
            label="Desired Outcome",
            placeholder="What should happen instead?",
            style=discord.TextStyle.paragraph,
            max_length=600,
            required=False,
        )
        self.add_item(self.headline)
        self.add_item(self.details)
        self.add_item(self.outcome)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        channel = interaction.channel
        if channel is None or not hasattr(channel, "send"):
            await interaction.response.send_message("CLINX could not resolve a messageable channel for this form.", ephemeral=True)
            return

        ticket_id = f"{'SG' if self.mode == 'suggestion' else 'BG'}-{datetime.now(timezone.utc).strftime('%d%H%M')}-{secrets.token_hex(2).upper()}"
        card = build_feedback_card_view(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            mode=self.mode,
            ticket_id=ticket_id,
            title=self.headline.value,
            details=self.details.value,
            operator=interaction.user.display_name,
            outcome=self.outcome.value or "No extra outcome note provided.",
        )
        try:
            await channel.send(view=card)
        except discord.Forbidden:
            await interaction.response.send_message("CLINX cannot post the feedback card in this channel.", ephemeral=True)
            return
        await interaction.response.send_message(f"{'Suggestion' if self.mode == 'suggestion' else 'Bug report'} posted to {channel.mention}.", ephemeral=True)


class SuggestionBoardView(discord.ui.LayoutView):
    def __init__(self, bot_user: discord.ClientUser | None) -> None:
        super().__init__(timeout=None)
        self.bot_user = bot_user
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()
        hero_accessory = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        badge = discord.ui.Button(label="Ideas Open", style=discord.ButtonStyle.secondary, disabled=True)
        container = discord.ui.Container(
            discord.ui.Section(
                discord.ui.TextDisplay("## <> CLINX Feedback Board"),
                discord.ui.TextDisplay("Drop product ideas, UI upgrades, restore flow pain points, or reproducible bug reports here."),
                accessory=hero_accessory,
            ),
            discord.ui.Separator(),
            discord.ui.Section(
                discord.ui.TextDisplay("### Intake Rules"),
                discord.ui.TextDisplay("Use **Suggestion** for product or UX requests. Use **Bug Report** when the current bot behavior is broken or inconsistent."),
                accessory=badge,
            ),
            discord.ui.TextDisplay(
                "### Routing\n"
                "Submissions from this board are posted back into the channel as standalone CLINX cards so the thread stays visible."
            ),
            accent_color=0x6F8BFF,
        )
        self.add_item(container)
        self.add_item(
            discord.ui.ActionRow(
                self._make_modal_button("Suggestion", "suggestion", discord.ButtonStyle.primary),
                self._make_modal_button("Bug Report", "bug", discord.ButtonStyle.secondary),
                discord.ui.Button(label="Support", url=SUPPORT_URL),
            )
        )

    def _make_modal_button(
        self,
        label: str,
        mode: str,
        style: discord.ButtonStyle,
    ) -> discord.ui.Button:
        button = discord.ui.Button(label=label, style=style, custom_id=f"clinx:feedback:{mode}")

        async def callback(interaction: discord.Interaction) -> None:
            await interaction.response.send_modal(FeedbackModal(mode))

        button.callback = callback
        return button


intents = discord.Intents.default()
intents.guilds = True
intents.members = True


class ClinxBot(commands.Bot):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._groups_added = False
        self._startup_synced = False

    async def setup_hook(self) -> None:
        if not self._groups_added:
            self.tree.add_command(backup_group)
            self.tree.add_command(export_group)
            self.tree.add_command(import_group)
            self.tree.add_command(panel_group)
            self.add_view(SuggestionBoardView(None))
            self._groups_added = True

        if not self._startup_synced:
            synced = await self.tree.sync()
            self._startup_synced = True
            print(f"Synced {len(synced)} slash commands")


bot = ClinxBot(command_prefix=commands.when_mentioned, intents=intents)

backup_group = app_commands.Group(name="backup", description="Backup and restore commands")
export_group = app_commands.Group(name="export", description="Export server objects")
import_group = app_commands.Group(name="import", description="Import server objects")
panel_group = app_commands.Group(name="panel", description="Send CLINX interactive surfaces")


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
        "created_by_user_id": str(interaction.user.id),
        "created_by_display_name": str(interaction.user),
        "source_guild_id": source.id,
        "source_guild_name": source.name,
        "snapshot": await build_guild_snapshot(source),
    }
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
@app_commands.autocomplete(load_id=backup_id_autocomplete)
@app_commands.default_permissions(administrator=True)
async def backup_load(interaction: discord.Interaction, load_id: str, target_guild_id: int | None = None) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)

    store = load_backup_store()
    record = store.get("backups", {}).get(load_id)
    if record is None:
        await interaction.followup.send(embed=make_embed("Invalid Load ID", "No backup found with that ID.", EMBED_ERR), ephemeral=True)
        return
    if not can_access_backup(record, interaction.user.id):
        await interaction.followup.send(embed=make_embed("Access Denied", "That backup does not belong to your account.", EMBED_ERR), ephemeral=True)
        return

    target = interaction.guild if target_guild_id is None else bot.get_guild(target_guild_id)
    if target is None:
        await interaction.followup.send(embed=make_embed("Error", "Target guild not found.", EMBED_ERR), ephemeral=True)
        return

    view = BackupLoadPlannerView(
        interaction.user.id,
        load_id,
        record.get("source_guild_name", "Unknown Source"),
        record["snapshot"],
        target,
        interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
    )
    await interaction.followup.send(view=view, ephemeral=True)


@backup_group.command(name="list", description="List saved backup IDs")
@app_commands.default_permissions(administrator=True)
async def backup_list(interaction: discord.Interaction) -> None:
    store = load_backup_store()
    entries = get_user_backup_entries(store, interaction.user.id)[:20]
    if not entries:
        await interaction.response.send_message(embed=make_embed("Backups", "No backups found.", EMBED_INFO), ephemeral=True)
        return

    lines: list[str] = []
    for entry in entries:
        lines.append(f"`{entry['id']}`")
        lines.append(f"{entry.get('source_guild_name', 'Unknown Source')} - {format_backup_timestamp(entry.get('created_at'))}")
    await interaction.response.send_message(embed=make_embed("Backups", "\n".join(lines), EMBED_INFO), ephemeral=True)


@backup_group.command(name="delete", description="Delete a backup ID")
@app_commands.describe(load_id="Backup load ID")
@app_commands.autocomplete(load_id=backup_id_autocomplete)
@app_commands.default_permissions(administrator=True)
async def backup_delete(interaction: discord.Interaction, load_id: str) -> None:
    store = load_backup_store()
    record = store.get("backups", {}).get(load_id)
    if record is None:
        await interaction.response.send_message(embed=make_embed("Error", "Load ID not found.", EMBED_ERR), ephemeral=True)
        return
    if not can_access_backup(record, interaction.user.id):
        await interaction.response.send_message(embed=make_embed("Access Denied", "That backup does not belong to your account.", EMBED_ERR), ephemeral=True)
        return

    del store["backups"][load_id]
    save_backup_store(store)
    await interaction.response.send_message(embed=make_embed("Deleted", f"Removed `{load_id}`.", EMBED_OK), ephemeral=True)


@backup_group.command(name="status", description="Get current backup load status")
@app_commands.default_permissions(administrator=True)
async def backup_status(interaction: discord.Interaction) -> None:
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    job = BACKUP_LOAD_JOBS.get(guild.id)
    if not job:
        await interaction.response.send_message(embed=make_embed("Backup Status", "No backup load job found.", EMBED_INFO), ephemeral=True)
        return

    await interaction.response.send_message(
        embed=make_embed("Backup Status", build_backup_load_status_description(job), EMBED_INFO, interaction),
        ephemeral=True,
    )


@backup_group.command(name="cancel", description="Cancel running backup load")
@app_commands.default_permissions(administrator=True)
async def backup_cancel(interaction: discord.Interaction) -> None:
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    job = BACKUP_LOAD_JOBS.get(guild.id)
    if not job or job.get("status") != "running":
        await interaction.response.send_message(embed=make_embed("Backup", "No running backup load to cancel.", EMBED_INFO), ephemeral=True)
        return

    task = job.get("task")
    if task:
        task.cancel()
    await interaction.response.send_message(embed=make_embed("Backup", "Cancel requested.", EMBED_WARN), ephemeral=True)


@bot.tree.command(name="backupcreate", description="Alias of /backup create")
@app_commands.default_permissions(administrator=True)
async def backupcreate_alias(interaction: discord.Interaction, source_guild_id: int | None = None) -> None:
    await backup_create.callback(interaction, source_guild_id)


@bot.tree.command(name="backupload", description="Alias of /backup load")
@app_commands.autocomplete(load_id=backup_id_autocomplete)
@app_commands.default_permissions(administrator=True)
async def backupload_alias(interaction: discord.Interaction, load_id: str, target_guild_id: int | None = None) -> None:
    await backup_load.callback(interaction, load_id, target_guild_id)


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

    snapshot = await build_guild_snapshot(source, include_assets=False)
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
            await ch.delete(reason=f"/cleantoday by {interaction.user}")
            deleted += 1
        except discord.Forbidden:
            pass

    await interaction.followup.send(embed=make_embed("Clean Today Complete", f"Deleted `{deleted}` channels.", EMBED_OK), ephemeral=True)


@bot.tree.command(name="masschannels", description="Create channels from pasted layout text")
@app_commands.default_permissions(administrator=True)
async def masschannels(interaction: discord.Interaction, layout: str, create_categories: bool = True) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)

    guild = interaction.guild
    if guild is None:
        await interaction.followup.send(embed=make_embed("Error", "Run this command in a server.", EMBED_ERR), ephemeral=True)
        return

    items = parse_layout(layout)
    if not items:
        await interaction.followup.send(embed=make_embed("Mass Channels", "No valid channels found in layout.", EMBED_ERR), ephemeral=True)
        return

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



@panel_group.command(name="suggestion", description="Post the CLINX feedback board")
async def panel_suggestion(interaction: discord.Interaction) -> None:
    channel = interaction.channel
    if channel is None or not hasattr(channel, "send"):
        await interaction.response.send_message(embed=make_embed("Error", "CLINX could not post the board in this channel.", EMBED_ERR, interaction), ephemeral=True)
        return

    try:
        await channel.send(view=SuggestionBoardView(interaction.client.user if isinstance(interaction.client, commands.Bot) else None))
    except discord.Forbidden:
        await interaction.response.send_message(embed=make_embed("Error", "CLINX does not have permission to post the board in this channel.", EMBED_ERR, interaction), ephemeral=True)
        return
    await interaction.response.send_message(embed=make_embed("Board Posted", "The CLINX feedback board is now live in this channel.", EMBED_OK, interaction), ephemeral=True)
async def send_text_file(interaction: discord.Interaction, text: str, filename: str) -> None:
    data = io.BytesIO(text.encode("utf-8"))
    await interaction.response.send_message(file=discord.File(data, filename=filename), ephemeral=True)


@export_group.command(name="guild", description="Export full guild snapshot as JSON")
async def export_guild(interaction: discord.Interaction) -> None:
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    snapshot = await build_guild_snapshot(guild)
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


@bot.tree.command(name="help", description="Open the CLINX command library")
async def help_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.send_message(view=CommandLibraryView(interaction.client.user if isinstance(interaction.client, commands.Bot) else None))


@bot.tree.command(name="invite", description="Get bot invite link")
async def invite(interaction: discord.Interaction) -> None:
    app_id = bot.user.id if bot.user else None
    link = build_invite_url(app_id)
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








