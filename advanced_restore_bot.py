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
DEVELOPER_USER_IDS = {1240237445841420302}

DATA_DIR = Path(__file__).parent / "data"
BACKUP_FILE = DATA_DIR / "backups.json"
SAFETY_FILE = DATA_DIR / "safety.json"

EMBED_OK = 0x22C55E
EMBED_WARN = 0xFACC15
EMBED_ERR = 0xFF5A76
EMBED_INFO = 0x5B8CFF

IMPORT_JOBS: dict[int, dict[str, Any]] = {}
BACKUP_LOAD_JOBS: dict[int, dict[str, Any]] = {}
PENDING_SAFETY_REQUESTS: dict[int, dict[str, Any]] = {}


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
    if not SAFETY_FILE.exists():
        SAFETY_FILE.write_text(json.dumps({"guilds": {}}, indent=2), encoding="utf-8")


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
    store = json.loads(SAFETY_FILE.read_text(encoding="utf-8"))
    if not isinstance(store.get("guilds"), dict):
        store = {"guilds": {}}
        save_safety_store(store)
    return store


def save_safety_store(store: dict[str, Any]) -> None:
    ensure_storage()
    if not isinstance(store.get("guilds"), dict):
        store["guilds"] = {}
    SAFETY_FILE.write_text(json.dumps(store, indent=2), encoding="utf-8")


def get_guild_safety_bucket(store: dict[str, Any], guild_id: int) -> dict[str, Any]:
    guild_key = str(guild_id)
    guilds = store.setdefault("guilds", {})
    bucket = guilds.setdefault(guild_key, {"trusted_admin_ids": []})
    if not isinstance(bucket.get("trusted_admin_ids"), list):
        bucket["trusted_admin_ids"] = []
    bucket["trusted_admin_ids"] = sorted({str(user_id) for user_id in bucket["trusted_admin_ids"]})
    return bucket


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
    return percent, build_progress_bar(percent), min(index + 1, len(phases)), len(phases)


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


def has_administrator(user: discord.abc.User) -> bool:
    return isinstance(user, discord.Member) and user.guild_permissions.administrator


def is_developer_user(user: discord.abc.User) -> bool:
    return user.id in DEVELOPER_USER_IDS


def get_command_safety_tier(command_name: str, *, selected_actions: set[str] | None = None, destructive: bool = False) -> int:
    if command_name in {"help", "invite"}:
        return 0
    if command_name in {
        "backup_create",
        "backup_list",
        "backup_delete",
        "backupcreate",
        "backuplist",
        "export_guild",
        "export_channels",
        "export_roles",
        "export_channel",
        "export_role",
        "export_message",
        "export_reactions",
    }:
        return 1
    if command_name in {"restore_missing", "masschannels", "import_guild"}:
        return 2
    if command_name in {"backup_load", "backupload"}:
        if selected_actions and {"delete_roles", "delete_channels"} & selected_actions:
            return 3
        return 2
    if command_name == "cleantoday":
        return 3 if destructive else 1
    if command_name in {"leave", "safety_grant", "safety_revoke", "safety_list"}:
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
    if is_guild_owner(interaction.user, guild):
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
            await progress_callback(phase, detail, dict(result))

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
            except discord.Forbidden:
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
            except (discord.Forbidden, discord.HTTPException):
                continue

    if delete_roles and not create_only_missing:
        await report("Wiping Roles", "Clearing live roles so the backup stack can be rebuilt cleanly.")
        for role in sorted(target.roles, key=lambda r: r.position, reverse=True):
            if role.managed or role.is_default():
                continue
            try:
                await role.delete(reason="CLINX backup load: delete roles")
                result["deleted_roles"] += 1
            except (discord.Forbidden, discord.HTTPException):
                result["blocked_roles"] += 1

    if load_roles and not create_only_missing:
        await report("Rebuilding Roles", "Creating and updating the role stack from the backup snapshot.")
        existing_roles: dict[str, list[discord.Role]] = {}
        if not delete_roles:
            for live_role in sorted(target.roles, key=lambda item: item.position):
                if live_role.is_default() or live_role.managed:
                    continue
                existing_roles.setdefault(live_role.name, []).append(live_role)
        role_order: list[tuple[discord.Role, int]] = []
        for role_data in sorted(snapshot.get("roles", []), key=lambda r: r.get("position", 0)):
            role: discord.Role | None = None
            if not delete_roles:
                bucket = existing_roles.get(role_data["name"], [])
                if bucket:
                    role = bucket.pop(0)
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
                    role_order.append((created_role, int(role_data.get("position", created_role.position))))
                    result["created_roles"] += 1
                except (discord.Forbidden, discord.HTTPException):
                    result["blocked_roles"] += 1
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
                    result["blocked_roles"] += 1

        bot_member = target.me
        if role_order and bot_member is not None:
            max_position = max(1, bot_member.top_role.position - 1)
            manageable_roles = [item for item in role_order if item[0] < bot_member.top_role]
            manageable_roles.sort(key=lambda item: item[1])
            position_map: dict[discord.Role, int] = {}
            for slot, (role, _) in enumerate(manageable_roles, start=1):
                position_map[role] = min(slot, max_position)
            if position_map:
                try:
                    await target.edit_role_positions(position_map)
                except (discord.Forbidden, discord.HTTPException):
                    result["blocked_roles"] += len(position_map)

    if load_channels:
        await report("Finalizing Channels", "Binding channel permissions, topics, and structure against the rebuilt role stack.")
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

        existing_channels = {} if delete_channels else {live_channel_signature(channel): channel for channel in target.channels}
        for ch_data in snapshot.get("channels", []):
            channel_key = snapshot_channel_signature(ch_data)
            existing = precreated_channels.get(channel_key) or existing_channels.get(channel_key)
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
                except (discord.Forbidden, discord.HTTPException):
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
                    if channel_key not in precreated_channels:
                        result["updated_channels"] += 1
                elif isinstance(existing, discord.VoiceChannel) and ch_data["type"] == "voice":
                    await existing.edit(
                        category=category,
                        bitrate=ch_data.get("bitrate", existing.bitrate),
                        user_limit=ch_data.get("user_limit", 0),
                        overwrites=overwrites,
                    )
                    if channel_key not in precreated_channels:
                        result["updated_channels"] += 1
            except (discord.Forbidden, discord.HTTPException, AttributeError):
                pass

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
    async def report_phase(phase: str, detail: str, stats: dict[str, int]) -> None:
        job = BACKUP_LOAD_JOBS.get(guild_id)
        if not job:
            return
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
        blocked_role_count = sum(
            1
            for role in snapshot.get("roles", [])
            if int(role.get("position", 0)) >= bot_member.top_role.position
        )
        if blocked_role_count:
            warnings.append(
                f"`{blocked_role_count}` backup roles sit at or above the CLINX role in this server. "
                "Move CLINX higher or those roles cannot be restored at the right position."
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
        children: list[discord.ui.Item[Any]] = [
            discord.ui.Section(
                discord.ui.TextDisplay("## <> Backup Vault Sealed"),
                discord.ui.TextDisplay("Snapshot locked. This recovery ID is ready for private restore use."),
                accessory=hero,
            ),
            discord.ui.Separator(),
            discord.ui.Section(
                discord.ui.TextDisplay("### Load ID"),
                discord.ui.TextDisplay(f"`{self.backup_id}`"),
                accessory=discord.ui.Button(label="Ready", style=discord.ButtonStyle.success, disabled=True),
            ),
            discord.ui.Section(
                discord.ui.TextDisplay("### Source"),
                discord.ui.TextDisplay(f"`{self.source.name}` ({self.source.id})"),
                accessory=discord.ui.Button(label="Private", style=discord.ButtonStyle.secondary, disabled=True),
            ),
            discord.ui.Section(
                discord.ui.TextDisplay("### Snapshot Payload"),
                discord.ui.TextDisplay(
                    f"`{snapshot_roles}` roles\n"
                    f"`{snapshot_categories}` categories\n"
                    f"`{snapshot_channels}` channels"
                ),
                accessory=discord.ui.Button(label="Vault", style=discord.ButtonStyle.primary, disabled=True),
            ),
            discord.ui.TextDisplay(
                "### Restore Profile\n"
                "- Roles, channels, categories, overwrites, and server settings were captured.\n"
                "- Use `/backup load` to open the planner and apply this snapshot."
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


class BackupListCardView(discord.ui.LayoutView):
    def __init__(self, bot_user: discord.ClientUser | None, entries: list[dict[str, Any]]) -> None:
        super().__init__(timeout=None)
        self.bot_user = bot_user
        self.entries = entries
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()
        hero = (
            discord.ui.Thumbnail(self.bot_user.display_avatar.url)
            if self.bot_user
            else discord.ui.Button(label="CLINX", disabled=True)
        )
        blocks: list[str] = []
        for index, entry in enumerate(self.entries, start=1):
            blocks.append(
                f"**{index}.** `{entry['id']}`\n"
                f"- Source: `{entry.get('source_guild_name', 'Unknown Source')}`\n"
                f"- Created: `{format_backup_timestamp(entry.get('created_at'))}`"
            )
        self.add_item(
            discord.ui.Container(
                discord.ui.Section(
                    discord.ui.TextDisplay("## <> Backup Vault"),
                    discord.ui.TextDisplay("Private recovery IDs owned by your account are listed here."),
                    accessory=hero,
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay("### Vault Feed"),
                    discord.ui.TextDisplay(f"`{len(self.entries)}` backup IDs ready for restore"),
                    accessory=discord.ui.Button(label="Private", style=discord.ButtonStyle.secondary, disabled=True),
                ),
                discord.ui.TextDisplay("### Your Backups\n" + "\n\n".join(blocks)),
                accent_color=EMBED_INFO,
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
    channel_id = job.get("status_channel_id")
    channel = bot.get_channel(channel_id) if channel_id else None
    if channel is None and channel_id:
        try:
            channel = await bot.fetch_channel(channel_id)
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            channel = None
    if channel is not None and hasattr(channel, "send"):
        return channel

    guild = bot.get_guild(guild_id)
    if guild is None or bot.user is None:
        return None
    me = guild.me or guild.get_member(bot.user.id)

    restoring = discord.utils.get(guild.text_channels, name="restoring")
    if restoring is not None and me is not None:
        permissions = restoring.permissions_for(me)
        if permissions.view_channel and permissions.send_messages:
            job["status_channel_id"] = restoring.id
            job["status_message_id"] = None
            return restoring

    if me is not None and me.guild_permissions.manage_channels:
        try:
            restoring = await guild.create_text_channel("restoring", reason="CLINX backup load: restore status fallback channel")
            job["status_channel_id"] = restoring.id
            job["status_message_id"] = None
            return restoring
        except (discord.Forbidden, discord.HTTPException):
            pass

    fallback = resolve_notice_channel(guild)
    if fallback is not None and hasattr(fallback, "send"):
        fallback_id = getattr(fallback, "id", None)
        if fallback_id is not None:
            job["status_channel_id"] = fallback_id
            job["status_message_id"] = None
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
            f"`{progress_bar}` `{progress_percent}%`\n"
            f"Stage `{stage_index}` of `{stage_total}`"
        )
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
                    discord.ui.TextDisplay("Stage 3 of 3. The restore plan is live. CLINX keeps the public transit card updating in the active restore channel."),
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
                discord.ui.TextDisplay("Use **View Status** for a private snapshot or `/backup status` to re-post the public transit card. If the original channel is rebuilt, CLINX falls back to `#restoring`."),
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
            channel = interaction.channel
            if channel is not None and hasattr(channel, "send"):
                existing_job["status_channel_id"] = channel.id
                existing_job["status_message_id"] = None
                await interaction.response.send_message(view=BackupLoadStatusCardView(self.bot_user, existing_job), ephemeral=False)
                message = await interaction.original_response()
                existing_job["status_message_id"] = message.id
            else:
                await interaction.response.send_message(
                    embed=make_embed("Backup Busy", "A backup load is already running in this server. Use `/backup status` to check it.", EMBED_INFO, interaction),
                    ephemeral=False,
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
                    "phase_detail": "Owner approval completed. CLINX is opening the restore lane.",
                    "stats": {},
                    "warnings": warnings,
                    "status_channel_id": request["channel_id"],
                    "status_message_id": None,
                    "preserve_channel_id": request.get("preserve_channel_id"),
                    "task": None,
                    "approval_request_id": request["id"],
                }
                await sync_backup_status_message(self.target.id)
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
            "phase_detail": "CLINX is opening the restore lane and posting a live status card in this channel.",
            "stats": {},
            "warnings": warnings,
            "status_channel_id": interaction.channel.id if interaction.channel is not None and hasattr(interaction.channel, "send") else None,
            "status_message_id": None,
            "preserve_channel_id": interaction.channel.id if isinstance(interaction.channel, discord.TextChannel) else None,
            "task": None,
            "approval_request_id": None,
        }
        await sync_backup_status_message(self.target.id)
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
            CommandLibraryEntry("/backup create", "Create a recovery snapshot of the current server.", "Captures channels, roles, overwrites, and server settings into a reusable backup ID.", "Private", ("/backupcreate",)),
            CommandLibraryEntry("/backup load", "Load a backup with lane selection and live status.", "Opens the restore planner so you can choose what to rebuild before CLINX touches the target server.", "Private", ("/backupload",)),
            CommandLibraryEntry("/backup list", "List only the backups owned by your account.", "Shows your stored backup codes with creation time so only your account can load or delete them.", "Private", ("/backuplist",)),
            CommandLibraryEntry("/backup delete", "Remove a stored backup ID.", "Deletes a backup record from CLINX storage so it can no longer be loaded.", "Private"),
            CommandLibraryEntry("/backup status", "Inspect the current load job.", "Posts or refreshes the public live status card for the active restore job in this server.", "Public"),
            CommandLibraryEntry("/backup cancel", "Cancel the current backup load.", "Stops the active restore task for this server if one is running and updates the public live card.", "Public"),
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
            CommandLibraryEntry("/help", "Open the CLINX command library.", "Browse command lanes, page through the catalog, and inspect each command in a single surface.", "Public"),
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
            self.tree.add_command(safety_group)
            self._groups_added = True

        if not self._startup_synced:
            synced = await self.tree.sync()
            self._startup_synced = True
            print(f"Synced {len(synced)} slash commands")


bot = ClinxBot(command_prefix=commands.when_mentioned, intents=intents)

backup_group = app_commands.Group(name="backup", description="Backup and restore commands")
export_group = app_commands.Group(name="export", description="Export server objects")
import_group = app_commands.Group(name="import", description="Import server objects")
safety_group = app_commands.Group(name="safety", description="CLINX trust and approval controls")


@bot.event
async def on_ready() -> None:
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

    snapshot = await build_guild_snapshot(source)
    source_warnings = build_source_hierarchy_warnings(source)
    store = load_backup_store()
    backup_id = f"BKP-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{secrets.token_hex(3).upper()}"
    store["backups"][backup_id] = {
        "id": backup_id,
        "created_at": utc_now_iso(),
        "created_by_user_id": str(interaction.user.id),
        "created_by_display_name": str(interaction.user),
        "source_guild_id": source.id,
        "source_guild_name": source.name,
        "snapshot": snapshot,
    }
    save_backup_store(store)

    await interaction.followup.send(
        view=BackupCreatedCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            backup_id,
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
    store = load_backup_store()
    record = store.get("backups", {}).get(load_id)
    if record is None:
        await interaction.response.send_message(embed=make_embed("Invalid Load ID", "No backup found with that ID.", EMBED_ERR), ephemeral=True)
        return
    if not can_access_backup(record, interaction.user.id):
        await interaction.response.send_message(embed=make_embed("Access Denied", "That backup does not belong to your account.", EMBED_ERR), ephemeral=True)
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
    store = load_backup_store()
    entries = get_user_backup_entries(store, interaction.user.id)[:20]
    if not entries:
        await interaction.response.send_message(embed=make_embed("Backups", "No backups found.", EMBED_INFO), ephemeral=True)
        return

    await interaction.response.send_message(
        view=BackupListCardView(
            interaction.client.user if isinstance(interaction.client, commands.Bot) else None,
            entries,
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
    channel = interaction.channel
    if channel is not None and hasattr(channel, "send"):
        job["status_channel_id"] = channel.id
        job["status_message_id"] = None
        await interaction.response.send_message(view=BackupLoadStatusCardView(interaction.client.user if isinstance(interaction.client, commands.Bot) else None, job), ephemeral=False)
        message = await interaction.original_response()
        job["status_message_id"] = message.id
        return
    await interaction.response.send_message(embed=make_embed("Backup Status", build_backup_load_status_description(job), EMBED_INFO, interaction), ephemeral=False)


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


@bot.tree.command(name="cleantoday", description="Delete channels created today (UTC)")
@app_commands.default_permissions(administrator=True)
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


@bot.tree.command(name="masschannels", description="Create channels from pasted layout text")
@app_commands.default_permissions(administrator=True)
async def masschannels(interaction: discord.Interaction, layout: str, create_categories: bool = True) -> None:
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


@bot.tree.command(name="deleteallroles", description="Developer only: delete every deletable role in this server")
async def deleteallroles(interaction: discord.Interaction) -> None:
    if not is_developer_user(interaction.user):
        await send_access_denied(interaction, "This CLINX developer command is locked to the bot developer.")
        return

    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run this command inside a server.", EMBED_ERR, interaction), ephemeral=True)
        return

    me = guild.me
    if me is None and bot.user is not None:
        me = guild.get_member(bot.user.id)
    if me is None:
        await interaction.response.send_message(embed=make_embed("Error", "CLINX could not resolve its member state in this server.", EMBED_ERR, interaction), ephemeral=True)
        return

    deletable_roles: list[discord.Role] = []
    blocked_roles: list[discord.Role] = []
    for role in sorted(guild.roles, key=lambda item: item.position, reverse=True):
        if role.is_default() or role.managed:
            continue
        if role >= me.top_role:
            blocked_roles.append(role)
            continue
        deletable_roles.append(role)

    if not deletable_roles and not blocked_roles:
        await interaction.response.send_message(
            embed=make_embed("Role Purge", "There are no deletable roles in this server.", EMBED_INFO, interaction),
            ephemeral=True,
        )
        return

    queued_lines = [f"Queued for deletion: `{len(deletable_roles)}`"]
    if blocked_roles:
        queued_lines.append(f"Blocked by role hierarchy: `{len(blocked_roles)}`")
    await interaction.response.send_message(
        embed=make_embed("Role Purge Started", "\n".join(queued_lines), EMBED_WARN, interaction),
        ephemeral=True,
    )

    deleted = 0
    failed = 0
    for role in deletable_roles:
        try:
            await role.delete(reason=f"CLINX developer purge requested by {interaction.user} ({interaction.user.id})")
            deleted += 1
        except (discord.Forbidden, discord.HTTPException):
            failed += 1

    result_lines = [f"Deleted: `{deleted}`"]
    if blocked_roles:
        result_lines.append(f"Skipped by hierarchy: `{len(blocked_roles)}`")
    if failed:
        result_lines.append(f"Failed: `{failed}`")
    color = EMBED_OK if failed == 0 else EMBED_WARN
    await interaction.edit_original_response(
        embed=make_embed("Role Purge Complete", "\n".join(result_lines), color, interaction),
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
    await interaction.response.send_message(embed=make_embed("Safety", f"{user.mention} is now a trusted CLINX admin in this server.", EMBED_OK), ephemeral=True)


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
    await interaction.response.send_message(embed=make_embed("Safety", f"{user.mention} is no longer a trusted CLINX admin in this server.", EMBED_WARN), ephemeral=True)


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
    if not trusted_ids:
        await interaction.response.send_message(embed=make_embed("Safety", "No trusted CLINX admins are configured for this server.", EMBED_INFO), ephemeral=True)
        return
    lines = []
    for user_id in trusted_ids:
        member = interaction.guild.get_member(int(user_id))
        lines.append(member.mention if member else f"`{user_id}`")
    await interaction.response.send_message(embed=make_embed("Safety", "\n".join(lines), EMBED_INFO), ephemeral=True)


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








