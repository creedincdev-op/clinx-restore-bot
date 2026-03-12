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
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands

TOKEN = os.getenv("BOT_TOKEN")
DEFAULT_BACKUP_GUILD_ID = os.getenv("DEFAULT_BACKUP_GUILD_ID")

DATA_DIR = Path(__file__).parent / "data"
BACKUP_FILE = DATA_DIR / "backups.json"

EMBED_OK = 0x2ECC71
EMBED_WARN = 0xF1C40F
EMBED_ERR = 0xE74C3C
EMBED_INFO = 0x3498DB

IMPORT_JOBS: dict[int, dict[str, Any]] = {}


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
        BACKUP_FILE.write_text(json.dumps({"backups": {}}, indent=2), encoding="utf-8")


def load_backup_store() -> dict[str, Any]:
    ensure_storage()
    return json.loads(BACKUP_FILE.read_text(encoding="utf-8"))


def save_backup_store(store: dict[str, Any]) -> None:
    ensure_storage()
    BACKUP_FILE.write_text(json.dumps(store, indent=2), encoding="utf-8")


@dataclass
class ParsedChannel:
    kind: str
    name: str
    topic: str | None
    category: str | None


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
    precreated_categories: dict[str, discord.CategoryChannel] = {}

    if delete_channels and not create_only_missing:
        for channel in list(target.channels):
            try:
                await channel.delete(reason="CLINX backup load: delete channels")
                result["deleted_channels"] += 1
            except discord.Forbidden:
                pass

        if load_channels:
            for cat_data in snapshot.get("categories", []):
                try:
                    created_category = await target.create_category(name=cat_data["name"])
                    precreated_categories[cat_data["name"]] = created_category
                    result["created_categories"] += 1
                except discord.Forbidden:
                    continue

    if delete_roles and not create_only_missing:
        for role in sorted(target.roles, key=lambda r: r.position, reverse=True):
            if role.managed or role.is_default():
                continue
            try:
                await role.delete(reason="CLINX backup load: delete roles")
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
                    await target.create_role(
                        name=role_data["name"],
                        permissions=permissions,
                        colour=color,
                        hoist=role_data.get("hoist", False),
                        mentionable=role_data.get("mentionable", False),
                        reason="CLINX backup load: create role",
                    )
                    result["created_roles"] += 1
                except discord.Forbidden:
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
                    result["updated_roles"] += 1
                except discord.Forbidden:
                    pass

    if load_channels:
        category_map: dict[str, discord.CategoryChannel] = dict(precreated_categories)

        for cat_data in snapshot.get("categories", []):
            existing = category_map.get(cat_data["name"]) or discord.utils.get(target.categories, name=cat_data["name"])
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

        for ch_data in snapshot.get("channels", []):
            existing = discord.utils.get(target.channels, name=ch_data["name"])
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
                    result["updated_channels"] += 1
                elif isinstance(existing, discord.VoiceChannel) and ch_data["type"] == "voice":
                    await existing.edit(
                        category=category,
                        bitrate=ch_data.get("bitrate", existing.bitrate),
                        user_limit=ch_data.get("user_limit", 0),
                        overwrites=overwrites,
                    )
                    result["updated_channels"] += 1
            except discord.Forbidden:
                pass

    if load_settings and not create_only_missing:
        settings = snapshot.get("settings", {})
        try:
            await target.edit(
                verification_level=discord.VerificationLevel(settings.get("verification_level", target.verification_level.value)),
                default_notifications=discord.NotificationLevel(settings.get("default_notifications", target.default_notifications.value)),
                explicit_content_filter=discord.ContentFilter(settings.get("explicit_content_filter", target.explicit_content_filter.value)),
                afk_timeout=settings.get("afk_timeout", target.afk_timeout),
                reason="CLINX backup load: update settings",
            )
            result["updated_settings"] = 1
        except discord.Forbidden:
            pass

    return result


class LoadActionSelect(discord.ui.Select):
    def __init__(self, parent_view: "LoadConfirmView") -> None:
        self.parent_view = parent_view
        options = [
            discord.SelectOption(label="Delete Roles", description="All existing roles will be deleted", value="delete_roles"),
            discord.SelectOption(label="Delete Channels", description="All existing channels will be deleted", value="delete_channels"),
            discord.SelectOption(label="Load Roles", description="Roles from backup will be loaded", value="load_roles", default=True),
            discord.SelectOption(label="Load Channels", description="Channels from backup will be loaded", value="load_channels", default=True),
            discord.SelectOption(label="Load Settings", description="Server settings will be updated", value="load_settings", default=True),
        ]
        super().__init__(placeholder="Select actions to perform", min_values=1, max_values=len(options), options=options)

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.parent_view.author_id:
            await interaction.response.send_message("Only the command user can change this menu.", ephemeral=True)
            return
        self.parent_view.selected_actions = set(self.values)
        await interaction.response.defer()


class LoadConfirmView(discord.ui.View):
    def __init__(self, author_id: int, snapshot: dict[str, Any], target: discord.Guild):
        super().__init__(timeout=180)
        self.author_id = author_id
        self.snapshot = snapshot
        self.target = target
        self.selected_actions: set[str] = {"load_roles", "load_channels", "load_settings"}
        self.add_item(LoadActionSelect(self))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the command user can use these controls.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Continue", style=discord.ButtonStyle.success)
    async def continue_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(
            embed=make_embed("Applying Backup", "Working on selected actions...", EMBED_INFO, interaction),
            view=self,
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
            await interaction.followup.send(
                embed=make_embed("Backup Load Failed", f"Error: `{exc}`", EMBED_ERR, interaction),
                ephemeral=True,
            )
            self.stop()
            return

        summary = (
            f"Deleted roles: `{stats['deleted_roles']}`\n"
            f"Deleted channels: `{stats['deleted_channels']}`\n"
            f"Created roles: `{stats['created_roles']}`\n"
            f"Updated roles: `{stats['updated_roles']}`\n"
            f"Created categories: `{stats['created_categories']}`\n"
            f"Created channels: `{stats['created_channels']}`\n"
            f"Updated channels: `{stats['updated_channels']}`\n"
            f"Updated settings: `{stats['updated_settings']}`"
        )
        await interaction.followup.send(
            embed=make_embed("Backup Load Complete", summary, EMBED_OK, interaction),
            ephemeral=True,
        )
        self.stop()
    async def on_timeout(self) -> None:
        for child in self.children:
            child.disabled = True

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.edit_message(
            embed=make_embed("Cancelled", "Backup load was cancelled.", EMBED_WARN, interaction),
            view=None,
        )
        self.stop()


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
            self._groups_added = True

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
        "source_guild_id": source.id,
        "source_guild_name": source.name,
        "snapshot": serialize_guild_snapshot(source),
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
@app_commands.default_permissions(administrator=True)
async def backup_load(interaction: discord.Interaction, load_id: str, target_guild_id: int | None = None) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)

    store = load_backup_store()
    record = store.get("backups", {}).get(load_id)
    if record is None:
        await interaction.followup.send(embed=make_embed("Invalid Load ID", "No backup found with that ID.", EMBED_ERR), ephemeral=True)
        return

    target = interaction.guild if target_guild_id is None else bot.get_guild(target_guild_id)
    if target is None:
        await interaction.followup.send(embed=make_embed("Error", "Target guild not found.", EMBED_ERR), ephemeral=True)
        return

    warning = (
        "Select what CLINX should load from this backup.\n"
        "Use the menu, then press Continue."
    )
    view = LoadConfirmView(interaction.user.id, record["snapshot"], target)
    await interaction.followup.send(embed=make_embed("Warning", warning, EMBED_WARN), view=view, ephemeral=True)


@backup_group.command(name="list", description="List saved backup IDs")
@app_commands.default_permissions(administrator=True)
async def backup_list(interaction: discord.Interaction) -> None:
    store = load_backup_store()
    entries = list(store.get("backups", {}).values())[-20:]
    if not entries:
        await interaction.response.send_message(embed=make_embed("Backups", "No backups found.", EMBED_INFO), ephemeral=True)
        return

    lines = [f"`{e['id']}` - {e.get('source_guild_name', 'unknown')}" for e in reversed(entries)]
    await interaction.response.send_message(embed=make_embed("Backups", "\n".join(lines), EMBED_INFO), ephemeral=True)


@backup_group.command(name="delete", description="Delete a backup ID")
@app_commands.describe(load_id="Backup load ID")
@app_commands.default_permissions(administrator=True)
async def backup_delete(interaction: discord.Interaction, load_id: str) -> None:
    store = load_backup_store()
    if load_id not in store.get("backups", {}):
        await interaction.response.send_message(embed=make_embed("Error", "Load ID not found.", EMBED_ERR), ephemeral=True)
        return

    del store["backups"][load_id]
    save_backup_store(store)
    await interaction.response.send_message(embed=make_embed("Deleted", f"Removed `{load_id}`.", EMBED_OK), ephemeral=True)


@bot.tree.command(name="backupcreate", description="Alias of /backup create")
@app_commands.default_permissions(administrator=True)
async def backupcreate_alias(interaction: discord.Interaction, source_guild_id: int | None = None) -> None:
    await backup_create.callback(interaction, source_guild_id)


@bot.tree.command(name="backupload", description="Alias of /backup load")
@app_commands.default_permissions(administrator=True)
async def backupload_alias(interaction: discord.Interaction, load_id: str, target_guild_id: int | None = None) -> None:
    await backup_load.callback(interaction, load_id, target_guild_id)


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



class BoostPanelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=300)
        self.bot_select = discord.ui.Select(
            placeholder="Choose custom bot",
            min_values=1,
            max_values=1,
            options=[discord.SelectOption(label="Custom bot", value="custom_bot", emoji="🚀")],
        )
        self.bot_select.callback = self._on_select
        self.add_item(self.bot_select)

    async def _on_select(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)

    @discord.ui.button(label="Claim", style=discord.ButtonStyle.success)
    async def claim(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_message(
            embed=make_embed("Claim Submitted", "Boost reward claim received.", EMBED_OK, interaction),
            ephemeral=True,
        )


class SuggestionModal(discord.ui.Modal, title="Suggestion Form"):
    title_input = discord.ui.TextInput(label="Title", placeholder="Feature title", max_length=100)
    details_input = discord.ui.TextInput(
        label="Suggestion",
        placeholder="Describe your idea...",
        style=discord.TextStyle.paragraph,
        max_length=1500,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_message(
            embed=make_embed(
                "Suggestion Received",
                f"Title: `{self.title_input.value}`\nThanks for helping improve CLINX.",
                EMBED_OK,
                interaction,
            ),
            ephemeral=True,
        )


class SuggestionPanelView(discord.ui.View):
    @discord.ui.button(label="Suggestion", emoji="💡", style=discord.ButtonStyle.primary)
    async def open_suggestion(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(SuggestionModal())


@panel_group.command(name="boostrewards", description="Send a boost rewards panel")
async def panel_boost_rewards(interaction: discord.Interaction) -> None:
    desc = (
        "**Boost Rewards**\n\n"
        "1 Boost Rewards\n"
        "- Promote your server\n"
        "- Access to use banner and /dmall\n\n"
        "2 Boost Rewards\n"
        "- 1.1 boost rewards + DM all bot source code\n"
        "- 2.1 boost rewards + custom user install bot with host\n\n"
        "**Custom bot**\n"
        "User install bots work everywhere no need to add them to a server."
    )
    await interaction.response.send_message(
        embed=make_embed("@everyone", desc, EMBED_INFO, interaction),
        view=BoostPanelView(),
    )


@panel_group.command(name="suggestion", description="Send a suggestion form panel")
async def panel_suggestion(interaction: discord.Interaction) -> None:
    desc = (
        "Submit your idea to help improve the bot!\n\n"
        "Click the Suggestion button below to open the submission form."
    )
    await interaction.response.send_message(
        embed=make_embed("Suggestion Form", desc, EMBED_INFO, interaction),
        view=SuggestionPanelView(),
    )
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
    desc = (
        "`/backup create` `load` `list` `delete`\n"
        "`/restore_missing` `cleantoday` `masschannels`\n"
        "`/export guild|channels|channel|roles|role|message|reactions`\n"
        "`/import guild|status|cancel`\n"
        "`/panel boostrewards|suggestion`\nAliases: `/backupcreate` and `/backupload`"
    )
    await interaction.response.send_message(embed=make_embed("CLINX Help", desc, EMBED_INFO), ephemeral=True)


@bot.tree.command(name="invite", description="Get bot invite link")
async def invite(interaction: discord.Interaction) -> None:
    app_id = bot.user.id if bot.user else "YOUR_APP_ID"
    link = f"https://discord.com/oauth2/authorize?client_id={app_id}&permissions=8&integration_type=0&scope=bot+applications.commands"
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








