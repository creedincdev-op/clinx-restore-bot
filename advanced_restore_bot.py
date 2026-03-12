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
from typing import Any, Awaitable, Callable, TypeVar

import discord
from discord import app_commands
from discord.ext import commands

TOKEN = os.getenv("BOT_TOKEN")
DEFAULT_BACKUP_GUILD_ID = os.getenv("DEFAULT_BACKUP_GUILD_ID")
SUPPORT_SERVER_URL = os.getenv("SUPPORT_SERVER_URL", "https://discord.gg/V6YEw2Wxcb")

DATA_DIR = Path(__file__).parent / "data"
BACKUP_FILE = DATA_DIR / "backups.json"
SAFETY_FILE = DATA_DIR / "safety.json"

EMBED_OK = 0x2ECC71
EMBED_WARN = 0xF1C40F
EMBED_ERR = 0xE74C3C
EMBED_INFO = 0x3498DB

IMPORT_JOBS: dict[int, dict[str, Any]] = {}
PENDING_APPROVALS: dict[int, "PendingApproval"] = {}
ACTION_DELAY_SECONDS = max(0.0, float(os.getenv("DISCORD_ACTION_DELAY_SECONDS", "0.65")))
ACTION_RETRY_LIMIT = max(1, int(os.getenv("DISCORD_ACTION_RETRY_LIMIT", "5")))
ACTION_GATE_LOCK: asyncio.Lock | None = None
ACTION_LAST_DISPATCH_AT = 0.0
APPROVAL_TIMEOUT_SECONDS = 120.0
SAFETY_TIER_PUBLIC = 0
SAFETY_TIER_SAFE_ADMIN = 1
SAFETY_TIER_APPROVAL = 2
SAFETY_TIER_DESTRUCTIVE = 3
SAFETY_TIER_OWNER_ONLY = 4
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
    if not SAFETY_FILE.exists():
        SAFETY_FILE.write_text(json.dumps({"guilds": {}}, indent=2), encoding="utf-8")


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


def normalize_safety_store(store: dict[str, Any]) -> dict[str, Any]:
    guilds = store.get("guilds")
    if not isinstance(guilds, dict):
        guilds = {}
    store["guilds"] = guilds

    for guild_key, entry in list(guilds.items()):
        if not isinstance(entry, dict):
            entry = {}
            guilds[guild_key] = entry

        trusted_admin_ids = entry.get("trusted_admin_ids")
        if not isinstance(trusted_admin_ids, list):
            trusted_admin_ids = []

        normalized_ids: list[int] = []
        seen_ids: set[int] = set()
        for raw_id in trusted_admin_ids:
            try:
                trusted_admin_id = int(raw_id)
            except (TypeError, ValueError):
                continue
            if trusted_admin_id in seen_ids:
                continue
            seen_ids.add(trusted_admin_id)
            normalized_ids.append(trusted_admin_id)

        entry["trusted_admin_ids"] = normalized_ids

    return store


def load_safety_store() -> dict[str, Any]:
    ensure_storage()
    store = json.loads(SAFETY_FILE.read_text(encoding="utf-8"))
    return normalize_safety_store(store)


def save_safety_store(store: dict[str, Any]) -> None:
    ensure_storage()
    SAFETY_FILE.write_text(json.dumps(normalize_safety_store(store), indent=2), encoding="utf-8")


def get_guild_safety_entry(store: dict[str, Any], guild_id: int) -> dict[str, Any]:
    guilds = store.setdefault("guilds", {})
    return guilds.setdefault(str(guild_id), {"trusted_admin_ids": []})


def get_trusted_admin_ids(guild_id: int) -> set[int]:
    store = load_safety_store()
    entry = get_guild_safety_entry(store, guild_id)
    return {int(user_id) for user_id in entry.get("trusted_admin_ids", [])}


def add_trusted_admin(guild_id: int, user_id: int) -> None:
    store = load_safety_store()
    entry = get_guild_safety_entry(store, guild_id)
    trusted_admin_ids = {int(raw_id) for raw_id in entry.get("trusted_admin_ids", [])}
    trusted_admin_ids.add(int(user_id))
    entry["trusted_admin_ids"] = sorted(trusted_admin_ids)
    save_safety_store(store)


def remove_trusted_admin(guild_id: int, user_id: int) -> None:
    store = load_safety_store()
    entry = get_guild_safety_entry(store, guild_id)
    entry["trusted_admin_ids"] = [int(raw_id) for raw_id in entry.get("trusted_admin_ids", []) if int(raw_id) != int(user_id)]
    save_safety_store(store)


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


def has_administrator_permission(user: discord.abc.User | discord.Member) -> bool:
    permissions = getattr(user, "guild_permissions", None)
    return bool(permissions and permissions.administrator)


def is_guild_owner(user: discord.abc.User | discord.Member, guild: discord.Guild | None) -> bool:
    return guild is not None and user.id == guild.owner_id


def is_trusted_admin(user_id: int, guild_id: int) -> bool:
    return user_id in get_trusted_admin_ids(guild_id)


def is_trusted_operator(user: discord.abc.User | discord.Member, guild: discord.Guild | None) -> bool:
    if guild is None:
        return False
    if is_guild_owner(user, guild):
        return True
    return has_administrator_permission(user) and is_trusted_admin(user.id, guild.id)


def get_command_safety_tier(command_name: str, context: dict[str, Any] | None = None) -> int:
    normalized_name = command_name.casefold().strip()
    destructive = bool(context and context.get("destructive"))
    safety_map = {
        "help": SAFETY_TIER_PUBLIC,
        "invite": SAFETY_TIER_PUBLIC,
        "backup create": SAFETY_TIER_SAFE_ADMIN,
        "backup list": SAFETY_TIER_SAFE_ADMIN,
        "backup delete": SAFETY_TIER_SAFE_ADMIN,
        "backup load planner": SAFETY_TIER_SAFE_ADMIN,
        "backupcreate": SAFETY_TIER_SAFE_ADMIN,
        "backuplist": SAFETY_TIER_SAFE_ADMIN,
        "backupload planner": SAFETY_TIER_SAFE_ADMIN,
        "panel suggestion": SAFETY_TIER_SAFE_ADMIN,
        "export guild": SAFETY_TIER_SAFE_ADMIN,
        "export channels": SAFETY_TIER_SAFE_ADMIN,
        "export roles": SAFETY_TIER_SAFE_ADMIN,
        "export channel": SAFETY_TIER_SAFE_ADMIN,
        "export role": SAFETY_TIER_SAFE_ADMIN,
        "export message": SAFETY_TIER_SAFE_ADMIN,
        "export reactions": SAFETY_TIER_SAFE_ADMIN,
        "restore_missing": SAFETY_TIER_APPROVAL,
        "masschannels": SAFETY_TIER_APPROVAL,
        "masschannels modal": SAFETY_TIER_SAFE_ADMIN,
        "import guild": SAFETY_TIER_APPROVAL,
        "import status": SAFETY_TIER_SAFE_ADMIN,
        "import cancel": SAFETY_TIER_SAFE_ADMIN,
        "cleantoday": SAFETY_TIER_DESTRUCTIVE,
        "leave": SAFETY_TIER_OWNER_ONLY,
        "safety grant": SAFETY_TIER_OWNER_ONLY,
        "safety revoke": SAFETY_TIER_OWNER_ONLY,
        "safety list": SAFETY_TIER_OWNER_ONLY,
    }
    if normalized_name in {"backup load", "backupload"}:
        return SAFETY_TIER_DESTRUCTIVE if destructive else SAFETY_TIER_APPROVAL
    return safety_map.get(normalized_name, SAFETY_TIER_SAFE_ADMIN)


def require_clinx_access(
    interaction: discord.Interaction,
    command_name: str,
    *,
    context: dict[str, Any] | None = None,
) -> "ClinxAccessDecision":
    guild = interaction.guild
    user = interaction.user
    tier = get_command_safety_tier(command_name, context)

    if tier == SAFETY_TIER_PUBLIC:
        return ClinxAccessDecision(tier=tier, direct_allowed=True, requires_owner_approval=False)

    if guild is None:
        return ClinxAccessDecision(
            tier=tier,
            direct_allowed=False,
            requires_owner_approval=False,
            denial_message="Run this command in a server.",
        )

    if is_guild_owner(user, guild):
        return ClinxAccessDecision(tier=tier, direct_allowed=True, requires_owner_approval=False)

    is_admin = has_administrator_permission(user)
    trusted_operator = is_trusted_operator(user, guild)

    if tier == SAFETY_TIER_OWNER_ONLY:
        return ClinxAccessDecision(
            tier=tier,
            direct_allowed=False,
            requires_owner_approval=False,
            denial_message="Only the server owner can use this CLINX command.",
        )

    if not is_admin:
        return ClinxAccessDecision(
            tier=tier,
            direct_allowed=False,
            requires_owner_approval=False,
            denial_message="Administrator permission is required for this CLINX command.",
        )

    if tier == SAFETY_TIER_SAFE_ADMIN:
        return ClinxAccessDecision(tier=tier, direct_allowed=True, requires_owner_approval=False)

    if trusted_operator:
        return ClinxAccessDecision(tier=tier, direct_allowed=True, requires_owner_approval=False)

    return ClinxAccessDecision(tier=tier, direct_allowed=False, requires_owner_approval=True)


async def send_ephemeral_embed(
    interaction: discord.Interaction,
    title: str,
    description: str,
    color: int = EMBED_INFO,
) -> None:
    embed = make_embed(title, description, color, interaction)
    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


async def enforce_clinx_access(
    interaction: discord.Interaction,
    command_name: str,
    *,
    context: dict[str, Any] | None = None,
) -> "ClinxAccessDecision | None":
    decision = require_clinx_access(interaction, command_name, context=context)
    if decision.denial_message:
        await send_ephemeral_embed(interaction, "Access Denied", decision.denial_message, EMBED_ERR)
        return None
    return decision


def cleanup_pending_approval(guild_id: int) -> None:
    pending = PENDING_APPROVALS.get(guild_id)
    if pending is None:
        return
    if pending.expires_monotonic <= time.monotonic():
        PENDING_APPROVALS.pop(guild_id, None)


def has_pending_approval(guild_id: int) -> bool:
    cleanup_pending_approval(guild_id)
    return guild_id in PENDING_APPROVALS


def format_approval_summary(
    *,
    requester_id: int,
    command_name: str,
    target_name: str,
    route_text: str | None,
    scope_title: str,
    scope_text: str,
    risk_label: str,
) -> str:
    parts = [
        f"**Requester**\n<@{requester_id}>",
        f"**Command**\n`/{command_name}`",
        f"**Target Server**\n`{target_name}`",
    ]
    if route_text:
        parts.append(f"**Route**\n{route_text}")
    parts.append(f"**{scope_title}**\n{scope_text}")
    parts.append(f"**Risk**\n`{risk_label}`")
    return "\n\n".join(parts)


def build_approval_embed(
    pending: "PendingApproval",
    *,
    title: str,
    preview_title: str,
    preview_body: str,
    color: int,
) -> discord.Embed:
    return make_embed(
        title,
        f"{pending.summary_text}\n\n**{preview_title}**\n{preview_body}",
        color,
    )


async def update_approval_message(
    message: discord.Message,
    pending: "PendingApproval",
    *,
    title: str,
    preview_title: str,
    preview_body: str,
    color: int,
    clear_content: bool = False,
    view: discord.ui.View | None = None,
) -> None:
    await message.edit(
        content=None if clear_content else message.content,
        embed=build_approval_embed(
            pending,
            title=title,
            preview_title=preview_title,
            preview_body=preview_body,
            color=color,
        ),
        view=view,
    )


class OwnerApprovalView(discord.ui.View):
    def __init__(self, *, guild_id: int, owner_id: int, request_id: str) -> None:
        super().__init__(timeout=APPROVAL_TIMEOUT_SECONDS)
        self.guild_id = guild_id
        self.owner_id = owner_id
        self.request_id = request_id
        self.message: discord.Message | None = None

    def disable_all_items(self) -> None:
        for child in self.children:
            child.disabled = True

    def get_pending(self) -> "PendingApproval | None":
        cleanup_pending_approval(self.guild_id)
        pending = PENDING_APPROVALS.get(self.guild_id)
        if pending is None or pending.request_id != self.request_id:
            return None
        return pending

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.owner_id:
            return True
        await interaction.response.send_message("Only the server owner can approve or deny this CLINX request.", ephemeral=True)
        return False

    async def on_timeout(self) -> None:
        pending = self.get_pending()
        if pending is None:
            return

        PENDING_APPROVALS.pop(self.guild_id, None)
        self.disable_all_items()
        if self.message is None:
            return

        try:
            await update_approval_message(
                self.message,
                pending,
                title="Owner Approval Expired",
                preview_title="Request Result",
                preview_body="No owner approval was received before the request timed out.",
                color=EMBED_WARN,
                clear_content=True,
                view=self,
            )
        except discord.HTTPException:
            pass

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success)
    async def approve(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        pending = self.get_pending()
        if pending is None:
            await interaction.response.send_message("This CLINX approval request is no longer active.", ephemeral=True)
            return

        PENDING_APPROVALS.pop(self.guild_id, None)
        self.disable_all_items()
        await interaction.response.edit_message(
            content=None,
            embed=build_approval_embed(
                pending,
                title="Owner Approval Granted",
                preview_title="Execution State",
                preview_body="CLINX is running the approved action now.",
                color=BACKUP_PLANNER_ACCENT,
            ),
            view=self,
        )

        try:
            await pending.execute(interaction.message)
        except Exception as exc:
            try:
                await update_approval_message(
                    interaction.message,
                    pending,
                    title="Approved Action Failed",
                    preview_title="Failure Details",
                    preview_body=f"`{exc}`",
                    color=EMBED_ERR,
                    clear_content=True,
                    view=self,
                )
            except discord.HTTPException:
                pass

    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger)
    async def deny(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        pending = self.get_pending()
        if pending is None:
            await interaction.response.send_message("This CLINX approval request is no longer active.", ephemeral=True)
            return

        PENDING_APPROVALS.pop(self.guild_id, None)
        self.disable_all_items()
        await interaction.response.edit_message(
            content=None,
            embed=build_approval_embed(
                pending,
                title="Owner Approval Denied",
                preview_title="Request Result",
                preview_body="The server owner denied this action. Nothing was changed.",
                color=EMBED_ERR,
            ),
            view=self,
        )


async def queue_owner_approval(
    interaction: discord.Interaction,
    *,
    command_name: str,
    tier: int,
    risk_label: str,
    summary_text: str,
    preview_title: str,
    preview_body: str,
    execute: Callable[[discord.Message], Awaitable[None]],
) -> "PendingApproval | None":
    guild = interaction.guild
    channel = interaction.channel
    if guild is None or channel is None or not hasattr(channel, "send"):
        await send_ephemeral_embed(interaction, "Approval Error", "CLINX could not open an owner approval request in this channel.", EMBED_ERR)
        return None

    if has_pending_approval(guild.id):
        pending = PENDING_APPROVALS[guild.id]
        await send_ephemeral_embed(
            interaction,
            "Approval Pending",
            f"`/{pending.command_name}` is already waiting for the server owner in this guild. Resolve that request first.",
            EMBED_WARN,
        )
        return None

    pending = PendingApproval(
        request_id=secrets.token_hex(4).upper(),
        guild_id=guild.id,
        owner_id=guild.owner_id,
        requester_id=interaction.user.id,
        command_name=command_name,
        tier=tier,
        risk_label=risk_label,
        summary_text=summary_text,
        preview_title=preview_title,
        preview_body=preview_body,
        execute=execute,
        created_monotonic=time.monotonic(),
        expires_monotonic=time.monotonic() + APPROVAL_TIMEOUT_SECONDS,
    )
    view = OwnerApprovalView(guild_id=guild.id, owner_id=guild.owner_id, request_id=pending.request_id)
    approval_message = await channel.send(
        content=f"<@{guild.owner_id}> CLINX approval required for <@{interaction.user.id}>.",
        embed=build_approval_embed(
            pending,
            title="Owner Approval Required",
            preview_title=preview_title,
            preview_body=preview_body,
            color=EMBED_WARN if tier == SAFETY_TIER_APPROVAL else EMBED_ERR,
        ),
        view=view,
        allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
    )
    view.message = approval_message
    pending.message_id = approval_message.id
    PENDING_APPROVALS[guild.id] = pending
    return pending


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


@dataclass
class ClinxAccessDecision:
    tier: int
    direct_allowed: bool
    requires_owner_approval: bool
    denial_message: str | None = None


@dataclass
class PendingApproval:
    request_id: str
    guild_id: int
    owner_id: int
    requester_id: int
    command_name: str
    tier: int
    risk_label: str
    summary_text: str
    preview_title: str
    preview_body: str
    execute: Callable[[discord.Message], Awaitable[None]]
    created_monotonic: float
    expires_monotonic: float
    message_id: int | None = None


COMMAND_PAGE_SIZE = 5
LIBRARY_ACCENT = 0x5B7CFF
SUGGESTION_ACCENT = 0x39D0C8
BACKUP_PLANNER_ACCENT = 0x6CCBFF
BACKUP_ACTION_ORDER = (
    "load_roles",
    "load_channels",
    "load_settings",
    "delete_roles",
    "delete_channels",
)
BACKUP_ACTION_LABELS = {
    "delete_roles": "Delete Roles",
    "delete_channels": "Delete Channels",
    "load_roles": "Load Roles",
    "load_channels": "Load Channels",
    "load_settings": "Load Settings",
}
BACKUP_DELETE_DEPENDENCIES = {
    "delete_roles": "load_roles",
    "delete_channels": "load_channels",
}
COMMAND_LIBRARY: dict[str, list[LibraryCommand]] = {
    "Backup": [
        LibraryCommand("backup create", "Create a fresh server backup and return its load ID.", "Snapshots the selected source guild, stores it in the CLINX backup DB, and tags the creator in the backup record."),
        LibraryCommand("backup load", "Open the restore planner for one of your backups.", "The load ID field autocompletes only backups created by your account. Untrusted admins need owner approval before protected restore plans execute."),
        LibraryCommand("backup list", "Show only the backups created by your account.", "Lists your own recent backup IDs with source guild names and exact creation timestamps."),
        LibraryCommand("backup delete", "Delete one of your backup codes from the DB.", "Removes a backup only if it belongs to your account and unregisters it from your stored backup list."),
        LibraryCommand("backupcreate", "Alias of /backup create.", "Shortcut alias for fast backup creation."),
        LibraryCommand("backupload", "Alias of /backup load.", "Shortcut alias for opening the backup restore planner."),
        LibraryCommand("backuplist", "Alias of /backup list.", "Shortcut alias for opening your private backup list anywhere CLINX is installed."),
    ],
    "Transfer": [
        LibraryCommand("restore_missing", "Create only missing categories and channels.", "Compares the source guild snapshot against the target guild and creates only what is absent. Untrusted admins need owner approval."),
        LibraryCommand("cleantoday", "Delete channels created today.", "Runs as a dry-run first unless confirm is enabled, then deletes only channels created on the current UTC date."),
        LibraryCommand("masschannels", "Open the bulk layout importer modal.", "Paste one or two large layout blocks and CLINX will infer categories, text channels, voice channels, and channel topics. Untrusted admins need owner approval to execute."),
        LibraryCommand("panel suggestion", "Post the public suggestion board.", "Sends a public CLINX suggestion board to the current channel while keeping the command acknowledgement private. Admin access is required."),
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
        LibraryCommand("leave", "Make CLINX leave the current server.", "Only the server owner can use this when you intentionally want the bot removed from a guild."),
        LibraryCommand("safety grant", "Owner-only: trust an admin for protected CLINX actions.", "Adds a guild-specific trusted admin who can bypass owner approval for Tier 2 and Tier 3 actions."),
        LibraryCommand("safety revoke", "Owner-only: remove trusted admin access.", "Removes a guild-specific trusted admin from the CLINX safety allowlist."),
        LibraryCommand("safety list", "Owner-only: list the CLINX trusted admins.", "Shows which members in the current guild can bypass owner approval for protected actions."),
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


def get_action_gate_lock() -> asyncio.Lock:
    global ACTION_GATE_LOCK
    if ACTION_GATE_LOCK is None:
        ACTION_GATE_LOCK = asyncio.Lock()
    return ACTION_GATE_LOCK


async def throttled_discord_call(factory: Callable[[], Awaitable[T]]) -> T:
    global ACTION_LAST_DISPATCH_AT
    last_error: discord.HTTPException | None = None

    for _ in range(ACTION_RETRY_LIMIT):
        retry_after: float | None = None
        async with get_action_gate_lock():
            if ACTION_DELAY_SECONDS > 0:
                elapsed = time.monotonic() - ACTION_LAST_DISPATCH_AT
                if elapsed < ACTION_DELAY_SECONDS:
                    await asyncio.sleep(ACTION_DELAY_SECONDS - elapsed)

            try:
                result = await factory()
            except discord.HTTPException as exc:
                ACTION_LAST_DISPATCH_AT = time.monotonic()
                if exc.status != 429:
                    raise
                last_error = exc
                retry_after = extract_retry_after(exc, max(ACTION_DELAY_SECONDS, 0.75))
            else:
                ACTION_LAST_DISPATCH_AT = time.monotonic()
                return result

        if retry_after is not None:
            await asyncio.sleep(retry_after)

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


async def serialize_asset(asset: discord.Asset | None) -> str | None:
    if asset is None:
        return None
    try:
        return base64.b64encode(await asset.read()).decode("ascii")
    except discord.HTTPException:
        return None


def serialize_channel_ref(channel: discord.abc.GuildChannel | None) -> str | None:
    return channel.name if channel is not None else None


async def serialize_settings(guild: discord.Guild, *, include_assets: bool = True) -> dict[str, Any]:
    icon_image = await serialize_asset(guild.icon) if include_assets else None
    banner_image = await serialize_asset(guild.banner) if include_assets else None
    splash_image = await serialize_asset(guild.splash) if include_assets else None
    discovery_splash_image = await serialize_asset(guild.discovery_splash) if include_assets else None
    return {
        "name": guild.name,
        "description": guild.description,
        "icon_image": icon_image,
        "banner_image": banner_image,
        "splash_image": splash_image,
        "discovery_splash_image": discovery_splash_image,
        "verification_level": int(guild.verification_level.value),
        "default_notifications": int(guild.default_notifications.value),
        "explicit_content_filter": int(guild.explicit_content_filter.value),
        "afk_timeout": guild.afk_timeout,
        "preferred_locale": str(guild.preferred_locale),
        "premium_progress_bar_enabled": bool(guild.premium_progress_bar_enabled),
        "system_channel": serialize_channel_ref(guild.system_channel),
        "system_channel_flags": guild.system_channel_flags.value,
        "rules_channel": serialize_channel_ref(guild.rules_channel),
        "public_updates_channel": serialize_channel_ref(guild.public_updates_channel),
        "afk_channel": serialize_channel_ref(guild.afk_channel),
    }


async def serialize_guild_snapshot(guild: discord.Guild, *, include_assets: bool = True) -> dict[str, Any]:
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
        "source_guild_id": guild.id,
        "source_guild_name": guild.name,
        "roles": serialize_roles(guild),
        "settings": await serialize_settings(guild, include_assets=include_assets),
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


def decode_snapshot_asset(payload: Any) -> bytes | None:
    if not payload:
        return None
    try:
        return base64.b64decode(payload)
    except (ValueError, TypeError):
        return None


def resolve_text_channel_by_name(guild: discord.Guild, channel_name: str | None) -> discord.TextChannel | None:
    if not channel_name:
        return None
    return discord.utils.get(guild.text_channels, name=channel_name)


def resolve_voice_channel_by_name(guild: discord.Guild, channel_name: str | None) -> discord.VoiceChannel | None:
    if not channel_name:
        return None
    return discord.utils.get(guild.voice_channels, name=channel_name)


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

    if load_settings and not create_only_missing:
        settings = snapshot.get("settings", {})
        edit_kwargs: dict[str, Any] = {
            "name": settings.get("name", target.name),
            "description": settings.get("description"),
            "verification_level": discord.VerificationLevel(settings.get("verification_level", target.verification_level.value)),
            "default_notifications": discord.NotificationLevel(settings.get("default_notifications", target.default_notifications.value)),
            "explicit_content_filter": discord.ContentFilter(settings.get("explicit_content_filter", target.explicit_content_filter.value)),
            "afk_timeout": settings.get("afk_timeout", target.afk_timeout),
            "afk_channel": resolve_voice_channel_by_name(target, settings.get("afk_channel")),
            "system_channel": resolve_text_channel_by_name(target, settings.get("system_channel")),
            "rules_channel": resolve_text_channel_by_name(target, settings.get("rules_channel")),
            "public_updates_channel": resolve_text_channel_by_name(target, settings.get("public_updates_channel")),
            "premium_progress_bar_enabled": bool(settings.get("premium_progress_bar_enabled", target.premium_progress_bar_enabled)),
            "reason": "CLINX backup load: update settings",
        }

        preferred_locale = settings.get("preferred_locale")
        if preferred_locale:
            try:
                edit_kwargs["preferred_locale"] = discord.Locale(preferred_locale)
            except ValueError:
                pass

        system_channel_flags = settings.get("system_channel_flags")
        if system_channel_flags is not None:
            try:
                edit_kwargs["system_channel_flags"] = discord.SystemChannelFlags._from_value(int(system_channel_flags))
            except (TypeError, ValueError):
                pass

        settings_applied = False
        try:
            await throttled_discord_call(lambda edit_kwargs=edit_kwargs: target.edit(**edit_kwargs))
            settings_applied = True
        except discord.Forbidden:
            pass

        icon_bytes = decode_snapshot_asset(settings.get("icon_image"))
        banner_bytes = decode_snapshot_asset(settings.get("banner_image"))
        splash_bytes = decode_snapshot_asset(settings.get("splash_image"))
        discovery_splash_bytes = decode_snapshot_asset(settings.get("discovery_splash_image"))

        for asset_key, asset_bytes in (
            ("icon", icon_bytes),
            ("banner", banner_bytes),
            ("splash", splash_bytes),
            ("discovery_splash", discovery_splash_bytes),
        ):
            if asset_key not in settings and asset_bytes is None:
                continue
            try:
                await throttled_discord_call(
                    lambda asset_key=asset_key, asset_bytes=asset_bytes: target.edit(
                        **{asset_key: asset_bytes},
                        reason=f"CLINX backup load: update {asset_key.replace('_', ' ')}",
                    )
                )
                settings_applied = True
            except (discord.Forbidden, discord.HTTPException):
                continue

        if settings_applied:
            result["updated_settings"] = 1

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


def preview_mass_channels_layout(
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

    category_cache: set[str] = {category.name for category in guild.categories}
    seen_channels: set[tuple[str | None, str]] = set()

    for item in items:
        if item.category and item.category not in category_cache and create_categories:
            category_cache.add(item.category)
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

        result["created_channels"] += 1

    return result


def build_restore_missing_preview(snapshot: dict[str, Any], target: discord.Guild) -> dict[str, int]:
    preview = {
        "created_categories": 0,
        "created_channels": 0,
        "conflicting_channels": 0,
    }
    existing_categories = {category.name for category in target.categories}
    existing_channels = {channel.name: channel for channel in target.channels}

    for category_data in snapshot.get("categories", []):
        if category_data["name"] not in existing_categories:
            preview["created_categories"] += 1

    for channel_data in snapshot.get("channels", []):
        existing = existing_channels.get(channel_data["name"])
        if existing is None:
            preview["created_channels"] += 1
        elif str(existing.type) != channel_data["type"]:
            preview["conflicting_channels"] += 1

    return preview


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
        "deleted_roles": "live roles cleared before rebuild",
        "created_roles": "roles created from backup",
        "updated_roles": "roles updated in place",
        "deleted_channels": "live channels cleared before rebuild",
        "created_categories": "categories created",
        "created_channels": "channels created",
        "updated_channels": "channels updated in place",
        "updated_settings": "server profile synced",
        "conflicting_channels": "channel type conflicts need review",
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
            lines.append(f"• **{value}** {label_map[key]}")

    if not lines:
        return "• No restore lanes are armed yet."

    return "\n".join(lines)


def normalize_backup_actions(selected_actions: set[str]) -> set[str]:
    normalized = set(selected_actions)
    for delete_action, load_action in BACKUP_DELETE_DEPENDENCIES.items():
        if load_action not in normalized:
            normalized.discard(delete_action)
    return normalized


def is_backup_action_available(selected_actions: set[str], action_key: str) -> bool:
    required = BACKUP_DELETE_DEPENDENCIES.get(action_key)
    if required is None:
        return True
    return required in selected_actions


def render_backup_lane_lines(selected_actions: set[str]) -> str:
    lines: list[str] = []
    if "load_roles" in selected_actions:
        role_mode = "replace the live role stack from backup" if "delete_roles" in selected_actions else "merge backup roles into the live stack"
        lines.append(f"• **Roles Lane**: {role_mode}")
    if "load_channels" in selected_actions:
        channel_mode = "wipe the live channel tree, then rebuild from backup" if "delete_channels" in selected_actions else "create and update the channel tree from backup"
        lines.append(f"• **Channels Lane**: {channel_mode}")
    if "load_settings" in selected_actions:
        lines.append("• **Settings Lane**: sync guild profile and server configuration")
    if not lines:
        return "• No restore lanes are armed."
    return "\n".join(lines)


def render_backup_guardrail_lines(selected_actions: set[str]) -> str:
    lines: list[str] = []
    if "load_roles" not in selected_actions:
        lines.append("• `Delete Roles` unlocks only after `Load Roles` is enabled.")
    elif "delete_roles" in selected_actions:
        lines.append("• Roles will be wiped first, then rebuilt from the backup snapshot.")

    if "load_channels" not in selected_actions:
        lines.append("• `Delete Channels` unlocks only after `Load Channels` is enabled.")
    elif "delete_channels" in selected_actions:
        lines.append("• Channels will be wiped first, then the backup layout will be rebuilt.")

    if not lines:
        if all(load_action in selected_actions for load_action in BACKUP_DELETE_DEPENDENCIES.values()):
            lines.append("• Matching rebuild lanes are armed. Optional wipe lanes are now available if you want a full replace instead of a merge.")
        else:
            lines.append("• Destructive wipes stay locked until a matching rebuild lane is armed.")
    return "\n".join(lines)


def render_backup_settings_lines(snapshot: dict[str, Any], selected_actions: set[str]) -> str:
    if "load_settings" not in selected_actions:
        return "• Settings lane is currently off."

    settings = snapshot.get("settings", {})
    lines = [
        "• **Core Profile**: name, description, verification, notifications, content filter, AFK, locale, and progress bar",
    ]

    if settings.get("icon_image"):
        lines.append("• **Server Icon** will sync from the backup snapshot")
    if settings.get("banner_image"):
        lines.append("• **Server Banner** will sync from the backup snapshot")
    if settings.get("splash_image"):
        lines.append("• **Invite Splash** will sync from the backup snapshot")
    if settings.get("discovery_splash_image"):
        lines.append("• **Discovery Splash** will sync from the backup snapshot")

    linked_channels: list[str] = []
    if settings.get("system_channel"):
        linked_channels.append("system channel")
    if settings.get("rules_channel"):
        linked_channels.append("rules channel")
    if settings.get("public_updates_channel"):
        linked_channels.append("updates channel")
    if settings.get("afk_channel"):
        linked_channels.append("AFK channel")
    if linked_channels:
        lines.append(f"• **Linked Channels**: {', '.join(linked_channels)}")

    return "\n".join(lines)


def render_backup_result_lines(stats: dict[str, int]) -> str:
    lines = [
        f"• **{stats['deleted_roles']}** roles deleted",
        f"• **{stats['created_roles']}** roles created",
        f"• **{stats['updated_roles']}** roles updated",
        f"• **{stats['deleted_channels']}** channels deleted",
        f"• **{stats['created_categories']}** categories created",
        f"• **{stats['created_channels']}** channels created",
        f"• **{stats['updated_channels']}** channels updated",
        f"• **{stats['updated_settings']}** settings synced",
    ]
    return "\n".join(lines)


def build_backup_operation_embed(
    *,
    title: str,
    backup_id: str,
    source_name: str,
    target_name: str,
    selected_actions: set[str],
    changes_title: str,
    changes_body: str,
    color: int,
) -> discord.Embed:
    return make_embed(
        title,
        (
            f"**Backup Vault**\n`{backup_id}`\n\n"
            f"**Route**\n`{source_name}` -> `{target_name}`\n\n"
            f"**Restore Lanes**\n{render_backup_lane_lines(selected_actions)}\n\n"
            f"**{changes_title}**\n{changes_body}"
        ),
        color,
    )


async def run_backup_load_operation(
    *,
    backup_id: str,
    source_name: str,
    snapshot: dict[str, Any],
    target: discord.Guild,
    selected_actions: set[str],
    private_interaction: discord.Interaction | None = None,
    public_status_message: discord.Message | None = None,
) -> None:
    plan = build_backup_plan_preview(snapshot, target, selected_actions)

    if private_interaction is not None:
        await private_interaction.edit_original_response(
            view=BackupLoadStatusView(
                title="## `<>` Applying Backup",
                subtitle=f"CLINX is rebuilding `{target.name}` from backup `{backup_id}`.",
                body="Write pacing is active. Roles, channels, and server settings are being restored in a controlled sequence to avoid Discord rate spikes.",
                accent_color=BACKUP_PLANNER_ACCENT,
            )
        )

    if public_status_message is not None:
        await public_status_message.edit(
            content=None,
            embed=build_backup_operation_embed(
                title="Backup Load Started",
                backup_id=backup_id,
                source_name=source_name,
                target_name=target.name,
                selected_actions=selected_actions,
                changes_title="Planned Changes",
                changes_body=render_backup_plan_lines(plan),
                color=BACKUP_PLANNER_ACCENT,
            ),
            view=None,
        )

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
    except Exception as exc:
        if public_status_message is not None:
            try:
                await public_status_message.edit(
                    embed=build_backup_operation_embed(
                        title="Backup Load Failed",
                        backup_id=backup_id,
                        source_name=source_name,
                        target_name=target.name,
                        selected_actions=selected_actions,
                        changes_title="Failure Details",
                        changes_body=f"`{exc}`",
                        color=EMBED_ERR,
                    ),
                    view=None,
                )
            except discord.HTTPException:
                pass
        if private_interaction is not None:
            await private_interaction.edit_original_response(
                view=BackupLoadStatusView(
                    title="## `x` Backup Load Failed",
                    subtitle=f"Backup `{backup_id}` did not finish cleanly.",
                    body=f"`{exc}`",
                    accent_color=EMBED_ERR,
                )
            )
        if private_interaction is not None:
            raise
        return

    result_body = render_backup_result_lines(stats)
    if public_status_message is not None:
        try:
            await public_status_message.edit(
                embed=build_backup_operation_embed(
                    title="Backup Load Complete",
                    backup_id=backup_id,
                    source_name=source_name,
                    target_name=target.name,
                    selected_actions=selected_actions,
                    changes_title="Applied Changes",
                    changes_body=result_body,
                    color=EMBED_OK,
                ),
                view=None,
            )
        except discord.HTTPException:
            pass

    if private_interaction is not None:
        await private_interaction.edit_original_response(
            view=BackupLoadStatusView(
                title="## `<>` Backup Rebuild Complete",
                subtitle=f"`{target.name}` now matches the selected lanes from backup `{backup_id}`.",
                body=result_body,
                accent_color=EMBED_OK,
            )
        )


def render_backup_detail_lines(
    snapshot: dict[str, Any],
    source_name: str,
    target: discord.Guild,
    selected_actions: set[str],
    plan: dict[str, int],
) -> str:
    detail_lines = [
        f"**Source Server**\n`{snapshot.get('source_guild_name', source_name)}`",
        f"**Target Server**\n`{target.name}`",
        f"**Restore Lanes**\n{render_backup_lane_lines(selected_actions)}",
        f"**Snapshot Scope**\nRoles `{len(snapshot.get('roles', []))}` | Categories `{len(snapshot.get('categories', []))}` | Channels `{len(snapshot.get('channels', []))}`",
        f"**Settings Scope**\n{render_backup_settings_lines(snapshot, selected_actions)}",
    ]
    if plan.get("conflicting_channels"):
        detail_lines.append("**Conflict Watch**\nSome existing channel names already use a different channel type. CLINX will not auto-convert those lanes.")
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
        normalized_actions = normalize_backup_actions(selected_actions)
        is_selected = action_key in normalized_actions
        is_destructive = action_key.startswith("delete_")
        is_available = is_backup_action_available(normalized_actions, action_key)
        style = discord.ButtonStyle.secondary
        if is_selected and is_destructive:
            style = discord.ButtonStyle.danger
        elif is_selected:
            style = discord.ButtonStyle.primary

        super().__init__(label=BACKUP_ACTION_LABELS[action_key], style=style, disabled=not is_available)
        self.backup_id = backup_id
        self.source_name = source_name
        self.author_id = author_id
        self.snapshot = snapshot
        self.target = target
        self.selected_actions = normalized_actions
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
        next_actions = normalize_backup_actions(next_actions)

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


async def execute_backup_load_request(
    interaction: discord.Interaction,
    *,
    backup_id: str,
    source_name: str,
    snapshot: dict[str, Any],
    target: discord.Guild,
    selected_actions: set[str],
) -> None:
    destructive = any(action in selected_actions for action in BACKUP_DELETE_DEPENDENCIES)
    decision = require_clinx_access(
        interaction,
        "backup load",
        context={"destructive": destructive},
    )
    if decision.denial_message:
        await interaction.response.send_message(decision.denial_message, ephemeral=True)
        return

    plan = build_backup_plan_preview(snapshot, target, selected_actions)
    if decision.requires_owner_approval:
        route_text = f"`{source_name}` -> `{target.name}`"
        risk_label = "Destructive" if destructive else "Non-Destructive"
        summary_text = format_approval_summary(
            requester_id=interaction.user.id,
            command_name="backup load",
            target_name=target.name,
            route_text=route_text,
            scope_title="Selected Actions",
            scope_text=", ".join(BACKUP_ACTION_LABELS[action] for action in BACKUP_ACTION_ORDER if action in selected_actions) or "No actions selected",
            risk_label=risk_label,
        )
        if has_pending_approval(interaction.guild.id):
            await interaction.response.send_message("Another protected CLINX request is already waiting for the owner in this server.", ephemeral=True)
            return
        await interaction.response.edit_message(
            view=BackupLoadStatusView(
                title="## `<>` Waiting For Owner Approval",
                subtitle="The restore plan is staged and waiting for the server owner.",
                body="CLINX will execute this backup load only after the owner approves the public request card in this channel.",
                accent_color=EMBED_WARN,
            )
        )

        async def execute_backup_load(message: discord.Message) -> None:
            await run_backup_load_operation(
                backup_id=backup_id,
                source_name=source_name,
                snapshot=snapshot,
                target=target,
                selected_actions=set(selected_actions),
                public_status_message=message,
            )

        queued = await queue_owner_approval(
            interaction,
            command_name="backup load",
            tier=decision.tier,
            risk_label=risk_label,
            summary_text=summary_text,
            preview_title="Planned Changes",
            preview_body=render_backup_plan_lines(plan),
            execute=execute_backup_load,
        )
        if queued is None:
            await interaction.edit_original_response(
                view=BackupLoadStatusView(
                    title="## `x` Approval Request Failed",
                    subtitle="CLINX could not queue the owner approval request.",
                    body="Resolve any existing approval card in this guild, then reopen the planner and try again.",
                    accent_color=EMBED_ERR,
                )
            )
        return

    await interaction.response.edit_message(
        view=BackupLoadStatusView(
            title="## `<>` Applying Backup",
            subtitle=f"CLINX is rebuilding `{target.name}` from backup `{backup_id}`.",
            body="Write pacing is active. Roles, channels, and server settings are being restored in a controlled sequence to avoid Discord rate spikes.",
            accent_color=BACKUP_PLANNER_ACCENT,
        )
    )
    public_status_message: discord.Message | None = None
    channel = interaction.channel
    if interaction.guild is not None and channel is not None and hasattr(channel, "send"):
        try:
            public_status_message = await channel.send(
                embed=build_backup_operation_embed(
                    title="Backup Load Started",
                    backup_id=backup_id,
                    source_name=source_name,
                    target_name=target.name,
                    selected_actions=selected_actions,
                    changes_title="Planned Changes",
                    changes_body=render_backup_plan_lines(plan),
                    color=BACKUP_PLANNER_ACCENT,
                ),
                allowed_mentions=discord.AllowedMentions.none(),
            )
        except discord.HTTPException:
            public_status_message = None

    try:
        await run_backup_load_operation(
            backup_id=backup_id,
            source_name=source_name,
            snapshot=snapshot,
            target=target,
            selected_actions=selected_actions,
            private_interaction=interaction,
            public_status_message=public_status_message,
        )
    except Exception:
        return


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
            label="Review Changes",
            style=discord.ButtonStyle.primary,
            disabled=not selected_actions,
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
            view=BackupLoadReviewView(
                backup_id=self.backup_id,
                source_name=self.source_name,
                author_id=self.author_id,
                snapshot=self.snapshot,
                target=self.target,
                selected_actions=self.selected_actions,
            )
        )


class BackupLoadBackButton(discord.ui.Button["BackupLoadReviewView"]):
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
        super().__init__(label="Back", style=discord.ButtonStyle.secondary)
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
            view=BackupLoadPlannerView(
                backup_id=self.backup_id,
                source_name=self.source_name,
                author_id=self.author_id,
                snapshot=self.snapshot,
                target=self.target,
                selected_actions=self.selected_actions,
            )
        )


class BackupLoadConfirmButton(discord.ui.Button["BackupLoadReviewView"]):
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
        super().__init__(label="Confirm Apply", style=discord.ButtonStyle.success, disabled=not selected_actions)
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

        await execute_backup_load_request(
            interaction,
            backup_id=self.backup_id,
            source_name=self.source_name,
            snapshot=self.snapshot,
            target=self.target,
            selected_actions=self.selected_actions,
        )


class BackupLoadCancelButton(discord.ui.Button["BackupLoadPlannerView"]):
    def __init__(self) -> None:
        super().__init__(label="Cancel", style=discord.ButtonStyle.danger)

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.edit_message(
            view=BackupLoadStatusView(
                title="## `-` Backup Plan Cancelled",
                subtitle="No changes were applied.",
                body="Re-run `/backup load` whenever you want to reopen the planner.",
                accent_color=EMBED_WARN,
            )
        )


class BackupLoadReviewView(discord.ui.LayoutView):
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
        super().__init__(timeout=300)
        plan = build_backup_plan_preview(snapshot, target, selected_actions)
        destructive = any(action in selected_actions for action in BACKUP_DELETE_DEPENDENCIES)
        risk_label = "Destructive" if destructive else "Non-Destructive"
        selected_count = len(selected_actions)

        self.add_item(
            discord.ui.Container(
                discord.ui.TextDisplay("## `<>` Backup Load Confirmation"),
                discord.ui.TextDisplay("Final review. The next confirmation is the point where CLINX can touch the live server."),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay(f"### Backup Vault\n`{backup_id}`"),
                    discord.ui.TextDisplay(f"### Route\n`{source_name}` -> `{target.name}`"),
                    accessory=discord.ui.Button(
                        label=risk_label,
                        style=discord.ButtonStyle.danger if destructive else discord.ButtonStyle.secondary,
                        disabled=True,
                    ),
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay(f"### Selected Lanes\n{render_backup_lane_lines(selected_actions)}"),
                    discord.ui.TextDisplay(f"### Projected Changes\n{render_backup_plan_lines(plan)}"),
                    accessory=discord.ui.Button(
                        label=f"{selected_count} staged",
                        style=discord.ButtonStyle.secondary,
                        disabled=True,
                    ),
                ),
                discord.ui.Separator(),
                discord.ui.TextDisplay(
                    "### Final Check\nConfirm once more to start the blue live apply card. If owner approval is required, CLINX will send the public approval request after this step."
                ),
                accent_color=BACKUP_PLANNER_ACCENT,
            )
        )
        self.add_item(
            discord.ui.ActionRow(
                BackupLoadBackButton(
                    backup_id=backup_id,
                    source_name=source_name,
                    author_id=author_id,
                    snapshot=snapshot,
                    target=target,
                    selected_actions=selected_actions,
                ),
                BackupLoadConfirmButton(
                    backup_id=backup_id,
                    source_name=source_name,
                    author_id=author_id,
                    snapshot=snapshot,
                    target=target,
                    selected_actions=selected_actions,
                ),
                BackupLoadCancelButton(),
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
        self.selected_actions = normalize_backup_actions(
            set(selected_actions or {"load_roles", "load_channels", "load_settings", "delete_roles", "delete_channels"})
        )
        self.detail_mode = detail_mode

        plan = build_backup_plan_preview(snapshot, target, self.selected_actions)
        selected_count = len(self.selected_actions)
        run_mode_label = "Wipe + Rebuild" if any(action in self.selected_actions for action in BACKUP_DELETE_DEPENDENCIES) else "Rebuild Only"
        snapshot_scope = (
            f"### Snapshot Scope\n"
            f"• **{len(snapshot.get('roles', []))}** roles in backup\n"
            f"• **{len(snapshot.get('categories', []))}** categories in backup\n"
            f"• **{len(snapshot.get('channels', []))}** channels in backup"
        )
        detail_block = (
            render_backup_detail_lines(snapshot, source_name, target, self.selected_actions, plan)
            if self.detail_mode
            else "**Detail View**\nOpen `View Detail` to inspect the source server, target server, and conflict notes before you run the load."
        )

        self.add_item(
            discord.ui.Container(
                discord.ui.TextDisplay("## `<>` Backup Load Planner"),
                discord.ui.TextDisplay(
                    "Pick the restore lanes here. Projected changes appear on the next confirmation step, not on this stage."
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay(f"### Backup Vault\n`{backup_id}`"),
                    discord.ui.TextDisplay(f"### Route\n`{source_name}` -> `{target.name}`"),
                    accessory=discord.ui.Button(
                        label=f"{selected_count} staged",
                        style=discord.ButtonStyle.secondary,
                        disabled=True,
                    ),
                ),
                discord.ui.Separator(),
                discord.ui.Section(
                    discord.ui.TextDisplay(f"### Restore Lanes\n{render_backup_lane_lines(self.selected_actions)}"),
                    discord.ui.TextDisplay(snapshot_scope),
                    accessory=discord.ui.Button(
                        label=run_mode_label,
                        style=discord.ButtonStyle.secondary,
                        disabled=True,
                    ),
                ),
                discord.ui.Separator(),
                discord.ui.TextDisplay(f"### Settings Payload\n{render_backup_settings_lines(snapshot, self.selected_actions)}"),
                discord.ui.Separator(),
                discord.ui.TextDisplay(f"### Safety Locks\n{render_backup_guardrail_lines(self.selected_actions)}"),
                discord.ui.Separator(),
                discord.ui.TextDisplay(detail_block),
                accent_color=BACKUP_PLANNER_ACCENT,
            )
        )

        self.add_item(
            discord.ui.ActionRow(
                BackupLoadActionButton(
                    backup_id=backup_id,
                    source_name=source_name,
                    author_id=author_id,
                    snapshot=snapshot,
                    target=target,
                    selected_actions=self.selected_actions,
                    detail_mode=detail_mode,
                    action_key="load_roles",
                ),
                BackupLoadActionButton(
                    backup_id=backup_id,
                    source_name=source_name,
                    author_id=author_id,
                    snapshot=snapshot,
                    target=target,
                    selected_actions=self.selected_actions,
                    detail_mode=detail_mode,
                    action_key="load_channels",
                ),
                BackupLoadActionButton(
                    backup_id=backup_id,
                    source_name=source_name,
                    author_id=author_id,
                    snapshot=snapshot,
                    target=target,
                    selected_actions=self.selected_actions,
                    detail_mode=detail_mode,
                    action_key="load_settings",
                ),
            )
        )

        self.add_item(
            discord.ui.ActionRow(
                BackupLoadActionButton(
                    backup_id=backup_id,
                    source_name=source_name,
                    author_id=author_id,
                    snapshot=snapshot,
                    target=target,
                    selected_actions=self.selected_actions,
                    detail_mode=detail_mode,
                    action_key="delete_roles",
                ),
                BackupLoadActionButton(
                    backup_id=backup_id,
                    source_name=source_name,
                    author_id=author_id,
                    snapshot=snapshot,
                    target=target,
                    selected_actions=self.selected_actions,
                    detail_mode=detail_mode,
                    action_key="delete_channels",
                ),
                BackupLoadDetailButton(
                    backup_id=backup_id,
                    source_name=source_name,
                    author_id=author_id,
                    snapshot=snapshot,
                    target=target,
                    selected_actions=self.selected_actions,
                    detail_mode=detail_mode,
                ),
            )
        )
        self.add_item(
            discord.ui.ActionRow(
                BackupLoadContinueButton(
                    backup_id=backup_id,
                    source_name=source_name,
                    author_id=author_id,
                    snapshot=snapshot,
                    target=target,
                    selected_actions=self.selected_actions,
                ),
                BackupLoadCancelButton(),
            )
        )


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
            self.tree.add_command(safety_group)
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
safety_group = app_commands.Group(name="safety", description="Owner-only CLINX safety controls")


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
    if await enforce_clinx_access(interaction, "backup create") is None:
        return
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
        "snapshot": await serialize_guild_snapshot(source),
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
    if await enforce_clinx_access(interaction, "backup load planner") is None:
        return
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
    if await enforce_clinx_access(interaction, "backup list") is None:
        return
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
    if await enforce_clinx_access(interaction, "backup delete") is None:
        return
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
    decision = await enforce_clinx_access(interaction, "restore_missing")
    if decision is None:
        return
    await interaction.response.defer(thinking=True)

    resolved_source = source_guild_id or resolve_default_backup_guild_id()
    source = bot.get_guild(resolved_source) if resolved_source else None
    target = interaction.guild if target_guild_id is None else bot.get_guild(target_guild_id)

    if source is None or target is None:
        await interaction.followup.send(embed=make_embed("Error", "Could not resolve source or target guild.", EMBED_ERR), ephemeral=True)
        return

    snapshot = await serialize_guild_snapshot(source, include_assets=False)
    preview = build_restore_missing_preview(snapshot, target)
    preview_lines = [
        f"• **{preview['created_categories']}** missing categories will be created",
        f"• **{preview['created_channels']}** missing channels will be created",
    ]
    if preview["conflicting_channels"]:
        preview_lines.append(f"• **{preview['conflicting_channels']}** channel names already exist with a different type and will be skipped")

    if decision.requires_owner_approval:
        pending_ref: dict[str, PendingApproval] = {}

        async def execute_restore_missing(message: discord.Message) -> None:
            pending = pending_ref["value"]
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
                await update_approval_message(
                    message,
                    pending,
                    title="Approved Restore Missing Complete",
                    preview_title="Applied Changes",
                    preview_body=(
                        f"• **{stats['created_categories']}** categories created\n"
                        f"• **{stats['created_channels']}** channels created"
                    ),
                    color=EMBED_OK,
                    clear_content=True,
                    view=None,
                )
            except Exception as exc:
                await update_approval_message(
                    message,
                    pending,
                    title="Approved Restore Missing Failed",
                    preview_title="Failure Details",
                    preview_body=f"`{exc}`",
                    color=EMBED_ERR,
                    clear_content=True,
                    view=None,
                )

        queued = await queue_owner_approval(
            interaction,
            command_name="restore_missing",
            tier=decision.tier,
            risk_label="Non-Destructive",
            summary_text=format_approval_summary(
                requester_id=interaction.user.id,
                command_name="restore_missing",
                target_name=target.name,
                route_text=f"`{source.name}` -> `{target.name}`",
                scope_title="Request Scope",
                scope_text="Create only missing categories and channels from the source snapshot.",
                risk_label="Non-Destructive",
            ),
            preview_title="Planned Changes",
            preview_body="\n".join(preview_lines),
            execute=execute_restore_missing,
        )
        if queued is None:
            return
        pending_ref["value"] = queued
        await interaction.followup.send(
            embed=make_embed("Approval Requested", "The server owner must approve this restore request before CLINX will run it.", EMBED_INFO),
            ephemeral=True,
        )
        return

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
    )


@bot.tree.command(name="cleantoday", description="Delete channels created today (UTC)")
@app_commands.default_permissions(administrator=True)
async def cleantoday(interaction: discord.Interaction, confirm: bool = False) -> None:
    decision = await enforce_clinx_access(interaction, "cleantoday")
    if decision is None:
        return
    await interaction.response.defer(thinking=True)

    guild = interaction.guild
    if guild is None:
        await interaction.followup.send(embed=make_embed("Error", "Run this command in a server.", EMBED_ERR), ephemeral=True)
        return

    today = datetime.now(timezone.utc).date()
    targets = [ch for ch in guild.channels if getattr(ch, "created_at", None) and ch.created_at.date() == today]

    if not targets:
        await interaction.followup.send(embed=make_embed("Clean Today", "No channels created today (UTC).", EMBED_INFO))
        return

    if not confirm:
        await interaction.followup.send(
            embed=make_embed("Warning", f"Dry run: `{len(targets)}` channels would be deleted. Run again with `confirm=true`.", EMBED_WARN),
        )
        return

    if decision.requires_owner_approval:
        pending_ref: dict[str, PendingApproval] = {}

        async def execute_clean_today(message: discord.Message) -> None:
            pending = pending_ref["value"]
            deleted = 0
            try:
                for channel in targets:
                    try:
                        await throttled_discord_call(lambda channel=channel: channel.delete(reason=f"/cleantoday by {interaction.user}"))
                        deleted += 1
                    except (discord.Forbidden, discord.HTTPException):
                        continue
                await update_approval_message(
                    message,
                    pending,
                    title="Approved Clean Today Complete",
                    preview_title="Applied Changes",
                    preview_body=f"• **{deleted}** channels created today were deleted.",
                    color=EMBED_OK,
                    clear_content=True,
                    view=None,
                )
            except Exception as exc:
                await update_approval_message(
                    message,
                    pending,
                    title="Approved Clean Today Failed",
                    preview_title="Failure Details",
                    preview_body=f"`{exc}`",
                    color=EMBED_ERR,
                    clear_content=True,
                    view=None,
                )

        queued = await queue_owner_approval(
            interaction,
            command_name="cleantoday",
            tier=decision.tier,
            risk_label="Destructive",
            summary_text=format_approval_summary(
                requester_id=interaction.user.id,
                command_name="cleantoday",
                target_name=guild.name,
                route_text=None,
                scope_title="Request Scope",
                scope_text="Delete every channel in this guild that was created today (UTC).",
                risk_label="Destructive",
            ),
            preview_title="Planned Changes",
            preview_body=f"• **{len(targets)}** channels created today (UTC) are queued for deletion.",
            execute=execute_clean_today,
        )
        if queued is None:
            return
        pending_ref["value"] = queued
        await interaction.followup.send(
            embed=make_embed("Approval Requested", "The server owner must approve this cleanup request before CLINX will delete anything.", EMBED_INFO),
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
        except (discord.Forbidden, discord.HTTPException):
            pass

    await interaction.followup.send(embed=make_embed("Clean Today Complete", f"Deleted `{deleted}` channels.", EMBED_OK))


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
        decision = await enforce_clinx_access(interaction, "masschannels")
        if decision is None:
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(embed=make_embed("Error", "Run this command in a server.", EMBED_ERR), ephemeral=True)
            return

        await interaction.response.defer(thinking=True)
        layout = self.layout_input.value
        if self.extra_layout_input.value:
            layout = f"{layout}\n{self.extra_layout_input.value}"

        try:
            preview = preview_mass_channels_layout(
                guild,
                layout,
                create_categories=self.create_categories,
            )
        except ValueError as exc:
            await interaction.followup.send(embed=make_embed("Mass Channels", str(exc), EMBED_ERR), ephemeral=True)
            return

        if decision.requires_owner_approval:
            pending_ref: dict[str, PendingApproval] = {}

            async def execute_mass_channels(message: discord.Message) -> None:
                pending = pending_ref["value"]
                try:
                    stats = await create_mass_channels_from_layout(
                        guild,
                        layout,
                        create_categories=self.create_categories,
                    )
                    await update_approval_message(
                        message,
                        pending,
                        title="Approved Mass Create Complete",
                        preview_title="Applied Changes",
                        preview_body=(
                            f"• **{stats['created_categories']}** categories created\n"
                            f"• **{stats['created_channels']}** channels created\n"
                            f"• **{stats['skipped_channels']}** duplicate or existing entries skipped"
                        ),
                        color=EMBED_OK,
                        clear_content=True,
                        view=None,
                    )
                except Exception as exc:
                    await update_approval_message(
                        message,
                        pending,
                        title="Approved Mass Create Failed",
                        preview_title="Failure Details",
                        preview_body=f"`{exc}`",
                        color=EMBED_ERR,
                        clear_content=True,
                        view=None,
                    )

            queued = await queue_owner_approval(
                interaction,
                command_name="masschannels",
                tier=decision.tier,
                risk_label="Non-Destructive",
                summary_text=format_approval_summary(
                    requester_id=interaction.user.id,
                    command_name="masschannels",
                    target_name=guild.name,
                    route_text=None,
                    scope_title="Request Scope",
                    scope_text="Create categories and channels from the submitted layout block.",
                    risk_label="Non-Destructive",
                ),
                preview_title="Planned Changes",
                preview_body=(
                    f"• **{preview['created_categories']}** categories will be created\n"
                    f"• **{preview['created_channels']}** channels will be created\n"
                    f"• **{preview['skipped_channels']}** duplicate or existing entries will be skipped"
                ),
                execute=execute_mass_channels,
            )
            if queued is None:
                return
            pending_ref["value"] = queued
            await interaction.followup.send(
                embed=make_embed("Approval Requested", "The server owner must approve this mass channel request before CLINX will create anything.", EMBED_INFO),
                ephemeral=True,
            )
            return

        stats = await create_mass_channels_from_layout(
            guild,
            layout,
            create_categories=self.create_categories,
        )

        summary = (
            f"Created categories: `{stats['created_categories']}`\n"
            f"Created channels: `{stats['created_channels']}`\n"
            f"Skipped existing/duplicates: `{stats['skipped_channels']}`"
        )
        await interaction.followup.send(embed=make_embed("Mass Create Complete", summary, EMBED_OK))


@bot.tree.command(name="masschannels", description="Open the bulk channel creator modal")
@app_commands.default_permissions(administrator=True)
async def masschannels(interaction: discord.Interaction, create_categories: bool = True) -> None:
    if await enforce_clinx_access(interaction, "masschannels modal") is None:
        return
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
            discord.ui.ActionRow(
                SuggestionBoardLaunchButton(
                    "Suggestion",
                    label="Submit Suggestion",
                    style=discord.ButtonStyle.primary,
                    custom_id="suggestion-board-open-suggestion",
                    emoji="💡",
                ),
                SuggestionBoardLaunchButton(
                    "Bug Report",
                    label="Report Bug",
                    style=discord.ButtonStyle.secondary,
                    custom_id="suggestion-board-open-bug",
                    emoji="🛠️",
                ),
                discord.ui.Button(
                    label="Support",
                    style=discord.ButtonStyle.link,
                    url=SUPPORT_SERVER_URL,
                ),
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

        self.add_item(
            discord.ui.ActionRow(
                *(LibraryCategoryButton(category_name, self.category) for category_name in categories)
            )
        )
        self.add_item(
            discord.ui.ActionRow(
                LibraryCommandSelect(self.category, self.page, self.selected_command)
            )
        )
        self.add_item(
            discord.ui.ActionRow(
                LibraryPageButton(
                    direction=-1,
                    category=self.category,
                    page=self.page,
                    selected_command=self.selected_command,
                ),
                LibraryPageButton(
                    direction=1,
                    category=self.category,
                    page=self.page,
                    selected_command=self.selected_command,
                ),
                discord.ui.Button(
                    label="Invite",
                    style=discord.ButtonStyle.link,
                    url=build_invite_link(),
                ),
                discord.ui.Button(
                    label="Support",
                    style=discord.ButtonStyle.link,
                    url=SUPPORT_SERVER_URL,
                ),
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
    if await enforce_clinx_access(interaction, "panel suggestion") is None:
        return
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
    if await enforce_clinx_access(interaction, "export guild") is None:
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    snapshot = await serialize_guild_snapshot(guild)
    await send_text_file(interaction, json.dumps(snapshot, indent=2), f"guild_{guild.id}.json")


@export_group.command(name="channels", description="Export channels as JSON or CSV")
@app_commands.choices(fmt=[app_commands.Choice(name="json", value="json"), app_commands.Choice(name="csv", value="csv")])
async def export_channels(interaction: discord.Interaction, fmt: app_commands.Choice[str]) -> None:
    if await enforce_clinx_access(interaction, "export channels") is None:
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    channels = (await serialize_guild_snapshot(guild, include_assets=False))["channels"]
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
    if await enforce_clinx_access(interaction, "export roles") is None:
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
    if await enforce_clinx_access(interaction, "export channel") is None:
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
    if await enforce_clinx_access(interaction, "export role") is None:
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
    if await enforce_clinx_access(interaction, "export message") is None:
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
    if await enforce_clinx_access(interaction, "export reactions") is None:
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


async def run_import_job(
    guild: discord.Guild,
    snapshot: dict[str, Any],
    *,
    status_message: discord.Message | None = None,
    pending: PendingApproval | None = None,
) -> None:
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
        if status_message is not None and pending is not None:
            await update_approval_message(
                status_message,
                pending,
                title="Approved Import Complete",
                preview_title="Import Result",
                preview_body="The approved snapshot import completed successfully.",
                color=EMBED_OK,
                clear_content=True,
                view=None,
            )
    except asyncio.CancelledError:
        IMPORT_JOBS[guild.id]["status"] = "cancelled"
        IMPORT_JOBS[guild.id]["finished_at"] = utc_now_iso()
        if status_message is not None and pending is not None:
            try:
                await update_approval_message(
                    status_message,
                    pending,
                    title="Approved Import Cancelled",
                    preview_title="Import Result",
                    preview_body="The running import was cancelled before completion.",
                    color=EMBED_WARN,
                    clear_content=True,
                    view=None,
                )
            except discord.HTTPException:
                pass
        raise
    except Exception as exc:
        IMPORT_JOBS[guild.id]["status"] = "failed"
        IMPORT_JOBS[guild.id]["error"] = str(exc)
        IMPORT_JOBS[guild.id]["finished_at"] = utc_now_iso()
        if status_message is not None and pending is not None:
            try:
                await update_approval_message(
                    status_message,
                    pending,
                    title="Approved Import Failed",
                    preview_title="Failure Details",
                    preview_body=f"`{exc}`",
                    color=EMBED_ERR,
                    clear_content=True,
                    view=None,
                )
            except discord.HTTPException:
                pass


@import_group.command(name="guild", description="Import a guild snapshot JSON file")
@app_commands.default_permissions(administrator=True)
async def import_guild(interaction: discord.Interaction, file: discord.Attachment) -> None:
    decision = await enforce_clinx_access(interaction, "import guild")
    if decision is None:
        return
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

    if decision.requires_owner_approval:
        plan = build_backup_plan_preview(snapshot, interaction.guild, {"load_roles", "load_channels", "load_settings"})
        pending_ref: dict[str, PendingApproval] = {}

        async def execute_import(message: discord.Message) -> None:
            pending = pending_ref["value"]
            await update_approval_message(
                message,
                pending,
                title="Approved Import Started",
                preview_title="Execution State",
                preview_body="The approved guild snapshot import is running now.",
                color=BACKUP_PLANNER_ACCENT,
                clear_content=True,
                view=None,
            )
            task = asyncio.create_task(
                run_import_job(
                    interaction.guild,
                    snapshot,
                    status_message=message,
                    pending=pending,
                )
            )
            IMPORT_JOBS[interaction.guild.id] = {
                "status": "running",
                "started_at": utc_now_iso(),
                "task": task,
            }

        queued = await queue_owner_approval(
            interaction,
            command_name="import guild",
            tier=decision.tier,
            risk_label="Non-Destructive",
            summary_text=format_approval_summary(
                requester_id=interaction.user.id,
                command_name="import guild",
                target_name=interaction.guild.name,
                route_text="`uploaded snapshot` -> current server",
                scope_title="Request Scope",
                scope_text="Import roles, channels, and settings from the uploaded guild snapshot without deleting live objects first.",
                risk_label="Non-Destructive",
            ),
            preview_title="Planned Changes",
            preview_body=render_backup_plan_lines(plan),
            execute=execute_import,
        )
        if queued is None:
            return
        pending_ref["value"] = queued
        await interaction.followup.send(
            embed=make_embed("Approval Requested", "The server owner must approve this snapshot import before CLINX will start it.", EMBED_INFO),
            ephemeral=True,
        )
        return

    task = asyncio.create_task(run_import_job(interaction.guild, snapshot))
    IMPORT_JOBS[interaction.guild.id] = {"status": "running", "started_at": utc_now_iso(), "task": task}
    await interaction.followup.send(embed=make_embed("Import Started", "Use `/import status` to track progress.", EMBED_INFO), ephemeral=True)


@import_group.command(name="status", description="Get current import status")
@app_commands.default_permissions(administrator=True)
async def import_status(interaction: discord.Interaction) -> None:
    if await enforce_clinx_access(interaction, "import status") is None:
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
    if await enforce_clinx_access(interaction, "import cancel") is None:
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


@safety_group.command(name="grant", description="Owner-only: grant CLINX trusted admin access")
@app_commands.default_permissions(administrator=True)
async def safety_grant(interaction: discord.Interaction, user: discord.Member) -> None:
    if await enforce_clinx_access(interaction, "safety grant") is None:
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return
    if user.bot:
        await interaction.response.send_message(embed=make_embed("Safety", "Bots cannot be added as trusted admins.", EMBED_WARN), ephemeral=True)
        return
    if user.id == guild.owner_id:
        await interaction.response.send_message(embed=make_embed("Safety", "The server owner is always trusted.", EMBED_INFO), ephemeral=True)
        return

    add_trusted_admin(guild.id, user.id)
    await interaction.response.send_message(
        embed=make_embed("Trusted Admin Added", f"{user.mention} can now run Tier 2 and Tier 3 CLINX actions without owner approval.", EMBED_OK),
        ephemeral=True,
    )


@safety_group.command(name="revoke", description="Owner-only: revoke CLINX trusted admin access")
@app_commands.default_permissions(administrator=True)
async def safety_revoke(interaction: discord.Interaction, user: discord.Member) -> None:
    if await enforce_clinx_access(interaction, "safety revoke") is None:
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return
    if user.id == guild.owner_id:
        await interaction.response.send_message(embed=make_embed("Safety", "The server owner cannot be removed from CLINX trust.", EMBED_INFO), ephemeral=True)
        return

    remove_trusted_admin(guild.id, user.id)
    await interaction.response.send_message(
        embed=make_embed("Trusted Admin Removed", f"{user.mention} now requires owner approval for Tier 2 and Tier 3 CLINX actions.", EMBED_OK),
        ephemeral=True,
    )


@safety_group.command(name="list", description="Owner-only: list CLINX trusted admins")
@app_commands.default_permissions(administrator=True)
async def safety_list(interaction: discord.Interaction) -> None:
    if await enforce_clinx_access(interaction, "safety list") is None:
        return
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(embed=make_embed("Error", "Run in a server.", EMBED_ERR), ephemeral=True)
        return

    trusted_ids = sorted(get_trusted_admin_ids(guild.id))
    if not trusted_ids:
        await interaction.response.send_message(embed=make_embed("Trusted Admins", "No trusted admins are configured for this guild.", EMBED_INFO), ephemeral=True)
        return

    lines: list[str] = []
    for trusted_id in trusted_ids:
        member = guild.get_member(trusted_id)
        lines.append(member.mention if member else f"`{trusted_id}`")
    await interaction.response.send_message(embed=make_embed("Trusted Admins", "\n".join(lines), EMBED_INFO), ephemeral=True)


@bot.tree.command(name="help", description="Get command help")
async def help_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.send_message(view=CommandLibraryView())


@bot.tree.command(name="invite", description="Get bot invite link")
async def invite(interaction: discord.Interaction) -> None:
    link = build_invite_link()
    await interaction.response.send_message(embed=make_embed("Invite CLINX", link, EMBED_INFO))


@bot.tree.command(name="leave", description="Make bot leave this server")
@app_commands.default_permissions(administrator=True)
async def leave(interaction: discord.Interaction) -> None:
    if await enforce_clinx_access(interaction, "leave") is None:
        return
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








