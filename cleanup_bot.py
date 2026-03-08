import os

import discord
from discord.ext import commands

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN is not set.")
BACKUP_SERVER_ID = 1467366559902335202
MAIN_SERVER_ID = 1418476529368825989

intents = discord.Intents.default()
intents.guilds = True

bot = commands.Bot(command_prefix="!", intents=intents)


def convert_overwrites(overwrites, main_guild):
    new = {}
    for target, overwrite in overwrites.items():

        if isinstance(target, discord.Role):
            role = discord.utils.get(main_guild.roles, name=target.name)
            if role:
                new[role] = overwrite

        elif isinstance(target, discord.Member):
            member = main_guild.get_member(target.id)
            if member:
                new[member] = overwrite

    return new


@bot.event
async def on_ready():

    backup = bot.get_guild(BACKUP_SERVER_ID)
    main = bot.get_guild(MAIN_SERVER_ID)

    print("Starting server repair...")

    category_map = {}

    # ---- CATEGORY SYNC ----
    for bcat in backup.categories:

        mcat = discord.utils.get(main.categories, name=bcat.name)

        overwrites = convert_overwrites(bcat.overwrites, main)

        if mcat is None:

            mcat = await main.create_category(
                name=bcat.name,
                overwrites=overwrites
            )

            print(f"Created category: {bcat.name}")

        else:

            await mcat.edit(overwrites=overwrites)

        category_map[bcat.id] = mcat


    # ---- CHANNEL SYNC ----
    for bch in backup.channels:

        mch = discord.utils.get(main.channels, name=bch.name)

        category = None
        if bch.category:
            category = category_map.get(bch.category.id)

        overwrites = convert_overwrites(bch.overwrites, main)

        if mch is None:

            if isinstance(bch, discord.TextChannel):

                await main.create_text_channel(
                    name=bch.name,
                    category=category,
                    topic=bch.topic,
                    slowmode_delay=bch.slowmode_delay,
                    nsfw=bch.nsfw,
                    overwrites=overwrites
                )

                print(f"Created missing text channel: {bch.name}")

            elif isinstance(bch, discord.VoiceChannel):

                await main.create_voice_channel(
                    name=bch.name,
                    category=category,
                    bitrate=bch.bitrate,
                    user_limit=bch.user_limit,
                    overwrites=overwrites
                )

                print(f"Created missing voice channel: {bch.name}")

        else:

            await mch.edit(
                category=category,
                overwrites=overwrites
            )

            print(f"Synced permissions: {bch.name}")

    print("Server repair finished.")

bot.run(TOKEN)

