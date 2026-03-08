import os

import discord
from discord.ext import commands

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN is not set.")
BACKUP_SERVER_ID = 1467366559902335202
MAIN_SERVER_ID = 1418476529368825989

intents = discord.Intents(guilds=True)

bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():

    print(f"Logged in as {bot.user}")
    print("Starting server verification...")

    backup = bot.get_guild(BACKUP_SERVER_ID)
    main = bot.get_guild(MAIN_SERVER_ID)

    missing_categories = []
    missing_channels = []

    # ----- CATEGORY CHECK -----
    for bcat in backup.categories:

        mcat = discord.utils.get(main.categories, name=bcat.name)

        if mcat is None:
            missing_categories.append(bcat.name)

    # ----- CHANNEL CHECK -----
    for bch in backup.channels:

        mch = discord.utils.get(main.channels, name=bch.name)

        if mch is None:
            missing_channels.append(bch.name)

    print("\n===== VERIFICATION RESULT =====\n")

    if not missing_categories and not missing_channels:
        print("âœ… Server structure matches backup. Nothing missing.")

    else:

        if missing_categories:
            print("Missing categories:")
            for c in missing_categories:
                print(f" - {c}")

        if missing_channels:
            print("\nMissing channels:")
            for c in missing_channels:
                print(f" - {c}")

    print("\nVerification finished.")

    await bot.close()


bot.run(TOKEN)

